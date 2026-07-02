// Package forwarding provides the CreatorForwardingRunner — a durable,
// lease-based, supervised background runner that replaces the volatile
// in-memory scheduleCreatorPolling goroutines.
//
// The runner claims PENDING/RETRY_WAIT creator_forwardings rows,
// polls the remote creator engine for completion, and transitions
// completed rows to READY_TO_FORWARD so the downstream ForwardingService
// can enqueue them as proper Velox Jobs.
//
// Architecture:
//
//	tick → ClaimCreatorForwardings (batch) → for each lease:
//	  → start lease-renewal goroutine (every leaseDuration/3)
//	  → poll remote engine via GetPipelineStatus
//	  → on terminal+success → MarkCreatorForwardingReadyToForward
//	  → on terminal+failure → MarkCreatorForwardingFailed
//	  → on transient error    → MarkCreatorForwardingRetry (backoff)
//	  → cancel renewal goroutine
//
// The runner uses a bounded semaphore for concurrency and exposes
// lightweight metrics (queue depth, oldest pending age) for monitoring.
package forwarding

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"velox-server/internal/costmodel"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/remoteengine"
	"velox-server/internal/routing"
	"velox-server/internal/store"
)

// ── Config ───────────────────────────────────────────────────────────────

// RunnerConfig tunes the CreatorForwardingRunner.
type RunnerConfig struct {
	// PollInterval is how often the runner scans for claimable forwardings.
	PollInterval time.Duration
	// LeaseDuration is how long a claim is held before another runner can
	// re-claim it. Should be > the worst-case remote poll latency.
	LeaseDuration time.Duration
	// MaxAttempts per forwarding before declaring FAILED. 0 means default (12).
	MaxAttempts int
	// ClaimBatch limits how many forwardings the runner claims per tick.
	ClaimBatch int
	// Concurrency limits how many forwardings are processed concurrently.
	Concurrency int
	// BackoffSchedule maps attempt number (1-based) to the delay before the
	// next attempt. The last entry is used for all subsequent attempts.
	// Only used for transient errors (poll failures); non-terminal "still
	// running" statuses release the claim immediately (no backoff).
	BackoffSchedule []time.Duration
}

// DefaultRunnerConfig returns sensible defaults matching the audit
// recommended values.
func DefaultRunnerConfig() *RunnerConfig {
	return &RunnerConfig{
		PollInterval:  5 * time.Second,
		LeaseDuration: 5 * time.Minute,
		ClaimBatch:    20,
		MaxAttempts:    12,
		Concurrency:   4,
		BackoffSchedule: []time.Duration{
			30 * time.Second,
			2 * time.Minute,
			10 * time.Minute,
			30 * time.Minute,
		},
	}
}

// backoffForAttempt returns the backoff delay for the given 1-based
// attempt number using the configured schedule.
func (cfg *RunnerConfig) backoffForAttempt(attempt int) time.Duration {
	if len(cfg.BackoffSchedule) == 0 {
		return 30 * time.Second
	}
	idx := attempt - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(cfg.BackoffSchedule) {
		idx = len(cfg.BackoffSchedule) - 1
	}
	return cfg.BackoffSchedule[idx]
}

// ── Metrics ──────────────────────────────────────────────────────────────

// RunnerMetrics exposes lightweight counters for the CreatorForwardingRunner.
// Uses atomic integers so the metrics are safe to read from any goroutine
// (e.g. from a /metrics HTTP handler or a supervisor probe).
type RunnerMetrics struct {
	Claimed       atomic.Int64 // total forwardings claimed
	Forwarded     atomic.Int64 // successfully transitioned to READY_TO_FORWARD
	Failed        atomic.Int64 // terminal failures
	Retried       atomic.Int64 // retries scheduled
	QueueDepth    atomic.Int64 // approximate: PENDING + RETRY_WAIT count
	OldestPending atomic.Int64 // approximate: oldest PENDING age in seconds
}

// Snapshot returns a point-in-time copy of the metric values.
func (m *RunnerMetrics) Snapshot() map[string]int64 {
	return map[string]int64{
		"forwarding_claimed":        m.Claimed.Load(),
		"forwarding_forwarded":      m.Forwarded.Load(),
		"forwarding_failed":         m.Failed.Load(),
		"forwarding_retried":        m.Retried.Load(),
		"forwarding_queue_depth":    m.QueueDepth.Load(),
		"forwarding_oldest_pending": m.OldestPending.Load(),
	}
}

// ── Runner ───────────────────────────────────────────────────────────────

// CreatorForwardingRunner drives the persistent polling of remote creator
// jobs and transitions completed results atomically to FORWARDED (Job+Task
// creation + forwarding status update in a single SQLite transaction).
type CreatorForwardingRunner struct {
	cfg      *RunnerConfig
	dbStore  *store.SQLiteStore
	client   *remoteengine.Client
	enqueuer *enqueue.Enqueuer
	identity string
	metrics  *RunnerMetrics

	sem chan struct{} // bounded concurrency

	mu        sync.Mutex
	stopCh    chan struct{}
	stoppedCh chan struct{}
}

// NewCreatorForwardingRunner wires a runner. dbStore is the durable anchor;
// client provides access to the remote creator engine. enqueuer is optional
// (nil-safe) — when set, the runner handles the full poll+enqueue lifecycle
// atomically; when nil, the runner only polls and stores payloads.
func NewCreatorForwardingRunner(cfg *RunnerConfig, dbStore *store.SQLiteStore, client *remoteengine.Client, enqueuer *enqueue.Enqueuer, identity string) *CreatorForwardingRunner {
	if cfg == nil {
		cfg = DefaultRunnerConfig()
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 4
	}
	if identity == "" {
		identity = fmt.Sprintf("cf-runner-%d", time.Now().UnixNano())
	}
	return &CreatorForwardingRunner{
		cfg:       cfg,
		dbStore:   dbStore,
		client:    client,
		enqueuer:  enqueuer,
		identity:  identity,
		metrics:   &RunnerMetrics{},
		sem:       make(chan struct{}, cfg.Concurrency),
		stopCh:    make(chan struct{}),
		stoppedCh: make(chan struct{}),
	}
}

// Metrics returns the runner's metrics for external consumption.
func (r *CreatorForwardingRunner) Metrics() *RunnerMetrics {
	return r.metrics
}

// Run is the durable tick loop. It blocks until ctx is cancelled or Stop is
// called. The loop polls the database at cfg.PollInterval, claims up to
// ClaimBatch claimable forwardings per cycle, and dispatches each to
// processLease with bounded concurrency.
func (r *CreatorForwardingRunner) Run(ctx context.Context) error {
	if r == nil {
		return fmt.Errorf("forwarding: nil runner")
	}
	defer close(r.stoppedCh)

	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	// Metrics refresh runs on a separate, slower cadence (every 30s)
	// to avoid hitting the DB with COUNT/strftime queries every 5s.
	metricsTicker := time.NewTicker(30 * time.Second)
	defer metricsTicker.Stop()

	// Initial metrics snapshot on startup.
	r.refreshMetrics(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.stopCh:
			return nil
		case <-ticker.C:
			if err := r.tick(ctx); err != nil {
				log.Printf("[FORWARDING] tick error: %v", err)
			}
		case <-metricsTicker.C:
			r.refreshMetrics(ctx)
		}
	}
}

// Stop signals the runner to exit after the in-flight tick completes.
func (r *CreatorForwardingRunner) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	select {
	case <-r.stopCh:
	default:
		close(r.stopCh)
	}
	<-r.stoppedCh
}

// tick performs one poll: claim up to ClaimBatch claimable forwardings,
// then process each one with bounded concurrency.
func (r *CreatorForwardingRunner) tick(ctx context.Context) error {
	if r.client == nil || !r.client.IsConfigured() {
		return nil // remote creator not configured; no work to do
	}

	leases, err := r.dbStore.ClaimCreatorForwardings(ctx, r.identity, "cf", r.cfg.LeaseDuration, r.cfg.ClaimBatch)
	if err != nil {
		return fmt.Errorf("claim forwardings: %w", err)
	}
	if len(leases) == 0 {
		return nil
	}

	r.metrics.Claimed.Add(int64(len(leases)))
	log.Printf("[FORWARDING] claimed %d forwardings", len(leases))

	var wg sync.WaitGroup
	for _, lease := range leases {
		wg.Add(1)
		go func(l store.CreatorForwardingLease) {
			defer wg.Done()
			// Acquire semaphore (bounded concurrency).
			select {
			case r.sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-r.sem }()

			r.processLease(ctx, l)
		}(lease)
	}
	wg.Wait()
	return nil
}

// processLease handles a single claimed forwarding: polls the remote
// creator, manages lease renewal, and transitions to the appropriate
// next state. When the enqueuer is configured and the remote creator
// completes successfully, the runner handles the full lifecycle
// atomically: Job+Task+TaskSpec creation + FORWARDED marking in a
// single SQLite transaction.
//
// Lease-loss propagation: a cancellable processing context (procCtx) is
// created for this lease. The renewal loop receives its cancel function;
// if the lease is lost (RenewCreatorForwardingLease returns
// ErrTransitionConflict), the renewal loop cancels procCtx, causing all
// in-flight operations (GetPipelineStatus, DB writes) to fail with a
// context error. The runner then exits without touching the row — the
// new lease holder owns it.
func (r *CreatorForwardingRunner) processLease(ctx context.Context, lease store.CreatorForwardingLease) {
	// Create a processing context that the renewal loop can cancel
	// if the lease is lost.
	procCtx, procCancel := context.WithCancel(ctx)
	defer procCancel()

	// Start lease renewal in background.
	go r.renewLeaseLoop(procCtx, procCancel, lease)

	// Poll remote creator for status — uses procCtx so lease loss
	// cancels the in-flight request.
	resp, err := r.client.GetPipelineStatus(procCtx, lease.SourceJobID)
	if err != nil {
		log.Printf("[FORWARDING] poll failed forwarding=%s source_job=%s attempt=%d: %v",
			lease.ForwardingID, lease.SourceJobID, lease.AttemptCount, err)
		// Check if we lost the lease (procCtx was cancelled by renewal loop).
		if procCtx.Err() != nil {
			log.Printf("[FORWARDING] lease lost during poll forwarding=%s; abandoning", lease.ForwardingID)
			return
		}
		r.handleRetry(ctx, lease, "POLL_ERROR", err.Error())
		r.metrics.Retried.Add(1)
		return
	}

	// Classify the remote status.
	switch {
	case isTerminalSuccess(resp.Status):
		// Remote creator completed successfully.
		if r.enqueuer != nil {
			// Full atomic lifecycle: build Job+TaskSpec and enqueue+forward
			// in a single SQLite transaction.
			r.atomicEnqueueAndForward(ctx, lease, resp.Result)
		} else {
			// Fallback: store payload for a separate forwarding service.
			payloadJSON, payloadSHA256 := marshalPayload(resp.Result)
			if payloadJSON == "" && payloadSHA256 == "" {
				// Non-serializable payload — mark BLOCKED permanently.
				log.Printf("[FORWARDING] payload marshal failed forwarding=%s; marking BLOCKED", lease.ForwardingID)
				if err := r.dbStore.MarkCreatorForwardingBlocked(ctx,
					lease.ForwardingID, r.identity, lease.LeaseID,
					"PAYLOAD_MARSHAL_ERROR",
					"result payload is not JSON-serializable",
				); err != nil {
					log.Printf("[FORWARDING] mark blocked forwarding=%s: %v", lease.ForwardingID, err)
				}
				r.metrics.Failed.Add(1)
				return
			}
			if err := r.dbStore.MarkCreatorForwardingReadyToForward(ctx,
				lease.ForwardingID, r.identity, lease.LeaseID,
				payloadJSON, payloadSHA256,
			); err != nil {
				log.Printf("[FORWARDING] mark ready-to-forward failed forwarding=%s: %v", lease.ForwardingID, err)
				r.handleRetry(ctx, lease, "MARK_READY_ERROR", err.Error())
				r.metrics.Retried.Add(1)
				return
			}
			log.Printf("[FORWARDING] ready-to-forward forwarding=%s source_job=%s source_provider=%s",
				lease.ForwardingID, lease.SourceJobID, lease.SourceProvider)
			r.metrics.Forwarded.Add(1)
		}

	case isTerminalFailure(resp.Status):
		// Remote creator failed.
		errMsg := resp.Error
		if errMsg == "" {
			errMsg = fmt.Sprintf("remote status: %s", resp.Status)
		}
		if err := r.dbStore.MarkCreatorForwardingFailed(ctx,
			lease.ForwardingID, r.identity, lease.LeaseID,
			"REMOTE_FAILED", errMsg,
		); err != nil {
			log.Printf("[FORWARDING] mark failed forwarding=%s: %v", lease.ForwardingID, err)
		}
		log.Printf("[FORWARDING] failed forwarding=%s source_job=%s status=%s",
			lease.ForwardingID, lease.SourceJobID, resp.Status)
		r.metrics.Failed.Add(1)

	default:
		// Still running / queued — release the claim immediately so another
		// runner (or the next tick) can pick it up. No backoff: the job is
		// still in progress, not errored.
		nextAttempt := time.Now().UTC() // immediate re-claim eligibility
		r.metrics.Retried.Add(1)
		if err := r.dbStore.MarkCreatorForwardingRetry(ctx,
			lease.ForwardingID, r.identity, lease.LeaseID,
			"NOT_FINISHED", fmt.Sprintf("remote status: %s", resp.Status),
			nextAttempt,
		); err != nil {
			log.Printf("[FORWARDING] mark retry (still-running) failed forwarding=%s: %v", lease.ForwardingID, err)
		}
	}
}

// atomicEnqueueAndForward builds the Job+TaskSpec from the remote creator
// result and calls AtomicForwardAndEnqueue to create the Job+Task+TaskSpec
// and mark the forwarding as FORWARDED in a single SQLite transaction.
// On failure, the forwarding is transitioned to RETRY_WAIT with backoff.
func (r *CreatorForwardingRunner) atomicEnqueueAndForward(ctx context.Context, lease store.CreatorForwardingLease, result map[string]interface{}) {
	if result == nil {
		result = map[string]interface{}{}
	}
	// Inject the forwarding key so DeriveForwardingJobID produces a
	// deterministic job_id.
	fwdKey := routing.FormatForwardingKey(
		lease.SourceProvider, lease.SourceJobID, lease.TargetExecutorID,
	)
	result[routing.KeyForwardingKey] = fwdKey.String()

	// 1. Store the payload + release the poll lease (POLLING → READY_TO_FORWARD).
	//    The atomic enqueue will claim it back (READY_TO_FORWARD → FORWARDING)
	//    inside the same DB transaction.
	payloadJSON, payloadSHA256 := marshalPayload(result)
	if payloadJSON == "" && payloadSHA256 == "" {
		// Non-serializable payload — mark BLOCKED permanently.
		log.Printf("[FORWARDING] payload marshal failed forwarding=%s; marking BLOCKED", lease.ForwardingID)
		if err := r.dbStore.MarkCreatorForwardingBlocked(ctx,
			lease.ForwardingID, r.identity, lease.LeaseID,
			"PAYLOAD_MARSHAL_ERROR",
			"enqueue payload is not JSON-serializable",
		); err != nil {
			log.Printf("[FORWARDING] mark blocked forwarding=%s: %v", lease.ForwardingID, err)
		}
		r.metrics.Failed.Add(1)
		return
	}
	if err := r.dbStore.MarkCreatorForwardingReadyToForward(ctx,
		lease.ForwardingID, r.identity, lease.LeaseID,
		payloadJSON, payloadSHA256,
	); err != nil {
		log.Printf("[FORWARDING] mark ready-to-forward failed forwarding=%s: %v", lease.ForwardingID, err)
		r.handleRetry(ctx, lease, "MARK_READY_ERROR", err.Error())
		r.metrics.Retried.Add(1)
		return
	}

	// 2. Prepare the Job+TaskSpec (business logic, no DB write).
	job, spec, priority, err := r.enqueuer.PrepareJobAndTask(ctx, result, costmodel.DefaultRequirements())
	if err != nil {
		log.Printf("[FORWARDING] prepare job+task failed forwarding=%s: %v", lease.ForwardingID, err)
		r.handleEnqueueRetry(ctx, lease, "PREPARE_FAILED", err.Error())
		return
	}

	// 3. Atomic enqueue + forward (one SQLite transaction).
	if err := r.dbStore.AtomicForwardAndEnqueue(ctx, lease.ForwardingID, job, spec, priority); err != nil {
		log.Printf("[FORWARDING] atomic forward+enqueue failed forwarding=%s: %v", lease.ForwardingID, err)
		r.handleEnqueueRetry(ctx, lease, "ENQUEUE_FAILED", err.Error())
		return
	}

	log.Printf("[FORWARDING] forwarded forwarding=%s → job=%s source=%s",
		lease.ForwardingID, job.ID, lease.SourceProvider)
	r.metrics.Forwarded.Add(1)
}

// handleEnqueueRetry transitions the forwarding to RETRY_WAIT with backoff
// when the enqueue phase fails (e.g. payload build error, atomic write
// conflict). Uses MarkCreatorForwardingEnqueueRetry which handles
// FORWARDING/READY_TO_FORWARD states. On max attempts or CAS failure,
// falls back to MarkCreatorForwardingFailed to prevent silent stuck rows.
func (r *CreatorForwardingRunner) handleEnqueueRetry(ctx context.Context, lease store.CreatorForwardingLease, code, msg string) {
	maxAttempts := r.cfg.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 12
	}
	if lease.AttemptCount >= maxAttempts {
		if err := r.dbStore.MarkCreatorForwardingFailed(ctx,
			lease.ForwardingID, r.identity, lease.LeaseID,
			"MAX_ENQUEUE_ATTEMPTS",
			fmt.Sprintf("exhausted %d attempts: %s", maxAttempts, msg),
		); err != nil {
			log.Printf("[FORWARDING] mark failed (max enqueue attempts) forwarding=%s: %v", lease.ForwardingID, err)
		}
		log.Printf("[FORWARDING] max enqueue attempts exhausted forwarding=%s source_job=%s attempts=%d",
			lease.ForwardingID, lease.SourceJobID, lease.AttemptCount)
		r.metrics.Failed.Add(1)
		return
	}

	backoff := r.cfg.backoffForAttempt(lease.AttemptCount)
	nextAttempt := time.Now().UTC().Add(backoff)
	if err := r.dbStore.MarkCreatorForwardingEnqueueRetry(ctx,
		lease.ForwardingID, code, msg, nextAttempt,
	); err != nil {
		// CAS failure (race with another runner or already transitioned) —
		// fall back to terminal failure to prevent the row from being
		// silently stuck in FORWARDING/READY_TO_FORWARD forever.
		log.Printf("[FORWARDING] enqueue retry CAS failed forwarding=%s: %v; marking FAILED", lease.ForwardingID, err)
		if ferr := r.dbStore.MarkCreatorForwardingFailed(ctx,
			lease.ForwardingID, r.identity, lease.LeaseID,
			"ENQUEUE_RETRY_CAS_FAILED",
			fmt.Sprintf("CAS failure on enqueue retry: %v", err),
		); ferr != nil {
			log.Printf("[FORWARDING] mark failed (CAS fallback) forwarding=%s: %v", lease.ForwardingID, ferr)
		}
		r.metrics.Failed.Add(1)
		return
	}
	r.metrics.Retried.Add(1)
}

// handleRetry transitions the forwarding to RETRY_WAIT with the
// backoff schedule applied. If max attempts are exhausted, the
// forwarding is marked FAILED instead.
func (r *CreatorForwardingRunner) handleRetry(ctx context.Context, lease store.CreatorForwardingLease, code, msg string) {
	maxAttempts := r.cfg.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 12
	}
	if lease.AttemptCount >= maxAttempts {
		if err := r.dbStore.MarkCreatorForwardingFailed(ctx,
			lease.ForwardingID, r.identity, lease.LeaseID,
			"MAX_ATTEMPTS",
			fmt.Sprintf("exhausted %d attempts: %s", maxAttempts, msg),
		); err != nil {
			log.Printf("[FORWARDING] mark failed (max attempts) forwarding=%s: %v", lease.ForwardingID, err)
		}
		log.Printf("[FORWARDING] max attempts exhausted forwarding=%s source_job=%s attempts=%d",
			lease.ForwardingID, lease.SourceJobID, lease.AttemptCount)
		r.metrics.Failed.Add(1)
		return
	}

	backoff := r.cfg.backoffForAttempt(lease.AttemptCount)
	nextAttempt := time.Now().UTC().Add(backoff)
	if err := r.dbStore.MarkCreatorForwardingRetry(ctx,
		lease.ForwardingID, r.identity, lease.LeaseID,
		code, msg, nextAttempt,
	); err != nil {
		log.Printf("[FORWARDING] mark retry failed forwarding=%s: %v", lease.ForwardingID, err)
	}
}

// renewLeaseLoop extends the lease periodically while processLease is
// polling the remote creator. Stops when the context is cancelled (which
// happens when processLease returns or when the lease is lost).
//
// Lease-loss propagation: if RenewCreatorForwardingLease returns
// ErrTransitionConflict (another runner preempted the lease), the loop
// calls procCancel to cancel the processing context, causing processLease
// to abort and release the forwarding without further DB writes.
func (r *CreatorForwardingRunner) renewLeaseLoop(ctx context.Context, procCancel context.CancelFunc, lease store.CreatorForwardingLease) {
	interval := r.cfg.LeaseDuration / 3
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			newExpiry := time.Now().UTC().Add(r.cfg.LeaseDuration)
			if err := r.dbStore.RenewCreatorForwardingLease(
				ctx, // bound to procCtx; cancelled on lease loss
				lease.ForwardingID, r.identity, lease.LeaseID, newExpiry,
			); err != nil {
				log.Printf("[FORWARDING] renew lease failed forwarding=%s: %v", lease.ForwardingID, err)
				// If the lease was preempted by another runner, cancel
				// processLease so it abandons the forwarding without
				// further DB writes.
				procCancel()
				return
			}
		}
	}
}

// refreshMetrics updates the lightweight queue depth and oldest pending
// age gauges. Called periodically by the Run loop (see refreshInterval).
// Errors are logged but not returned — metrics are best-effort.
//
// Delegates to the store's GetForwardingQueueMetrics so the runner
// never reaches through to r.dbStore.DB() directly — the repository
// owns the SQL, the runner owns the scheduling.
func (r *CreatorForwardingRunner) refreshMetrics(ctx context.Context) {
	if r.dbStore == nil {
		return
	}
	m, err := r.dbStore.GetForwardingQueueMetrics(ctx)
	if err != nil {
		log.Printf("[FORWARDING] metrics refresh: %v", err)
		return
	}
	r.metrics.QueueDepth.Store(m.QueueDepth)
	r.metrics.OldestPending.Store(int64(m.OldestPendingAge.Seconds()))
}

// ── Helpers ──────────────────────────────────────────────────────────────

func isTerminalSuccess(status string) bool {
	switch status {
	case "completed", "succeeded", "done":
		return true
	default:
		return false
	}
}

func isTerminalFailure(status string) bool {
	switch status {
	case "failed", "error":
		return true
	default:
		return false
	}
}

// marshalPayload serializes the result map. On JSON marshal failure,
// returns an empty payload with a zero hash — the caller should treat
// an empty SHA256 as a signal that the payload is not serializable.
func marshalPayload(result map[string]interface{}) (payloadJSON, payloadSHA256 string) {
	if result == nil {
		return "{}", sha256Hex([]byte("{}"))
	}
	raw, err := json.Marshal(result)
	if err != nil {
		// Non-serializable payload — return empty so the caller can
		// detect and mark BLOCKED rather than silently writing {}.
		return "", ""
	}
	payloadJSON = string(raw)
	payloadSHA256 = sha256Hex(raw)
	return
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
