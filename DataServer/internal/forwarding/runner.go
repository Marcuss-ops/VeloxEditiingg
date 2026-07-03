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
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"velox-server/internal/creatorflow"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/remoteengine"
	"velox-server/internal/store"
	"velox-server/internal/supervisor"
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
		MaxAttempts:   12,
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
	cfg          *RunnerConfig
	dbStore      *store.SQLiteStore
	client       *remoteengine.Client
	enqueuer     *enqueue.Enqueuer
	identity     string
	metrics      *RunnerMetrics
	resolver     *creatorflow.Resolver // canonical forward-completed entry point
	resolverOnce sync.Once             // guards lazyResolver against concurrent first-call race

	sem chan struct{} // bounded concurrency

	mu        sync.Mutex
	stopCh    chan struct{}
	stoppedCh chan struct{}
}

// NewCreatorForwardingRunner wires a runner. dbStore is the durable anchor;
// client provides access to the remote creator engine. enqueuer is optional
// (nil-safe) — when set, the runner handles the full poll+enqueue lifecycle
// atomically; when nil, the runner only polls and stores payloads.
//
// Resolver is built lazily via NewResolverMinimal on first call to
// atomicEnqueueAndForward; callers may pre-build it via SetResolver to
// share a single Resolver instance with the HTTP handler.
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

// SetResolver injects a pre-built Resolver so the runner uses the same
// instance the HTTP handler uses. Without this call the runner builds
// its own NewResolverMinimal on first use; both paths produce the
// same job_id + forwarding_id because both go through Resolver.Resolve.
//
// MUST be called before Run() starts. Calling SetResolver concurrently
// with lazyResolver (which fires resolverOnce) is a data race on
// r.resolver. In practice SetResolver is a composition-root / startup
// call, never called from a processing goroutine.
//
// Blocco 5 of the Verdetto (P1 #11): the runner's atomicEnqueueAndForward
// delegates to Resolver.Resolve rather than building the Job+TaskSpec
// inline + calling AtomicForwardAndEnqueue. This unifies the
// forward-completed path: handler and runner converge on the Resolver's
// idempotency + payload-marshal + URL-rewrite + atomic-write pipeline.
func (r *CreatorForwardingRunner) SetResolver(rs *creatorflow.Resolver) {
	if r == nil {
		return
	}
	r.resolver = rs
}

// lazyResolver returns the runner's Resolver, building a minimal one
// on first use if SetResolver was not called. The minimal resolver
// skips URL rewriting (the runner already received a complete payload
// from the remote engine) but owns the full atomic + idempotency flow.
//
// Thread-safety: processLease is called from concurrent goroutines
// (bounded by r.sem). Without a guard, two goroutines could both see
// r.resolver == nil and each build a separate minimal Resolver — a
// data race on the r.resolver field. sync.Once guarantees exactly one
// initialisation across all goroutines; subsequent calls are lock-free.
func (r *CreatorForwardingRunner) lazyResolver() *creatorflow.Resolver {
	r.resolverOnce.Do(func() {
		if r.resolver == nil {
			r.resolver = creatorflow.NewResolverMinimal(r.enqueuer, r.dbStore)
		}
	})
	return r.resolver
}

// Metrics returns the runner's metrics for external consumption.
func (r *CreatorForwardingRunner) Metrics() *RunnerMetrics {
	return r.metrics
}

// Run is the durable tick loop. It blocks until ctx is cancelled or Stop is
// called. The loop polls the database at cfg.PollInterval, claims up to
// ClaimBatch claimable forwardings per cycle, and dispatches each to
// processLease with bounded concurrency.
//
// Verdetto P1 #10 (Blocco 4): tick errors are CLASSIFIED rather than
// logged-and-continued. Per-element errors (one bad forwarding) are
// persisted on the row by processLease/handleRetry and don't count.
// Lease-lost is propagated via context cancellation by processLease.
// Infrastructure errors (DB closed, sql.ErrConnDone) accumulate in a
// supervisor.FailureTracker; once the consecutive-err threshold trips,
// Run returns the wrapped ErrInfrastructure to the BackgroundSupervisor
// so the ClassRestartable / ClassCritical restart machinery kicks in.
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

	tracker := supervisor.NewFailureTrackerWithClock(supervisor.DefaultRetryPolicy(), supervisor.RealClock{})

	// Initial metrics snapshot on startup.
	r.refreshMetrics(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.stopCh:
			return nil
		case <-ticker.C:
			err := r.tick(ctx)
			if err == nil {
				tracker.Reset()
				continue
			}
			classified := supervisor.ClassifyError(err)
			if escalated := tracker.Record(classified); escalated != nil {
				return fmt.Errorf("forwarding runner: %w", escalated)
			}
			// Per-element errors are already persisted on disk by
			// processLease / handleRetry / handleEnqueueRetry.
			// Lease-lost cancels the in-flight context. Neither
			// needs a log-and-continue entry; the runner silently
			// proceeds to the next tick.
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
// then process each one with bounded concurrency. Errors from the
// inner goroutines are aggregated under a mutex and the FIRST
// classified error is returned to Run so the existing
// supervisor.FailureTracker machinery can route it through the
// ClassRestartable / ClassCritical restart policy. Per-element errors
// are persisted on the row by processLease (so a single bad forwarding
// does not poison the consecutive-error counter); lease-lost cancels
// the in-flight context; infrastructure errors propagate.
func (r *CreatorForwardingRunner) tick(ctx context.Context) error {
	if r.client == nil || !r.client.IsConfigured() {
		return nil // remote creator not configured; no work to do
	}

	// P0-02: cap the claim batch at Concurrency so every claimed lease
	// can acquire the semaphore immediately without waiting. Leases
	// that sit behind the semaphore cannot be renewed (renewLeaseLoop
	// starts only after sem acquisition), so a ClaimBatch > Concurrency
	// creates a window where claimed leases expire before they start
	// processing. Capping at the source also avoids attempt_count
	// inflation from claim-then-release cycles.
	effectiveClaimBatch := r.cfg.ClaimBatch
	if effectiveClaimBatch > r.cfg.Concurrency {
		effectiveClaimBatch = r.cfg.Concurrency
	}
	leases, err := r.dbStore.ClaimCreatorForwardings(ctx, r.identity, "cf", r.cfg.LeaseDuration, effectiveClaimBatch)
	if err != nil {
		return fmt.Errorf("claim forwardings: %w", err)
	}
	if len(leases) == 0 {
		return nil
	}

	r.metrics.Claimed.Add(int64(len(leases)))
	log.Printf("[FORWARDING] claimed %d forwardings", len(leases))

	var (
		wg         sync.WaitGroup
		errMu      sync.Mutex
		aggregated error
	)
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

			if leaseErr := r.processLease(ctx, l); leaseErr != nil {
				errMu.Lock()
				if aggregated == nil {
					aggregated = leaseErr
				}
				errMu.Unlock()
			}
		}(lease)
	}
	wg.Wait()
	return aggregated
}

// processLease handles a single claimed forwarding: polls the remote
// creator, manages lease renewal, and transitions to the appropriate
// next state. Returns an error classified by supervisor.ClassifyError
// so the tick aggregator + FailureTracker can route it through the
// ClassRestartable / ClassCritical restart policy.
//
// Verdetto P0 #1 (Blocco 2): the previous void-returning variant
// produced false-success paths — MarkCreatorForwardingFailed /
// MarkCreatorForwardingRetry failures were only logged, while
// metrics (Failed / Retried) were incremented BEFORE the CAS
// actually persisted. The new contract:
//   - metrics are incremented ONLY after the SQL CAS returns nil
//   - non-nil CAS results return supervisor.ErrElementScoped so
//     the tracker does not count them toward the consecutive-error
//     threshold (they are per-row failures already represented in
//     the row state machine)
//   - lease-lost (procCtx cancelled by the renewal loop) returns
//     supervisor.ErrLeaseLost so the runner does not touch the row
//     (the new lease holder owns it)
//
// Lease-loss propagation: a cancellable processing context (procCtx) is
// created for this lease. The renewal loop receives its cancel function;
// if the lease is lost (RenewCreatorForwardingLease returns
// ErrTransitionConflict), the renewal loop cancels procCtx, causing all
// in-flight operations (GetPipelineStatus, DB writes) to fail with a
// context error. The runner then exits without touching the row — the
// new lease holder owns it.
func (r *CreatorForwardingRunner) processLease(ctx context.Context, lease store.CreatorForwardingLease) error {
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
			return errors.Join(supervisor.ErrLeaseLost, err)
		}
		// Poll error: the per-row retry path is run via handleRetry,
		// which returns an error if the MarkCreatorForwardingRetry
		// CAS failed. The metric increment is owned by handleRetry
		// (post-CAS).
		if retryErr := r.handleRetry(ctx, lease, "POLL_ERROR", err.Error()); retryErr != nil {
			return retryErr
		}
		return nil
	}

	// Defensive nil check: GetPipelineStatus should return (nil, error)
	// on failure, but some HTTP client edge cases (e.g. redirect to
	// empty body) can produce (nil, nil). Treat as a transient poll
	// error rather than panicking on resp.Status.
	if resp == nil {
		log.Printf("[FORWARDING] nil response forwarding=%s source_job=%s: GetPipelineStatus returned nil without error",
			lease.ForwardingID, lease.SourceJobID)
		if retryErr := r.handleRetry(ctx, lease, "NIL_RESPONSE",
			"GetPipelineStatus returned nil response without error"); retryErr != nil {
			return retryErr
		}
		return nil
	}

	// Classify the remote status.
	switch {
	case isTerminalSuccess(resp.Status):
		// Remote creator completed successfully.
		if r.enqueuer != nil {
			// Full atomic lifecycle: build Job+TaskSpec and enqueue+forward
			// in a single SQLite transaction. The metrics + classification
			// are owned by atomicEnqueueAndForward (post-CAS).
			return r.atomicEnqueueAndForward(ctx, lease, resp.Result)
		}
		// Fallback: store payload for a separate forwarding service.
		payloadJSON, payloadSHA256 := marshalPayload(resp.Result)
		if payloadJSON == "" && payloadSHA256 == "" {
			// Non-serializable payload — mark BLOCKED permanently.
			if err := r.dbStore.MarkCreatorForwardingBlocked(ctx,
				lease.ForwardingID, r.identity, lease.LeaseID,
				"PAYLOAD_MARSHAL_ERROR",
				"result payload is not JSON-serializable",
			); err != nil {
				return errors.Join(supervisor.ErrElementScoped,
					fmt.Errorf("mark blocked: %w", err))
			}
			log.Printf("[FORWARDING] payload marshal failed forwarding=%s; marked BLOCKED", lease.ForwardingID)
			r.metrics.Failed.Add(1)
			return nil
		}
		if err := r.dbStore.MarkCreatorForwardingReadyToForward(ctx,
			lease.ForwardingID, r.identity, lease.LeaseID,
			payloadJSON, payloadSHA256,
		); err != nil {
			// CAS failure: persist the retry on the row (if possible)
			// and report the element-scoped error so the tracker
			// does not count it.
			log.Printf("[FORWARDING] mark ready-to-forward failed forwarding=%s: %v", lease.ForwardingID, err)
			if retryErr := r.handleRetry(ctx, lease, "MARK_READY_ERROR", err.Error()); retryErr != nil {
				return retryErr
			}
			return nil
		}
		log.Printf("[FORWARDING] ready-to-forward forwarding=%s source_job=%s source_provider=%s",
			lease.ForwardingID, lease.SourceJobID, lease.SourceProvider)
		r.metrics.Forwarded.Add(1)
		return nil

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
			// CAS failure: keep row visible (a reaper can retry) but report
			// the failure so the supervisor knows the state didn't transition.
			return errors.Join(supervisor.ErrElementScoped,
				fmt.Errorf("mark failed: %w", err))
		}
		log.Printf("[FORWARDING] failed forwarding=%s source_job=%s status=%s",
			lease.ForwardingID, lease.SourceJobID, resp.Status)
		r.metrics.Failed.Add(1)
		return nil

	default:
		// Still running / queued — release the claim immediately so another
		// runner (or the next tick) can pick it up. No backoff: the job is
		// still in progress, not errored.
		nextAttempt := time.Now().UTC() // immediate re-claim eligibility
		if err := r.dbStore.MarkCreatorForwardingRetry(ctx,
			lease.ForwardingID, r.identity, lease.LeaseID,
			"NOT_FINISHED", fmt.Sprintf("remote status: %s", resp.Status),
			nextAttempt,
		); err != nil {
			return errors.Join(supervisor.ErrElementScoped,
				fmt.Errorf("mark retry (still-running): %w", err))
		}
		r.metrics.Retried.Add(1)
		return nil
	}
}

// atomicEnqueueAndForward resolves the forwarding through the canonical
// Resolver. The runner:
//
//  1. Marshals the result map first — non-serializable payloads mark the
//     forwarding BLOCKED before any Resolver call (the Resolver would
//     return a hard marshal error anyway, but BLOCKED is the row state
//     the runner already understands).
//  2. Promotes POLLING → READY_TO_FORWARD via the lease-aware
//     MarkCreatorForwardingReadyToForward transition (the runner has
//     a legitimate lease; the sync handler does not).
//  3. Delegates to Resolver.Resolve with req.ForwardingID = the run's
//     lease id. The Resolver runs idempotency + payload normalization +
//     URL rewriting + atomic CAS in a single place, returning the
//     (job_id, forwarding_id) pair the runner logs.
//
// Returns an error classified by supervisor.ClassifyError. The
// Verdetto P0 #1 contract: metrics (Forwarded) and per-row state
// transitions are persisted only when the corresponding CAS returns
// nil. CAS failures bubble up as supervisor.ErrElementScoped so the
// consecutive-error counter does not include them.
//
// Blocco 5 of the Verdetto (P1 #11): this method is now the single
// path the runner uses for the FORWARDING transition. The legacy
// inline (BuildPayload + PrepareJobAndTask + AtomicForwardAndEnqueue)
// sequence lives only inside the Resolver.
func (r *CreatorForwardingRunner) atomicEnqueueAndForward(ctx context.Context, lease store.CreatorForwardingLease, result map[string]interface{}) error {
	if result == nil {
		result = map[string]interface{}{}
	}

	// 1. Marshal safety check (same semantics as the runner's
	//    pre-resolver code path — non-serializable payloads BLOCK).
	payloadJSON, payloadSHA256 := marshalPayload(result)
	if payloadJSON == "" && payloadSHA256 == "" {
		if err := r.dbStore.MarkCreatorForwardingBlocked(ctx,
			lease.ForwardingID, r.identity, lease.LeaseID,
			"PAYLOAD_MARSHAL_ERROR",
			"enqueue payload is not JSON-serializable",
		); err != nil {
			return errors.Join(supervisor.ErrElementScoped,
				fmt.Errorf("mark blocked: %w", err))
		}
		log.Printf("[FORWARDING] payload marshal failed forwarding=%s; marked BLOCKED", lease.ForwardingID)
		r.metrics.Failed.Add(1)
		return nil
	}

	// 2. POLLING → READY_TO_FORWARD. The runner has a legitimate
	//    lease so the leasable CAS guard applies.
	if err := r.dbStore.MarkCreatorForwardingReadyToForward(ctx,
		lease.ForwardingID, r.identity, lease.LeaseID,
		payloadJSON, payloadSHA256,
	); err != nil {
		log.Printf("[FORWARDING] mark ready-to-forward failed forwarding=%s: %v", lease.ForwardingID, err)
		if retryErr := r.handleRetry(ctx, lease, "MARK_READY_ERROR", err.Error()); retryErr != nil {
			return retryErr
		}
		return nil
	}

	// 3. Delegate to the Resolver. The Resolver applies idempotency
	//    + payload normalization + atomic CAS.
	rs := r.lazyResolver()
	if rs == nil {
		// No enqueuer wired (forwarder-only runner); skip the
		// atomic step. The forwarding row is already READY_TO_FORWARD;
		// a separate forwarder can pick it up via ListReadyToForward.
		log.Printf("[FORWARDING] resolver unavailable for forwarding=%s; row left at READY_TO_FORWARD", lease.ForwardingID)
		return nil
	}
	out, err := rs.Resolve(ctx, creatorflow.ResolveRequest{
		ForwardingID:     lease.ForwardingID,
		SourceProvider:   lease.SourceProvider,
		SourceJobID:      lease.SourceJobID,
		TargetExecutorID: lease.TargetExecutorID,
		Payload:          result,
	})
	if err != nil {
		if errors.Is(err, creatorflow.ErrResolverNotComplete) {
			// Element-scoped: leave row at READY_TO_FORWARD for the
			// next tick to re-run the resolve.
			return errors.Join(supervisor.ErrElementScoped, err)
		}
		log.Printf("[FORWARDING] resolver.Resolve failed forwarding=%s: %v", lease.ForwardingID, err)
		if retryErr := r.handleEnqueueRetry(ctx, lease, "ENQUEUE_FAILED", err.Error()); retryErr != nil {
			return retryErr
		}
		return nil
	}
	if out == nil {
		// Resolver returned ErrResolverNotComplete-equivalent
		// sentinel (nil output normally paired with error, but be
		// conservative). Leave the row in READY_TO_FORWARD so the
		// next tick re-runs the resolve.
		return nil
	}
	log.Printf("[FORWARDING] forwarded forwarding=%s → job=%s source=%s (via Resolver)",
		lease.ForwardingID, out.JobID, lease.SourceProvider)
	r.metrics.Forwarded.Add(1)
	return nil
}

// handleEnqueueRetry transitions the forwarding to RETRY_WAIT with backoff
// when the enqueue phase fails (e.g. payload build error, atomic write
// conflict). Uses MarkCreatorForwardingEnqueueRetry which handles
// FORWARDING/READY_TO_FORWARD states. On max attempts or CAS failure,
// falls back to MarkCreatorForwardingFailed to prevent silent stuck rows.
//
// Returns an error classified by supervisor.ClassifyError. The
// Verdetto P0 #1 contract: metrics (Failed / Retried) are persisted
// only after the underlying SQL CAS returns nil.
func (r *CreatorForwardingRunner) handleEnqueueRetry(ctx context.Context, lease store.CreatorForwardingLease, code, msg string) error {
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
			return errors.Join(supervisor.ErrElementScoped,
				fmt.Errorf("mark failed (max enqueue attempts): %w", err))
		}
		log.Printf("[FORWARDING] max enqueue attempts exhausted forwarding=%s source_job=%s attempts=%d",
			lease.ForwardingID, lease.SourceJobID, lease.AttemptCount)
		r.metrics.Failed.Add(1)
		return nil
	}

	backoff := r.cfg.backoffForAttempt(lease.AttemptCount)
	nextAttempt := time.Now().UTC().Add(backoff)
	if err := r.dbStore.MarkCreatorForwardingEnqueueRetry(ctx,
		lease.ForwardingID, code, msg, nextAttempt,
	); err != nil {
		// CAS failure (race with another runner or already transitioned) —
		// fall back to terminal failure to prevent the row from being
		// silently stuck in FORWARDING/READY_TO_FORWARD forever.
		// With full lease-authority CAS, this is a best-effort safety
		// net: if another runner already claimed the row, the CAS will
		// also fail (which is correct — the new lease holder owns it).
		log.Printf("[FORWARDING] enqueue retry CAS failed forwarding=%s: %v; best-effort FAILED (may no-op if preempted)", lease.ForwardingID, err)
		if ferr := r.dbStore.MarkCreatorForwardingFailed(ctx,
			lease.ForwardingID, r.identity, lease.LeaseID,
			"ENQUEUE_RETRY_CAS_FAILED",
			fmt.Sprintf("CAS failure on enqueue retry: %v", err),
		); ferr != nil {
			return errors.Join(supervisor.ErrElementScoped,
				fmt.Errorf("mark failed (CAS fallback): %w (orig=%v)", ferr, err))
		}
		r.metrics.Failed.Add(1)
		return nil
	}
	r.metrics.Retried.Add(1)
	return nil
}

// handleRetry transitions the forwarding to RETRY_WAIT with the
// backoff schedule applied. If max attempts are exhausted, the
// forwarding is marked FAILED instead.
//
// Returns an error classified by supervisor.ClassifyError. The
// Verdetto P0 #1 contract: metrics (Failed / Retried) are persisted
// only after the underlying SQL CAS returns nil. The caller no
// longer adds the Retried metric — handleRetry owns it.
func (r *CreatorForwardingRunner) handleRetry(ctx context.Context, lease store.CreatorForwardingLease, code, msg string) error {
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
			return errors.Join(supervisor.ErrElementScoped,
				fmt.Errorf("mark failed (max attempts): %w", err))
		}
		log.Printf("[FORWARDING] max attempts exhausted forwarding=%s source_job=%s attempts=%d",
			lease.ForwardingID, lease.SourceJobID, lease.AttemptCount)
		r.metrics.Failed.Add(1)
		return nil
	}

	backoff := r.cfg.backoffForAttempt(lease.AttemptCount)
	nextAttempt := time.Now().UTC().Add(backoff)
	if err := r.dbStore.MarkCreatorForwardingRetry(ctx,
		lease.ForwardingID, r.identity, lease.LeaseID,
		code, msg, nextAttempt,
	); err != nil {
		return errors.Join(supervisor.ErrElementScoped,
			fmt.Errorf("mark retry: %w", err))
	}
	r.metrics.Retried.Add(1)
	return nil
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
