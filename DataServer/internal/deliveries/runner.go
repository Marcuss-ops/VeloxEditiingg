// Package deliveries runner: DB-driven delivery claim + lease + retry.
//
// DeliveryRunner is the durable analog of the legacy in-handler goroutines
// (maybeAutoUploadDrive). It claims a batch
// of pending/retryable/expired deliveries per tick via the typed
// ClaimDeliveries method (atomic UPDATE+RETURNING with lease columns),
// dispatches to the right provider via the Registry, persists the outcome
// through typed MarkDelivery* methods, and emits outbox events.
//
// Lease + retry semantics (PR4e):
//
//   - claim sets status=RUNNING, lease_id, lease_expires_at, locked_by
//   - on success: MarkDeliverySucceeded (RUNNING → SUCCEEDED)
//   - on transient failure: MarkDeliveryRetry (RUNNING → RETRY_WAIT with backoff)
//   - on permanent failure: MarkDeliveryFailed (RUNNING → FAILED)
//   - on auth failure: MarkDeliveryBlockedAuth (RUNNING → BLOCKED_AUTH)
//   - on rate limit: MarkDeliveryRetry with RetryAfter-based backoff
//   - zombie reclamation: claim picks up RUNNING rows with expired leases
//
// A restart mid-upload resolves cleanly because:
//
//   - the runner only acts on rows where claim succeeded
//   - lease_expires_at is set every tick; zombie deliveries are reclaimed
//     on the next tick after the lease expires
//   - the idempotency_key on (artifact_id, destination_id) prevents the
//     runner from duplicating work on the remote side
//
// File intentionally does NOT spawn goroutines: the caller (cmd/server
// bootstrap) starts one runner and calls Run(ctx) inside a goroutine.
package deliveries

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"velox-server/internal/store"
	"velox-server/internal/supervisor"
)

// RunnerConfig tunes the runner.
type RunnerConfig struct {
	// PollInterval is how often the runner scans for pending deliveries.
	PollInterval time.Duration
	// LeaseDuration is how long a claim is held before another runner
	// can re-claim it. Should be > the worst-case provider latency.
	LeaseDuration time.Duration
	// MaxAttempts per delivery before declaring FAILED.
	MaxAttempts int
	// ClaimBatch limits how many deliveries the runner can claim in a
	// single tick. Should be ≥ Concurrency to keep the pool saturated.
	ClaimBatch int
	// Concurrency limits how many deliveries are processed concurrently.
	// Each delivery gets its own lease renewal goroutine; a bounded pool
	// prevents resource exhaustion. Default 2.
	Concurrency int

	// BackoffSchedule maps attempt number (1-based) to the delay before
	// the next attempt. The last entry is used for all subsequent attempts.
	// Defaults to the canonical schedule: 30s, 2m, 10m, 30m.
	BackoffSchedule []time.Duration
}

// DefaultRunnerConfig returns sensible defaults.
func DefaultRunnerConfig() *RunnerConfig {
	return &RunnerConfig{
		PollInterval:  5 * time.Second,
		LeaseDuration: 5 * time.Minute,
		MaxAttempts:   5,
		ClaimBatch:    4,
		Concurrency:   2,
		BackoffSchedule: []time.Duration{
			30 * time.Second,
			2 * time.Minute,
			10 * time.Minute,
			30 * time.Minute,
		},
	}
}

// backoffForAttempt returns the backoff delay for the given 1-based attempt
// number using the configured schedule. If the attempt exceeds the schedule
// length, the last entry is used.
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

// DeliveryRunner drives delivery_attempts persistence + provider dispatch.
type DeliveryRunner struct {
	cfg      *RunnerConfig
	registry *Registry
	dbStore  *store.SQLiteStore

	sem chan struct{} // bounded concurrency

	mu        sync.Mutex
	stopCh    chan struct{}
	stoppedCh chan struct{}

	// identity holds a stable per-runner id written on delivery_attempts
	// so concurrent runners do not race on the same row.
	identity string
}

// NewDeliveryRunner wires a runner. dbStore is the durable anchor;
// registry supplies provider resolution.
func NewDeliveryRunner(cfg *RunnerConfig, registry *Registry, dbStore *store.SQLiteStore, identity string) *DeliveryRunner {
	if cfg == nil {
		cfg = DefaultRunnerConfig()
	}
	if registry == nil {
		registry = NewRegistry()
	}
	if identity == "" {
		identity = fmt.Sprintf("delivery-runner-%d", time.Now().UnixNano())
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 2
	}
	return &DeliveryRunner{
		cfg:       cfg,
		registry:  registry,
		dbStore:   dbStore,
		identity:  identity,
		sem:       make(chan struct{}, cfg.Concurrency),
		stopCh:    make(chan struct{}),
		stoppedCh: make(chan struct{}),
	}
}

// Run is the durable tick loop. It blocks until ctx is cancelled or Stop is
// called. The loop polls the database at cfg.PollInterval, claims up to
// ClaimBatch claimable deliveries per cycle, and dispatches each to its
// provider through the registry.
//
// Verdetto P1 #10 (Blocco 4): tick errors are CLASSIFIED rather than
// logged-and-continued. Per-element errors (one delivery hit a
// permanent / auth / rate-limit error) are persisted on the row by
// processLease via MarkDeliveryFailed / MarkDeliveryBlockedAuth /
// MarkDeliveryRetry and don't count. Lease-lost cancels the in-flight
// upload via the renewal-loop onFailure callback. Infrastructure errors
// (DB closed, sql.ErrConnDone) accumulate in a supervisor.FailureTracker;
// once the consecutive-error threshold trips, Run returns the wrapped
// ErrInfrastructure to the BackgroundSupervisor so the ClassRestartable /
// ClassCritical restart machinery kicks in.
func (r *DeliveryRunner) Run(ctx context.Context) error {
	if r == nil {
		return errors.New("deliveries: nil runner")
	}
	defer close(r.stoppedCh)

	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	tracker := supervisor.NewFailureTrackerWithClock(supervisor.DefaultRetryPolicy(), supervisor.RealClock{})

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
				return fmt.Errorf("delivery runner: %w", escalated)
			}
			// Per-element errors are already persisted on disk by
			// processLease. Lease-lost cancels the in-flight upload.
			// Neither needs a log-and-continue entry.
		}
	}
}

// Stop signals the runner to exit after the in-flight tick completes.
func (r *DeliveryRunner) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	select {
	case <-r.stopCh:
	default:
		close(r.stopCh)
	}
	<-r.stoppedCh
}

// tick performs one poll: claim up to ClaimBatch claimable deliveries,
// then process each one with bounded concurrency. Each lease starts
// processing immediately — the claim batch is capped at Concurrency so
// no row sits idle in memory with a ticking lease and no heartbeat.
func (r *DeliveryRunner) tick(ctx context.Context) error {
	batch := r.cfg.ClaimBatch
	if r.cfg.Concurrency > 0 && batch > r.cfg.Concurrency {
		batch = r.cfg.Concurrency
	}
	leases, err := r.dbStore.ClaimDeliveries(ctx, r.identity, r.cfg.LeaseDuration, batch)
	if err != nil {
		return fmt.Errorf("claim deliveries: %w", err)
	}
	if len(leases) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	for _, lease := range leases {
		wg.Add(1)
		go func(l store.DeliveryLease) {
			defer wg.Done()
			// Acquire semaphore (bounded concurrency).
			select {
			case r.sem <- struct{}{}:
			case <-ctx.Done():
				log.Printf("[DELIVERY] abandoning claimed lease %s: runner shutting down", l.DeliveryID)
				return
			}
			defer func() { <-r.sem }()

			if err := r.processLease(ctx, l); err != nil {
				log.Printf("[DELIVERY] delivery %s: %v", l.DeliveryID, err)
			}
		}(lease)
	}
	wg.Wait()
	return nil
}

// processLease resolves the provider for a claimed delivery and runs
// Deliver with a heartbeat goroutine that renews the lease every
// leaseDuration/3. If the renewal fails, the deliver context is
// cancelled to interrupt the upload.
//
// Phase 5.5: per-delivery retry_budget. The lease carries
// MaxAttempts (stamped from job_deliveries.max_attempts at
// claim time, which itself was set from
// job_delivery_plans.retry_budget at INSERT time). The runner
// overrides its runner-wide MaxAttempts on a per-delivery
// basis so a destination with a tighter or looser retry
// budget takes effect without a runner restart. A 0
// MaxAttempts falls back to r.cfg.MaxAttempts (the historical
// behavior).
func (r *DeliveryRunner) processLease(ctx context.Context, lease store.DeliveryLease) error {
	// Phase 5.5: per-delivery retry_budget override. The lease
	// carries MaxAttempts from job_deliveries.max_attempts (set
	// from job_delivery_plans.retry_budget at INSERT time). A 0
	// falls back to the runner-wide default for back-compat with
	// rows stamped before Phase 5.5.
	maxAttempts := r.cfg.MaxAttempts
	if lease.MaxAttempts > 0 {
		maxAttempts = lease.MaxAttempts
	}
	provider, err := r.registry.Resolve(lease.Provider)
	if err != nil {
		// Provider not configured → permanent failure.
		if err := r.dbStore.MarkDeliveryFailed(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, "PROVIDER_NOT_CONFIGURED", err.Error()); err != nil {
			log.Printf("[DELIVERY] mark failed for %s: %v", lease.DeliveryID, err)
		}
		return err
	}

	dest, err := r.hydrateDestination(ctx, lease.DestinationID)
	if err != nil {
		// Distinguish DESTINATION_NOT_FOUND (no row) from
		// DESTINATION_UNMAPPED (row exists but social_destination_id is
		// empty — opaque-mode fail-closed contract, see provider.go).
		code := "DESTINATION_NOT_FOUND"
		if errors.Is(err, ErrDestinationUnmapped) {
			code = "DESTINATION_UNMAPPED"
		}
		if mErr := r.dbStore.MarkDeliveryFailed(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, code, err.Error()); mErr != nil {
			log.Printf("[DELIVERY] mark failed for %s: %v", lease.DeliveryID, mErr)
		}
		return fmt.Errorf("hydrate destination: %w", err)
	}
	if metadata, metadataErr := r.dbStore.GetDeliveryPlanMetadata(ctx, lease.ArtifactID, lease.DestinationID); metadataErr != nil {
		return fmt.Errorf("hydrate delivery metadata: %w", metadataErr)
	} else {
		dest.DeliveryMetadataJSON = metadata
	}
	artifact, err := r.hydrateArtifact(ctx, lease.ArtifactID)
	if err != nil {
		if err := r.dbStore.MarkDeliveryFailed(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, "ARTIFACT_NOT_FOUND", err.Error()); err != nil {
			log.Printf("[DELIVERY] mark failed for %s: %v", lease.DeliveryID, err)
		}
		return fmt.Errorf("hydrate artifact: %w", err)
	}

	// Start a heartbeat goroutine to renew the lease periodically while
	// provider.Deliver is executing. If renewal fails (CAS conflict, e.g.
	// another runner reclaimed the lease), cancel the deliver context to
	// interrupt the upload.
	deliverCtx, cancelDeliver := context.WithCancel(ctx)
	defer cancelDeliver()

	heartbeatDone := make(chan struct{})
	go r.renewDeliveryLeaseLoop(deliverCtx, heartbeatDone, lease,
		func(err error) {
			log.Printf("[DELIVERY] lease renewal failed for %s: %v; interrupting upload", lease.DeliveryID, err)
			cancelDeliver()
		})

	res, runErr := provider.Deliver(deliverCtx, artifact, dest, lease.DeliveryID, lease.DeliveryID)

	// Stop the heartbeat goroutine and wait for it to exit.
	cancelDeliver()
	<-heartbeatDone

	// ── Success ──
	if runErr == nil && res != nil && res.Success {
		// Validate the provider result carries verifiable evidence.
		// A Success:true without a remote ID or URL is a programming
		// error in the provider adapter — treat as permanent failure.
		if err := validateProviderResult(res); err != nil {
			log.Printf("[DELIVERY] provider result validation failed for %s: %v", lease.DeliveryID, err)
			if merr := r.dbStore.MarkDeliveryFailed(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, "INVALID_RESULT", err.Error()); merr != nil {
				log.Printf("[DELIVERY] mark failed for %s: %v", lease.DeliveryID, merr)
			}
			return err
		}
		if err := r.dbStore.MarkDeliverySucceeded(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, res.RemoteID, res.RemoteURL); err != nil {
			return fmt.Errorf("mark succeeded: %w", err)
		}
		return nil
	}

	// ── Failure: classify + dispatch ──
	errClass := ClassifyError(runErr)
	errCode := classifyErrorCode(runErr)
	errMsg := ""
	if runErr != nil {
		errMsg = runErr.Error()
	}

	switch errClass {
	case ErrorClassPermanent:
		if err := r.dbStore.MarkDeliveryFailed(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, errCode, errMsg); err != nil {
			log.Printf("[DELIVERY] mark failed for %s: %v", lease.DeliveryID, err)
		}
		return runErr

	case ErrorClassAuth:
		if err := r.dbStore.MarkDeliveryBlockedAuth(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, errCode, errMsg); err != nil {
			log.Printf("[DELIVERY] mark blocked_auth for %s: %v", lease.DeliveryID, err)
		}
		return runErr

	case ErrorClassRateLimit:
		retryAfter := r.resolveRetryAfter(runErr)
		if lease.AttemptNumber >= maxAttempts {
			if err := r.dbStore.MarkDeliveryFailed(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, errCode, "max attempts reached: "+errMsg); err != nil {
				log.Printf("[DELIVERY] mark failed for %s: %v", lease.DeliveryID, err)
			}
			return fmt.Errorf("max attempts reached: %w", runErr)
		}
		backoff := retryAfter.Sub(time.Now().UTC())
		if backoff <= 0 {
			backoff = r.cfg.backoffForAttempt(lease.AttemptNumber)
		}
		nextAttempt := time.Now().UTC().Add(backoff)
		if err := r.dbStore.MarkDeliveryRetry(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, errCode, errMsg, nextAttempt); err != nil {
			log.Printf("[DELIVERY] mark retry for %s: %v", lease.DeliveryID, err)
		}
		return nil

	default: // ErrorClassTransient
		if lease.AttemptNumber >= maxAttempts {
			if err := r.dbStore.MarkDeliveryFailed(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, errCode, "max attempts reached: "+errMsg); err != nil {
				log.Printf("[DELIVERY] mark failed for %s: %v", lease.DeliveryID, err)
			}
			return fmt.Errorf("max attempts reached: %w", runErr)
		}
		backoff := r.cfg.backoffForAttempt(lease.AttemptNumber)
		nextAttempt := time.Now().UTC().Add(backoff)
		if err := r.dbStore.MarkDeliveryRetry(ctx, lease.DeliveryID, lease.RunnerID, lease.LeaseID, errCode, errMsg, nextAttempt); err != nil {
			log.Printf("[DELIVERY] mark retry for %s: %v", lease.DeliveryID, err)
		}
		return nil
	}
}

// renewDeliveryLeaseLoop extends the lease periodically (every
// leaseDuration/3) while provider.Deliver is running. When the deliver
// context is cancelled, the goroutine exits. When a renewal fails (e.g.
// CAS conflict from another runner reclaiming the lease), the onFailure
// callback is invoked so the upload can be interrupted.
func (r *DeliveryRunner) renewDeliveryLeaseLoop(ctx context.Context, done chan<- struct{}, lease store.DeliveryLease, onFailure func(error)) {
	defer close(done)

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
			if err := r.dbStore.RenewDeliveryLease(
				context.Background(), // intentionally detached from request ctx
				lease.DeliveryID, lease.RunnerID, lease.LeaseID, newExpiry,
			); err != nil {
				onFailure(err)
				return
			}
		}
	}
}

// resolveRetryAfter extracts the RetryAfter time from a ProviderError.
// Returns a zero time if the error does not carry RetryAfter.
func (r *DeliveryRunner) resolveRetryAfter(err error) time.Time {
	var pe *ProviderError
	if errors.As(err, &pe) && !pe.RetryAfter.IsZero() {
		return pe.RetryAfter
	}
	return time.Time{}
}

// validateProviderResult checks that a successful provider result carries
// verifiable evidence that the remote side actually created the output.
// A Success:true without at least one of RemoteID or RemoteURL is treated
// as a permanent failure — there is no proof the delivery completed.
func validateProviderResult(res *Result) error {
	if res == nil {
		return fmt.Errorf("%w: result is nil", ErrProviderPermanent)
	}
	if !res.Success {
		return fmt.Errorf("%w: result.Success is false", ErrProviderPermanent)
	}
	if res.RemoteID == "" && res.RemoteURL == "" {
		return fmt.Errorf("%w: both RemoteID and RemoteURL are empty after Success=true — no verifiable output", ErrProviderPermanent)
	}
	return nil
}

// classifyErrorCode produces a short machine-readable code for the error.
func classifyErrorCode(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, ErrProviderNotConfigured) {
		return "PROVIDER_NOT_CONFIGURED"
	}
	if errors.Is(err, ErrProviderPermanent) {
		return "PERMANENT"
	}
	if errors.Is(err, ErrProviderAuth) {
		return "AUTH"
	}
	if errors.Is(err, ErrProviderRateLimit) {
		return "RATE_LIMIT"
	}
	return "TRANSIENT"
}

// hydrateDestination reads delivery_destinations by id and converts the
// internal store type to the deliveries package's Destination shape that
// provider adapters consume.
//
// Opaque-mode fail-closed contract (Residuo 2 of YouTube → Social closure,
// migration 091):
//   * the YouTube-specific fields (AccountID, ChannelID, Language) are gone
//     from the typed Destination;
//   * SocialDestinationID is the opaque identifier resolved server-side by
//     the external Social API; the runner propagates it verbatim;
//   * if SocialDestinationID is empty / whitelist-only, hydrate MUST
//     fail closed with ErrDestinationUnmapped so the runner records
//     DESTINATION_UNMAPPED on the delivery row (operators backfill).
func (r *DeliveryRunner) hydrateDestination(ctx context.Context, destID string) (*Destination, error) {
	d, err := r.dbStore.GetDeliveryDestination(ctx, destID)
	if err != nil {
		return nil, err
	}
	if d == nil {
		return nil, fmt.Errorf("deliveries: destination %s not found", destID)
	}
	if strings.TrimSpace(d.SocialDestinationID) == "" {
		return nil, fmt.Errorf("deliveries: destination %s: %w", destID, ErrDestinationUnmapped)
	}
	cfg := d.ConfigurationJSON
	if cfg == "" {
		cfg = "{}"
	}
	return &Destination{
		DestinationID:       d.DestinationID,
		Provider:            d.Provider,
		SocialDestinationID: d.SocialDestinationID,
		FolderID:            d.FolderID,
		Name:                d.Name,
		Enabled:             d.Enabled,
		ConfigurationJSON:   d.ConfigurationJSON,
		Configuration:       []byte(cfg),
	}, nil
}

// hydrateArtifact reads artifacts by id.
func (r *DeliveryRunner) hydrateArtifact(ctx context.Context, artID string) (*store.Artifact, error) {
	a, err := r.dbStore.GetArtifact(artID)
	if err != nil {
		return nil, err
	}
	return a, nil
}
