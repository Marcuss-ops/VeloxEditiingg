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
//
// File split by responsibility:
//   - runner.go        → struct, constructor, Run / Stop / tick
//   - runner_config.go → RunnerConfig + backoff schedule
//   - runner_process.go → processLease + lease renewal loop
//   - runner_helpers.go → result validation, error classification, hydrators
package deliveries

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"velox-server/internal/credentials"
	"velox-server/internal/logging"
	"velox-server/internal/store"
	"velox-server/internal/supervisor"
)

var errDeliveryStatePersistence = errors.New("delivery state persistence failed")

func deliveryStatePersistenceError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w: %s: %w", supervisor.ErrInfrastructure, errDeliveryStatePersistence, operation, err)
}

func joinDeliveryErrors(primary error, persistence ...error) error {
	var joined []error
	if primary != nil {
		joined = append(joined, primary)
	}
	for _, err := range persistence {
		if err != nil {
			joined = append(joined, err)
		}
	}
	return errors.Join(joined...)
}

// DeliveryRunner drives delivery_attempts persistence + provider dispatch.
type DeliveryRunner struct {
	cfg      *RunnerConfig
	registry *Registry
	dbStore  *store.SQLiteStore
	vault    *credentials.Vault

	sem chan struct{} // bounded concurrency

	mu        sync.Mutex
	stopCh    chan struct{}
	stoppedCh chan struct{}

	// identity holds a stable per-runner id written on delivery_attempts
	// so concurrent runners do not race on the same row.
	identity  string
	telemetry Telemetry
	logger    *logging.Logger // structured logger; nil-safe via logInfo/logWarn/logError
}

// WithTelemetry wires the bounded delivery measurement sink.
func (r *DeliveryRunner) WithTelemetry(t Telemetry) *DeliveryRunner {
	if r != nil {
		r.telemetry = t
	}
	return r
}

// WithCredentialVault connects the runner to the central credential
// boundary. Credential-aware providers fail closed when it is not set.
func (r *DeliveryRunner) WithCredentialVault(vault *credentials.Vault) *DeliveryRunner {
	if r != nil {
		r.vault = vault
	}
	return r
}

// WithLogger wires a structured logger for the runner's operator-facing
// events. Defaults to logging.NewLogger("deliveries.runner") at construction;
// tests inject a custom (or nil) logger to silence or redirect output.
func (r *DeliveryRunner) WithLogger(l *logging.Logger) *DeliveryRunner {
	if r != nil {
		r.logger = l
	}
	return r
}

// logInfo/logWarn/logError are nil-safe structured emit helpers so a nil
// injected logger (tests) never panics. They thread ctx so the logger can
// inject trace_id/span_id when an active span is present (GAP 4).
func (r *DeliveryRunner) logInfo(ctx context.Context, code string, fields map[string]interface{}) {
	if r != nil && r.logger != nil {
		r.logger.InfoContext(ctx, code, fields)
	}
}

func (r *DeliveryRunner) logWarn(ctx context.Context, code string, fields map[string]interface{}) {
	if r != nil && r.logger != nil {
		r.logger.WarnContext(ctx, code, fields)
	}
}

func (r *DeliveryRunner) logError(ctx context.Context, code string, fields map[string]interface{}) {
	if r != nil && r.logger != nil {
		r.logger.ErrorContext(ctx, code, fields)
	}
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
		cfg.Concurrency = 4
	}
	return &DeliveryRunner{
		cfg:       cfg,
		registry:  registry,
		dbStore:   dbStore,
		identity:  identity,
		logger:    logging.NewLogger("deliveries.runner"),
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
	if r.dbStore == nil {
		return errors.New("deliveries: runner store is not configured")
	}

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
// then process each one with bounded concurrency. ClaimBatch may exceed
// Concurrency: a lease waiting on the semaphore is renewed by a wait-phase
// heartbeat so it cannot expire before it starts; once a slot is acquired,
// processLease takes over with its own renewal loop.
func (r *DeliveryRunner) tick(ctx context.Context) error {
	if err := r.reconcileRecent(ctx); err != nil {
		r.logWarn(ctx, logging.CodeDeliveryReconcileSweepFail, logging.F("err", err))
	}
	leases, err := r.dbStore.ClaimDeliveries(ctx, r.identity, r.cfg.LeaseDuration, r.cfg.ClaimBatch)
	if err != nil {
		return fmt.Errorf("claim deliveries: %w", err)
	}
	if len(leases) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	stateErrors := make(chan error, len(leases))
	for _, lease := range leases {
		wg.Add(1)
		go func(l store.DeliveryLease) {
			defer wg.Done()

			// Wait-phase renewal (P0-02 amended): ClaimBatch may exceed
			// Concurrency, so a claimed lease can sit behind the semaphore.
			// Renew it here so it cannot expire before it starts. The loop
			// stops as soon as a slot is acquired and processLease starts
			// its own renewal loop, so there is no double renewal.
			waitCtx, cancelWait := context.WithCancel(ctx)
			waitDone := make(chan struct{})
			go r.renewDeliveryLeaseLoop(waitCtx, waitDone, l,
				func(err error) {
					r.logWarn(ctx, logging.CodeDeliveryLeaseRenewalFail, logging.F("delivery", l.DeliveryID, "err", err))
					cancelWait()
				})

			// Acquire semaphore (bounded concurrency); abandon the lease if
			// it is lost (re-claimed) while queued.
			select {
			case r.sem <- struct{}{}:
			case <-waitCtx.Done():
				r.logWarn(ctx, logging.CodeDeliveryLeaseAbandoned, logging.F("delivery", l.DeliveryID))
				return
			}
			cancelWait()
			<-waitDone
			defer func() { <-r.sem }()

			if err := r.processLease(ctx, l); err != nil {
				r.logError(ctx, logging.CodeDeliveryProcessFailed, logging.F("delivery", l.DeliveryID, "err", err))
				if errors.Is(err, errDeliveryStatePersistence) {
					stateErrors <- err
				}
			}
		}(lease)
	}
	wg.Wait()
	select {
	case err := <-stateErrors:
		return fmt.Errorf("delivery state persistence: %w", err)
	default:
	}
	return nil
}
