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
	"log"
	"sync"
	"time"

	"velox-server/internal/credentials"
	"velox-server/internal/store"
	"velox-server/internal/supervisor"
)

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
	if err := r.reconcileRecent(ctx); err != nil {
		log.Printf("[DELIVERY] reconciliation sweep: %v", err)
	}
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
