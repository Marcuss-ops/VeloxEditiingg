// Package deliveries runner: DB-driven delivery claim + lease + retry.
//
// DeliveryRunner is the durable analog of the legacy in-handler goroutines
// (maybeAutoUploadDrive / maybeAutoUploadYouTube). It claims a single
// pending job_delivery per tick (atomic UPDATE ... WHERE status='PENDING'),
// dispatches to the right provider via the Registry, persists the outcome,
// and emits outbox events. A restart mid-upload resolves cleanly because:
//
//   * the runner only acts on rows where claim succeeded
//   * lease_expires_at is reset every tick; zombie deliveries are claimed
//     again on the next tick after the lease expires
//   * the idempotency_key on (artifact_id, destination_id) prevents the
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
	"sync"
	"time"

	"velox-server/internal/store"
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
	// BackoffBase is the initial transient-error backoff (doubles per
	// failure up to BackoffMax).
	BackoffBase time.Duration
	BackoffMax  time.Duration
	// ClaimBatch limits how many deliveries the runner can claim in a
	// single tick (concurrency).
	ClaimBatch int
}

// DefaultRunnerConfig returns sensible defaults.
func DefaultRunnerConfig() *RunnerConfig {
	return &RunnerConfig{
		PollInterval:  5 * time.Second,
		LeaseDuration: 5 * time.Minute,
		MaxAttempts:   5,
		BackoffBase:   30 * time.Second,
		BackoffMax:    10 * time.Minute,
		ClaimBatch:    4,
	}
}

// DeliveryRunner drives delivery_attempts persistence + provider dispatch.
type DeliveryRunner struct {
	cfg      *RunnerConfig
	registry *Registry
	dbStore  *store.SQLiteStore

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
	return &DeliveryRunner{
		cfg:       cfg,
		registry:  registry,
		dbStore:   dbStore,
		identity:  identity,
		stopCh:    make(chan struct{}),
		stoppedCh: make(chan struct{}),
	}
}

// Run is the durable tick loop. It blocks until ctx is cancelled or Stop is
// called. The loop polls the database at cfg.PollInterval, claims up to
// ClaimBatch pending deliveries per cycle, and dispatches each to its
// provider through the registry.
func (r *DeliveryRunner) Run(ctx context.Context) error {
	if r == nil {
		return errors.New("deliveries: nil runner")
	}
	defer close(r.stoppedCh)

	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.stopCh:
			return nil
		case <-ticker.C:
			if err := r.tick(ctx); err != nil {
				log.Printf("[DELIVERY] tick error: %v", err)
			}
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

// tick performs one poll: claim up to ClaimBatch pending deliveries, run
// each through the registry, persist the outcome. Errors are logged; they
// do not stop the loop.
func (r *DeliveryRunner) tick(ctx context.Context) error {
	rows, err := r.dbStore.ClaimPendingDeliveries(ctx, r.identity, r.cfg.LeaseDuration, r.cfg.ClaimBatch)
	if err != nil {
		return fmt.Errorf("claim pending deliveries: %w", err)
	}
	for _, row := range rows {
		if err := r.processOne(ctx, row); err != nil {
			log.Printf("[DELIVERY] row %v: %v", row, err)
		}
	}
	return nil
}

// processOne resolves the provider for the claimed delivery and runs
// Deliver. The outcome is persisted via UpdateDeliveryAttempt, plus the
// job_deliveries.status is moved to SUCCEEDED, RETRY_WAIT, or FAILED.
func (r *DeliveryRunner) processOne(ctx context.Context, row map[string]interface{}) error {
	providerName, _ := row["provider"].(string)
	dest, err := r.hydrateDestination(ctx, row)
	if err != nil {
		return fmt.Errorf("hydrate destination: %w", err)
	}
	artifact, err := r.hydrateArtifact(ctx, row)
	if err != nil {
		return fmt.Errorf("hydrate artifact: %w", err)
	}

	provider, err := r.registry.Resolve(providerName)
	if err != nil {
		// Provider not configured → permanent failure.
		_ = r.dbStore.UpdateDeliveryAttempt(ctx, asStringFromMap(row, "delivery_id"), "FAILED", "", err.Error())
		_ = r.dbStore.UpdateJobDeliveryStatus(ctx, asStringFromMap(row, "delivery_id"), "FAILED")
		return err
	}

	attemptNumber, _ := row["attempt_number"].(int64)

	res, runErr := provider.Deliver(ctx, artifact, dest)
	if runErr == nil && res != nil && res.Success {
		_ = r.dbStore.UpdateDeliveryAttempt(ctx, asStringFromMap(row, "delivery_id"), "SUCCESS", res.RemoteURL, "")
		_ = r.dbStore.UpdateJobDeliveryStatus(ctx, asStringFromMap(row, "delivery_id"), "SUCCEEDED")
		_ = r.dbStore.UpdateJobDeliveryRemote(ctx, asStringFromMap(row, "delivery_id"), res.RemoteID, res.RemoteURL)
		return nil
	}

	// Failure path: classify + decide retry.
	if errors.Is(runErr, ErrProviderNotConfigured) || errors.Is(runErr, ErrProviderPermanent) {
		_ = r.dbStore.UpdateDeliveryAttempt(ctx, asStringFromMap(row, "delivery_id"), "FAILED", "", runErr.Error())
		_ = r.dbStore.UpdateJobDeliveryStatus(ctx, asStringFromMap(row, "delivery_id"), "FAILED")
		return runErr
	}

	// Transient: retry with backoff up to MaxAttempts.
	if attemptNumber >= int64(r.cfg.MaxAttempts) {
		_ = r.dbStore.UpdateDeliveryAttempt(ctx, asStringFromMap(row, "delivery_id"), "FAILED", "", "max attempts reached")
		_ = r.dbStore.UpdateJobDeliveryStatus(ctx, asStringFromMap(row, "delivery_id"), "FAILED")
		return errors.New("max attempts reached")
	}
	backoff := r.cfg.BackoffBase * time.Duration(1<<uint(attemptNumber-1))
	if backoff > r.cfg.BackoffMax {
		backoff = r.cfg.BackoffMax
	}
	_ = r.dbStore.UpdateDeliveryAttempt(ctx, asStringFromMap(row, "delivery_id"), "RETRY_WAIT", "", runErr.Error())
	_ = r.dbStore.UpdateJobDeliveryStatusWithBackoff(ctx, asStringFromMap(row, "delivery_id"), "PENDING", time.Now().UTC().Add(backoff))
	return nil
}

// hydrateDestination reads delivery_destinations by id and converts the
// internal store type to the deliveries package's Destination shape that
// provider adapters consume.
func (r *DeliveryRunner) hydrateDestination(ctx context.Context, row map[string]interface{}) (*Destination, error) {
	destID, _ := row["destination_id"].(string)
	d, err := r.dbStore.GetDeliveryDestination(ctx, destID)
	if err != nil {
		return nil, err
	}
	if d == nil {
		return nil, fmt.Errorf("deliveries: destination %s not found", destID)
	}
	cfg := d.ConfigurationJSON
	if cfg == "" {
		cfg = "{}"
	}
	return &Destination{
		DestinationID:     d.DestinationID,
		Provider:          d.Provider,
		AccountID:         d.AccountID,
		FolderID:          d.FolderID,
		ChannelID:         d.ChannelID,
		Language:          d.Language,
		Name:              d.Name,
		Enabled:           d.Enabled,
		ConfigurationJSON: d.ConfigurationJSON,
		Configuration:     []byte(cfg),
	}, nil
}

// hydrateArtifact reads artifacts by id.
func (r *DeliveryRunner) hydrateArtifact(ctx context.Context, row map[string]interface{}) (*store.Artifact, error) {
	artID, _ := row["artifact_id"].(string)
	a, err := r.dbStore.GetArtifact(artID)
	if err != nil {
		return nil, err
	}
	return a, nil
}

func asStringFromMap(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
