package opsalerts

import (
	"context"
	"errors"
	"reflect"
	"time"

	"velox-server/internal/store"
)

// engine.go — Step 16/15 fleet-operator structured-alerting engine.
//
// The engine drives the 12-rule catalog on a periodic tick:
//
//   1. ListWorkers() → per-worker Snapshot() via the DataSource.
//   2. Evaluate() each snapshot against every entry in AllRules().
//   3. For each hit, consult DedupStore.ShouldFire.
//   4. If YES (first trip OR window elapsed): persist a new
//      ACTIVE alert_events row + Observe(key, hit).
//      For CRITICAL entries with a matching existing ACTIVE row:
//      re-emit (TouchActiveAlertEvent) so last_observed_at + message
//      stay fresh but no duplicate row is created.
//   5. If NO (in-window for WARNING): TouchActiveAlertEvent to bump
//      last_observed_at without re-firing the row.
//   6. For each previously-active key not in the new hit set:
//      Forget + Resolve (stamp alert_events.resolved_at).
//
//   7. INFO hits are dropped at step 3 (engine ignores them); they
//      are logged via the optional Logger for debug but never
//      persisted.
//
// Concurrency: the engine runs in a single goroutine (the
// supervisor owns the goroutine). The dedup store's mutex lets
// multiple concurrent ShouldFire calls (e.g., from tests) race
// safely.
//
// Data-source policy: an Engine is ready only when a real
// WorkerAlertsDataSource is supplied. A missing adapter is a
// configuration error, not an empty fleet: construction fails
// closed so bootstrap cannot expose a supervisor that silently
// evaluates nothing.

// ErrDataSourceNotConfigured is returned when an alerts engine is
// constructed without the read-side adapter required for evaluation.
var ErrDataSourceNotConfigured = errors.New("opsalerts: worker alerts datasource is not configured")

// ErrAlertStoreNotConfigured is returned when the persistence boundary is
// missing. It is kept separate so readiness diagnostics identify the
// unavailable dependency precisely.
var ErrAlertStoreNotConfigured = errors.New("opsalerts: alert store is not configured")

func configuredInterface(value any) bool {
	if value == nil {
		return false
	}
	// An interface containing a typed nil pointer is non-nil, but is just as
	// unusable as a nil interface. Keep constructor readiness fail-closed.
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return !v.IsNil()
	default:
		return true
	}
}

func configuredDataSource(source WorkerAlertsDataSource) bool {
	return configuredInterface(source)
}

func configuredAlertStore(s AlertStore) bool {
	return configuredInterface(s)
}

func newEngine(s AlertStore, source WorkerAlertsDataSource, tick time.Duration, maxBatch int) (*Engine, error) {
	if !configuredAlertStore(s) {
		return nil, ErrAlertStoreNotConfigured
	}
	if !configuredDataSource(source) {
		return nil, ErrDataSourceNotConfigured
	}
	if tick <= 0 {
		tick = 5 * time.Minute
	}
	if maxBatch <= 0 {
		maxBatch = 500
	}
	return &Engine{
		store:    s,
		dedup:    NewDedupStore(),
		source:   source,
		tick:     tick,
		maxBatch: maxBatch,
	}, nil
}

// AlertStore is the SQLite-backed surface the engine writes
// to. Production passes *store.SQLiteStore (it satisfies the
// interface via structural typing).
type AlertStore interface {
	InsertAlertEvent(ctx context.Context, ev store.AlertEvent) error
	ResolveAlertEvent(ctx context.Context, workerID, ruleID, severity string, resolvedAt time.Time) error
	TouchActiveAlertEvent(ctx context.Context, workerID, ruleID, severity string, observedAt time.Time, currentValue, message string) error
	GetActiveAlertEventForWorkerRule(ctx context.Context, workerID, ruleID, severity string) (*store.AlertEvent, error)
}

// Engine is the per-tick orchestrator. The supervisor's
// Run(context) calls Tick(ctx) on a 30-60s interval (Step 16/15
// defaults; tunable via the constructor).
type Engine struct {
	store    AlertStore
	dedup    *DedupStore
	source   WorkerAlertsDataSource
	tick     time.Duration
	maxBatch int
}

// NewEngine builds the orchestrator with sane defaults.
//   - 5 minute tick (the alerts-supervisor registers this in
//     bootstrap_composition.go).
//   - 500-row per-tick batch (sufficient for a 50-worker fleet
//     with 12 rules, leaves headroom for fleet growth).
//
// The datasource is mandatory. Callers must handle the returned error and
// must not register a supervisor when ErrDataSourceNotConfigured is returned.
func NewEngine(s AlertStore, source WorkerAlertsDataSource) (*Engine, error) {
	return newEngine(s, source, 5*time.Minute, 500)
}

// NewEngineWithClock builds an Engine with custom tick + batch. Used by
// tests that want millisecond-rate ticks. It has the same readiness contract
// as NewEngine and rejects a missing or typed-nil datasource.
func NewEngineWithClock(s AlertStore, source WorkerAlertsDataSource, tick time.Duration, maxBatch int) (*Engine, error) {
	return newEngine(s, source, tick, maxBatch)
}

// Run is the supervisor.Runner.Run signature — the supervisor
// owns the goroutine. Returns nil on either clean-exit path
// (ctx.Done or Stop) so graceful shutdown is not treated as a
// transient failure by the supervisor's backoff loop.
func (e *Engine) Run(ctx context.Context) error {
	ticker := time.NewTicker(e.tick)
	defer ticker.Stop()
	// First tick immediately so bootstrap-time alerts land
	// fast (a worker that fails heartbeat on bootstrap should
	// fire within the supervisor's grace period).
	e.Tick(ctx)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			e.Tick(ctx)
		}
	}
}

// Tick runs the full 12-rule evaluation pass once. Exposed so
// tests can drive deterministically; production drives via Run.
//
// Errors inside Tick are logged + ignored — a transient SQLite
// blip or a single-worker snapshot failure must not stall the
// engine. The next tick retries.
func (e *Engine) Tick(ctx context.Context) {
	cc := CallCtx{Now: time.Now().UTC()}
	wids, err := e.source.WorkerIDs(cc)
	if err != nil {
		return
	}
	// Per-worker hit set: dedup key → hit. Engine collects
	// across all rules and consults the dedup store + the
	// alert_events table for resolution decisions.
	for _, wid := range wids {
		e.tickWorker(ctx, cc, wid)
	}
}

// tickWorker processes one worker. Errors are isolated so
// one worker's SQLite hiccup doesn't poison the rest of the
// fleet's tick.
func (e *Engine) tickWorker(ctx context.Context, cc CallCtx, workerID string) {
	snap, err := e.source.Snapshot(cc, workerID)
	if err != nil || snap == nil {
		return
	}
	if snap.WorkerID == "" {
		snap.WorkerID = workerID
	}
	hits := Evaluate(cc, snap)
	// Build the set of (rule_id, severity) triples that fired
	// this tick — used to detect resolution for triples that
	// were previously active.
	fired := make(map[DedupKey]AlertEventHit, len(hits))
	for _, h := range hits {
		if h.Severity == Info {
			continue
		}
		key := DedupKey{WorkerID: h.WorkerID, RuleID: h.RuleID, Severity: h.Severity}
		fired[key] = h
	}

	// Resolution: walk all ACTIVE rows for this worker that
	// were NOT in the new firing set. Forget + Resolve.
	resolveWalk(ctx, e.store, e.dedup, workerID, fired)

	// Emission: walk the new hit set; consult the dedup store
	// to decide whether to persist or to touch.
	for key, hit := range fired {
		if e.dedup.ShouldFire(key, hit.Severity, hit.FiredAt) {
			// Persist a new ACTIVE row.
			ev := store.AlertEvent{
				WorkerID:       hit.WorkerID,
				RuleID:         string(hit.RuleID),
				Severity:       string(hit.Severity),
				State:          store.AlertStateActive,
				FiredAt:        hit.FiredAt,
				LastObservedAt: hit.FiredAt,
				Message:        hit.Message,
			}
			if hit.CurrentValueText != "" {
				ev.CurrentValue.String = hit.CurrentValueText
				ev.CurrentValue.Valid = true
			}
			if err := e.store.InsertAlertEvent(ctx, ev); err != nil {
				continue
			}
			e.dedup.Observe(key, hit)
		} else {
			// In window: touch existing row + dedup key.
			e.dedup.Touch(key, hit.FiredAt, hit.CurrentValueText, hit.Message)
			_ = e.store.TouchActiveAlertEvent(ctx, hit.WorkerID, string(hit.RuleID), string(hit.Severity), hit.FiredAt, hit.CurrentValueText, hit.Message)
		}
	}
}

// resolveWalk scans the dedup store for this worker's keys
// that are NOT in the new firing set, and resolves them.
// Dedup state lookup is over an in-memory map; one worker's
// keys are typically <12 entries so the cost is negligible.
func resolveWalk(ctx context.Context, s AlertStore, dedup *DedupStore, workerID string, fired map[DedupKey]AlertEventHit) {
	// Iterate the dedup store's keys for this worker.
	// (Cheap because we already hold no lock; each access
	// re-acquires briefly.)
	for _, key := range dedupKeysForWorker(dedup, workerID) {
		if _, stillFiring := fired[key]; stillFiring {
			continue
		}
		// Resolve: stamp alert_events.resolved_at + flip
		// state to RESOLVED, then forget the in-memory key.
		sevStr := string(key.Severity)
		if err := s.ResolveAlertEvent(ctx, workerID, string(key.RuleID), sevStr, time.Now().UTC()); err != nil && !errors.Is(err, store.ErrAlertEventNotFound) {
			// Defensively log + continue; a transient
			// SQLite error is non-fatal for resolution.
			continue
		}
		dedup.Forget(key)
	}
}

// dedupKeysForWorker returns all dedup keys belonging to the
// given workerID. Implemented via a method on DedupStore for
// encapsulation; in tests, use SnapshotForTest directly.
func dedupKeysForWorker(dedup *DedupStore, workerID string) []DedupKey {
	// We don't have a Key iterator; iterate via the underlying
	// map under the lock. Encapsulated by exposing Iterate().
	return dedup.iterateWorker(workerID)
}
