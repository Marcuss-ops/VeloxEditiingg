package opsalerts

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"velox-server/internal/store"
	"velox-server/internal/supervisor"
)

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
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return !v.IsNil()
	default:
		return true
	}
}

func configuredDataSource(source WorkerAlertsDataSource) bool { return configuredInterface(source) }
func configuredAlertStore(s AlertStore) bool                  { return configuredInterface(s) }

// AlertStore is the SQLite-backed surface the engine writes to.
type AlertStore interface {
	InsertAlertEvent(ctx context.Context, ev store.AlertEvent) error
	ResolveAlertEvent(ctx context.Context, workerID, ruleID, severity string, resolvedAt time.Time) error
	TouchActiveAlertEvent(ctx context.Context, workerID, ruleID, severity string, observedAt time.Time, currentValue, message string) error
	GetActiveAlertEventForWorkerRule(ctx context.Context, workerID, ruleID, severity string) (*store.AlertEvent, error)
}

// WorkerEvaluationErrorSink receives aggregated per-worker failures. The
// category is closed and low-cardinality; worker IDs and free-form errors are
// intentionally excluded from metric labels.
type WorkerEvaluationErrorSink interface {
	RecordWorkerEvaluationErrors(category string, count uint64)
}

// Engine evaluates fleet alerts and persists deduplicated events. Failures
// affecting the whole pass are returned to the supervisor. Failures isolated
// to one worker are aggregated and metered while other workers continue.
type Engine struct {
	store        AlertStore
	dedup        *DedupStore
	source       WorkerAlertsDataSource
	tick         time.Duration
	maxBatch     int
	errorMetrics WorkerEvaluationErrorSink
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
	return &Engine{store: s, dedup: NewDedupStore(), source: source, tick: tick, maxBatch: maxBatch}, nil
}

// NewEngine builds the orchestrator with production defaults.
func NewEngine(s AlertStore, source WorkerAlertsDataSource) (*Engine, error) {
	return newEngine(s, source, 5*time.Minute, 500)
}

// NewEngineWithClock builds an Engine with custom tick and batch limits.
func NewEngineWithClock(s AlertStore, source WorkerAlertsDataSource, tick time.Duration, maxBatch int) (*Engine, error) {
	return newEngine(s, source, tick, maxBatch)
}

// SetErrorMetrics installs the optional sink for isolated worker failures.
func (e *Engine) SetErrorMetrics(sink WorkerEvaluationErrorSink) { e.errorMetrics = sink }

// Run is the supervisor runner contract. A global evaluation error is
// returned so supervisor restart/backoff policy can act on it.
func (e *Engine) Run(ctx context.Context) error {
	ticker := time.NewTicker(e.tick)
	defer ticker.Stop()
	if _, err := e.Evaluate(ctx); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if _, err := e.Evaluate(ctx); err != nil {
				return err
			}
		}
	}
}

// Evaluate runs one complete alert pass and returns the alerts observed.
// Worker inventory failures are infrastructure errors and propagate to the
// supervisor. Snapshot and per-worker persistence failures are isolated,
// aggregated by category, and reported through the optional metric sink.
func (e *Engine) Evaluate(ctx context.Context) ([]Alert, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cc := CallCtx{Now: time.Now().UTC()}
	workerIDs, err := e.source.WorkerIDs(cc)
	if err != nil {
		return nil, errors.Join(supervisor.ErrInfrastructure, fmt.Errorf("opsalerts: list workers: %w", err))
	}

	alerts := make([]Alert, 0)
	errorCounts := make(map[string]uint64)
	var infrastructureErrors []error
	for _, workerID := range workerIDs {
		workerAlerts, workerErrors, workerInfrastructureErrors := e.evaluateWorker(ctx, cc, workerID)
		alerts = append(alerts, workerAlerts...)
		for category, count := range workerErrors {
			errorCounts[category] += count
		}
		infrastructureErrors = append(infrastructureErrors, workerInfrastructureErrors...)
	}
	for category, count := range errorCounts {
		if e.errorMetrics != nil {
			e.errorMetrics.RecordWorkerEvaluationErrors(category, count)
		}
	}
	if len(infrastructureErrors) > 0 {
		return alerts, errors.Join(append([]error{supervisor.ErrInfrastructure}, infrastructureErrors...)...)
	}
	return alerts, nil
}

func (e *Engine) evaluateWorker(ctx context.Context, cc CallCtx, workerID string) ([]Alert, map[string]uint64, []error) {
	errorsByCategory := make(map[string]uint64)
	var infrastructureErrors []error
	snap, err := e.source.Snapshot(cc, workerID)
	if err != nil {
		if supervisor.IsInfrastructure(supervisor.ClassifyError(err)) {
			infrastructureErrors = append(infrastructureErrors, fmt.Errorf("opsalerts: snapshot worker=%s: %w", workerID, err))
		} else {
			errorsByCategory["snapshot"]++
		}
		return nil, errorsByCategory, infrastructureErrors
	}
	if snap == nil {
		errorsByCategory["snapshot_empty"]++
		return nil, errorsByCategory, infrastructureErrors
	}

	if snap.WorkerID == "" {
		snap.WorkerID = workerID
	}

	hits := evaluateSnapshot(cc, snap)
	fired := make(map[DedupKey]Alert, len(hits))
	for _, hit := range hits {
		if hit.Severity == Info {
			continue
		}
		key := DedupKey{WorkerID: hit.WorkerID, RuleID: hit.RuleID, Severity: hit.Severity}
		fired[key] = hit
	}

	resolveFailures, resolveInfrastructureErrors := e.resolveWorker(ctx, workerID, fired)
	errorsByCategory["store_resolve"] += resolveFailures
	infrastructureErrors = append(infrastructureErrors, resolveInfrastructureErrors...)
	for key, hit := range fired {
		if e.dedup.ShouldFire(key, hit.Severity, hit.FiredAt) {
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
				if supervisor.IsInfrastructure(supervisor.ClassifyError(err)) {
					infrastructureErrors = append(infrastructureErrors, fmt.Errorf("opsalerts: insert alert worker=%s: %w", workerID, err))
				} else {
					errorsByCategory["store_insert"]++
				}
				continue
			}
			e.dedup.Observe(key, hit)
			continue
		}
		e.dedup.Touch(key, hit.FiredAt, hit.CurrentValueText, hit.Message)
		if err := e.store.TouchActiveAlertEvent(ctx, hit.WorkerID, string(hit.RuleID), string(hit.Severity), hit.FiredAt, hit.CurrentValueText, hit.Message); err != nil {
			if supervisor.IsInfrastructure(supervisor.ClassifyError(err)) {
				infrastructureErrors = append(infrastructureErrors, fmt.Errorf("opsalerts: touch alert worker=%s: %w", workerID, err))
			} else {
				errorsByCategory["store_touch"]++
			}
		}
	}
	return hits, errorsByCategory, infrastructureErrors
}

func (e *Engine) resolveWorker(ctx context.Context, workerID string, fired map[DedupKey]Alert) (uint64, []error) {
	var failures uint64
	var infrastructureErrors []error
	for _, key := range dedupKeysForWorker(e.dedup, workerID) {
		if _, stillFiring := fired[key]; stillFiring {
			continue
		}
		if err := e.store.ResolveAlertEvent(ctx, workerID, string(key.RuleID), string(key.Severity), time.Now().UTC()); err != nil && !errors.Is(err, store.ErrAlertEventNotFound) {
			if supervisor.IsInfrastructure(supervisor.ClassifyError(err)) {
				infrastructureErrors = append(infrastructureErrors, fmt.Errorf("opsalerts: resolve alert worker=%s: %w", workerID, err))
			} else {
				failures++
			}
			continue
		}
		e.dedup.Forget(key)
	}
	return failures, infrastructureErrors
}

func dedupKeysForWorker(dedup *DedupStore, workerID string) []DedupKey {
	return dedup.iterateWorker(workerID)
}
