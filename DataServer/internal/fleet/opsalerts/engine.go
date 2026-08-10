package opsalerts

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	runtimealerts "velox-server/internal/alerts"
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

// DataSourceConfigured reports whether a datasource is safe to use,
// including the typed-nil interface case at composition boundaries.
func DataSourceConfigured(source WorkerAlertsDataSource) bool {
	return configuredDataSource(source)
}

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

var _ runtimealerts.Evaluator = (*Engine)(nil)

// Engine evaluates fleet alerts and persists deduplicated events. Failures
// affecting the whole pass are returned to the supervisor. Failures isolated
// to one worker are aggregated and metered while other workers continue.
type Engine struct {
	store               AlertStore
	dedup               *DedupStore
	source              WorkerAlertsDataSource
	tick                time.Duration
	maxBatch            int
	errorMetrics        WorkerEvaluationErrorSink
	runtimeErrorMetrics runtimealerts.ErrorMetrics
	pipeline            *runtimealerts.Pipeline
	runtime             *runtimealerts.Runtime
	passMu              sync.Mutex
	evaluateMu          sync.Mutex
	passWorkers         []string
	passSkipped         map[string]struct{}
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
	engine := &Engine{store: s, dedup: NewDedupStore(), source: source, tick: tick, maxBatch: maxBatch}
	// The fleet evaluator owns severity-aware deduplication and the touch/
	// resolve paths below. Keep the mandatory persistence sink in the claim
	// pipeline; optional notification sinks run only after persistence commits.
	// This prevents a notifier failure from releasing the claim and causing a
	// later tick to insert the same alert again.
	engine.pipeline = runtimealerts.NewPipeline(
		fleetRuntimeDedup{dedup: engine.dedup},
		fleetPersistenceSink{store: s},
	)
	engine.pipeline.OnSuppressed = func(ctx context.Context, event runtimealerts.AlertEvent) error {
		currentValue := ""
		if event.Labels != nil {
			currentValue = event.Labels["current_value"]
		}
		if err := s.TouchActiveAlertEvent(ctx, event.Subject, event.RuleID, event.Severity, event.FiredAt, currentValue, event.Description); err != nil {
			return runtimealerts.SinkError{Stage: "persistence", Err: err}
		}
		engine.dedup.Touch(DedupKey{WorkerID: event.Subject, RuleID: RuleID(event.RuleID), Severity: Severity(event.Severity)}, event.FiredAt, currentValue, event.Description)
		return nil
	}
	engine.runtime = runtimealerts.NewRuntime(engine, engine.pipeline, tick)
	engine.runtime.BeforeDispatch = engine.beforeDispatch
	engine.runtime.NormalizeDispatchError = engine.normalizeDispatchError
	return engine, nil
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
// A typed-nil sink is ignored so an error path cannot panic during partial
// composition.
func (e *Engine) SetErrorMetrics(sink WorkerEvaluationErrorSink) {
	if !configuredInterface(sink) {
		e.errorMetrics = nil
		return
	}
	e.errorMetrics = sink
}

// SetRuntimeErrorMetrics installs the shared low-cardinality sink for
// global evaluation failures that must also be propagated to supervisor.
func (e *Engine) SetRuntimeErrorMetrics(sink runtimealerts.ErrorMetrics) {
	if runtimealerts.ErrorMetricsConfigured(sink) {
		e.runtimeErrorMetrics = sink
		return
	}
	e.runtimeErrorMetrics = nil
}

func (e *Engine) recordRuntimeError(category string, count uint64) {
	if e.runtimeErrorMetrics != nil && count > 0 {
		e.runtimeErrorMetrics.RecordAlertEvaluationError("fleet", category, count)
	}
}

// AddSink registers an optional notification or secondary side-effect sink
// in the shared post-commit pipeline. Call during bootstrap before the engine
// starts; sinks must be idempotent by AlertEvent.EventID.
func (e *Engine) AddSink(sink runtimealerts.Sink) {
	if e == nil || sink == nil || e.pipeline == nil {
		return
	}
	e.pipeline.AddAfterCommitSink(sink)
}

func (e *Engine) normalizeDispatchError(err error) error {
	if err == nil {
		return nil
	}
	classified := supervisor.ClassifyError(err)
	if supervisor.IsInfrastructure(classified) {
		e.recordRuntimeError("infrastructure", 1)
		return classified
	}
	if runtimealerts.StageOf(err) == "persistence" {
		if e.errorMetrics != nil {
			e.errorMetrics.RecordWorkerEvaluationErrors("store_insert", 1)
		}
	} else if runtimealerts.StageOf(err) == "notifier" {
		if e.errorMetrics != nil {
			e.errorMetrics.RecordWorkerEvaluationErrors("notifier", 1)
		}
	} else if e.errorMetrics != nil {
		e.errorMetrics.RecordWorkerEvaluationErrors("pipeline", 1)
	}
	return nil
}

type fleetRuntimeDedup struct{ dedup *DedupStore }

func (d fleetRuntimeDedup) Prepare(event runtimealerts.AlertEvent, now time.Time) runtimealerts.AlertEvent {
	return d.dedup.Prepare(event, now)
}

func (d fleetRuntimeDedup) EventID(event runtimealerts.AlertEvent, now time.Time) string {
	return d.dedup.EventID(event, now)
}

func (d fleetRuntimeDedup) Claim(event runtimealerts.AlertEvent, now time.Time) (runtimealerts.Claim, bool) {
	key := DedupKey{WorkerID: event.Subject, RuleID: RuleID(event.RuleID), Severity: Severity(event.Severity)}
	hit := AlertEventHit{
		WorkerID:         event.Subject,
		RuleID:           key.RuleID,
		Severity:         key.Severity,
		CurrentValueText: event.Labels["current_value"],
		Message:          event.Description,
		FiredAt:          event.FiredAt,
	}
	claim, ok := d.dedup.Claim(key, key.Severity, now, hit)
	if !ok {
		return nil, false
	}
	return claimAdapter{claim: claim}, true
}

type claimAdapter struct {
	claim interface {
		Commit()
		Release()
	}
}

func (c claimAdapter) Commit()  { c.claim.Commit() }
func (c claimAdapter) Release() { c.claim.Release() }

type fleetPersistenceSink struct{ store AlertStore }

func (s fleetPersistenceSink) Process(ctx context.Context, event runtimealerts.AlertEvent) error {
	currentValue := ""
	if event.Labels != nil {
		currentValue = event.Labels["current_value"]
	}
	if err := s.store.InsertAlertEvent(ctx, store.AlertEvent{
		EventID:  event.EventID,
		WorkerID: event.Subject,
		RuleID:   event.RuleID,
		Severity: event.Severity,

		State:          store.AlertStateActive,
		FiredAt:        event.FiredAt,
		LastObservedAt: event.FiredAt,
		CurrentValue:   sql.NullString{String: currentValue, Valid: currentValue != ""},
		Message:        event.Description,
	}); err != nil {
		return runtimealerts.SinkError{Stage: "persistence", Err: err}
	}
	return nil
}

// Run delegates the complete fleet lifecycle to the common runtime:
// evaluator → event → dedup → persistence/notifier.
func (e *Engine) Run(ctx context.Context) error { return e.runtime.Run(ctx) }

// RunOnce exposes one complete shared runtime pass for compatibility tests
// and administrative pre-flight checks without making Evaluate perform side
// effects.
func (e *Engine) RunOnce(ctx context.Context) error { return e.runtime.RunOnce(ctx) }

func (e *Engine) beforeDispatch(ctx context.Context, events []runtimealerts.AlertEvent) error {
	e.passMu.Lock()
	workers := append([]string(nil), e.passWorkers...)
	e.passMu.Unlock()
	firedByWorker := make(map[string]map[DedupKey]Alert, len(workers))
	for _, event := range events {
		if firedByWorker[event.Subject] == nil {
			firedByWorker[event.Subject] = make(map[DedupKey]Alert)
		}
		firedByWorker[event.Subject][DedupKey{WorkerID: event.Subject, RuleID: RuleID(event.RuleID), Severity: Severity(event.Severity)}] = Alert{
			WorkerID: event.Subject,
			RuleID:   RuleID(event.RuleID),
			Severity: Severity(event.Severity),
		}
	}
	var errs []error
	e.passMu.Lock()
	skipped := make(map[string]struct{}, len(e.passSkipped))
	for workerID := range e.passSkipped {
		skipped[workerID] = struct{}{}
	}
	e.passMu.Unlock()
	for _, workerID := range workers {
		if _, ok := skipped[workerID]; ok {
			continue
		}
		failures, infrastructureErrors := e.resolveWorker(ctx, workerID, firedByWorker[workerID])
		if failures > 0 && e.errorMetrics != nil {
			e.errorMetrics.RecordWorkerEvaluationErrors("store_resolve", failures)
		}
		errs = append(errs, infrastructureErrors...)
	}
	return errors.Join(errs...)
}

// Evaluate runs one complete alert pass and returns the alerts observed.
// Worker inventory failures are infrastructure errors and propagate to the
// supervisor. Snapshot and per-worker persistence failures are isolated,
// aggregated by category, and reported through the optional metric sink.
func (e *Engine) Evaluate(ctx context.Context) ([]runtimealerts.AlertEvent, error) {
	e.evaluateMu.Lock()
	defer e.evaluateMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cc := CallCtx{Now: time.Now().UTC()}
	workerIDs, err := e.source.WorkerIDs(cc)
	if err != nil {
		e.recordRuntimeError("inventory", 1)
		return nil, errors.Join(supervisor.ErrInfrastructure, fmt.Errorf("opsalerts: list workers: %w", err))
	}

	alerts := make([]runtimealerts.AlertEvent, 0)
	errorCounts := make(map[string]uint64)
	var infrastructureErrors []error
	e.passMu.Lock()
	e.passWorkers = append(e.passWorkers[:0], workerIDs...)
	e.passSkipped = make(map[string]struct{})
	e.passMu.Unlock()
	for _, workerID := range workerIDs {
		workerAlerts, workerErrors, workerInfrastructureErrors := e.evaluateWorker(ctx, cc, workerID)
		alerts = append(alerts, workerAlerts...)
		for category, count := range workerErrors {
			errorCounts[category] += count
		}
		infrastructureErrors = append(infrastructureErrors, workerInfrastructureErrors...)
		if len(workerErrors) > 0 || len(workerInfrastructureErrors) > 0 {
			e.passMu.Lock()
			e.passSkipped[workerID] = struct{}{}
			e.passMu.Unlock()
		}
	}
	for category, count := range errorCounts {
		if e.errorMetrics != nil {
			e.errorMetrics.RecordWorkerEvaluationErrors(category, count)
		}
	}
	if len(infrastructureErrors) > 0 {
		e.recordRuntimeError("infrastructure", uint64(len(infrastructureErrors)))
		return alerts, errors.Join(append([]error{supervisor.ErrInfrastructure}, infrastructureErrors...)...)
	}
	return alerts, nil
}

func (e *Engine) evaluateWorker(ctx context.Context, cc CallCtx, workerID string) ([]runtimealerts.AlertEvent, map[string]uint64, []error) {
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

	sharedEvents := make([]runtimealerts.AlertEvent, 0, len(fired))
	for _, hit := range fired {
		event := runtimealerts.AlertEvent{
			Group:       runtimealerts.GroupFleet,
			RuleID:      string(hit.RuleID),
			Severity:    string(hit.Severity),
			Subject:     hit.WorkerID,
			Summary:     hit.Message,
			Description: hit.Message,
			Labels:      map[string]string{"current_value": hit.CurrentValueText},
			FiredAt:     hit.FiredAt,
		}
		sharedEvents = append(sharedEvents, event)
	}
	return sharedEvents, errorsByCategory, infrastructureErrors
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
