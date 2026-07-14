// Package metrics / collector_sinks.go
//
// External-package sink interfaces + their impls on *Collector, sliced
// out of collector.go so the Collector struct definition stays focused
// on registration. The sink pattern lets package-boundary callers
// (gRPC handler, completion supervisor, ingest) bind to a narrow
// declared interface instead of importing the full *Collector.
//
// Three sink contracts live here:
//
//   - PlacementRejectionSink  (gRPC handler matches candidates skipped
//     by the placement matcher)
//   - ConflictBudgetSink      (local interface mirrored in completion
//     package; the conflict-state-machine
//     triplet Reset/Observe/Escalate)
//   - IncReconcile / IncCommitDeadlineExceeded counters wired to
//     the completion reconcile supervisor
//   - RecordErrorClassification (Scorecard v2 / Step 13) stamps the
//     canonical (error_code, component, phase) family
//
// Compile-time guards var _ X = (*Collector)(nil) hold each contract;
// structural typing matches downstream consumers through Go's
// implicit interface implementation.
package metrics

// RecordPlacementRejection increments velox_placement_rejections_total
// for a single reason code. Called from the gRPC handler's placement
// pipeline: recordPlacementRejections for matcher-side skips and
// handleUnsupportedExecutorRejection for worker-side executor mismatches.
func (c *Collector) RecordPlacementRejection(reason string) {
	c.placementRejections.Inc([]string{reason}, 1)
}

// PlacementRejectionSink is the contract the gRPC handler depends on
// for forwarding placement rejection counters onto the Prometheus
// registry. Defined here (consumed-by-handler) following the same
// pattern as WorkerResourceSink.
//
// The placement pipeline calls RecordPlacementRejection for every
// candidate the placement matcher skipped, producing a per-reason
// time series (e.g. capacity_full, unsupported_executor).
type PlacementRejectionSink interface {
	RecordPlacementRejection(reason string)
}

// Compile-time guard: *Collector implements PlacementRejectionSink.
var _ PlacementRejectionSink = (*Collector)(nil)

// ConflictBudgetSink is the contract the completion package depends
// on for forwarding ConflictBudget state-machine transitions onto
// the Prometheus registry. The completion package owns a local
// structurally-identical interface (also named ConflictBudgetSink)
// to avoid a metrics→completion import; Go's structural typing
// matches the metrics.Collector method set to the local interface
// when bootstrap wires it up.
//
// Three semantically distinct calls so the test surface can mock
// each transition independently:
//
//   - ResetConflictBudget()                 — Record(nil) on a non-zero streak
//     (real reset, not a no-op reset).
//   - ObserveConflictStreakUnderThreshold(streak int)
//     — ErrTransitionConflict incremented
//     the streak but stayed under
//     threshold (counter + histogram
//     observation).
//   - EscalateConflictBudget(streak int)     — threshold crossed, the budget
//     returns ErrConflictBudgetExhausted
//     (counter + histogram observation
//     at the runup length, which is
//     the value of streak at the
//     escalation decision).
//
// The histogram observation on the escalation path is INTENTIONAL:
// it captures the runup length just before the threshold is crossed,
// which lets dashboards show the pre-escalation distribution alongside
// the under-threshold distribution.
type ConflictBudgetSink interface {
	ResetConflictBudget()
	ObserveConflictStreakUnderThreshold(streak int)
	EscalateConflictBudget(streak int)
}

// Compile-time guard: *Collector implements ConflictBudgetSink.
// Bootstrap wires the collector into the coordinator via the local
// completion.ConflictBudgetSink interface; structural typing matches.
var _ ConflictBudgetSink = (*Collector)(nil)

// ResetConflictBudget increments velox_conflict_streak_reset_total
// once per REAL reset (Record(nil) on a non-zero streak). No-op
// resets (streak already zero) deliberately do not increment so the
// counter measures actual transition density, not exit-rate noise.
func (c *Collector) ResetConflictBudget() {
	c.conflictStreakReset.Inc([]string{}, 1)
}

// ObserveConflictStreakUnderThreshold increments
// velox_conflict_stayed_under_threshold_total AND observes the
// histogram at the current streak length. Called inside Record
// for ErrTransitionConflict observations that did NOT cross the
// threshold. streak <= 0 is a no-op (the budget never decrements
// the counter on non-conflict inputs).
func (c *Collector) ObserveConflictStreakUnderThreshold(streak int) {
	if streak <= 0 {
		return
	}
	c.conflictStayedUnder.Inc([]string{}, 1)
	c.conflictStreakLength.Observe([]string{}, float64(streak))
}

// EscalateConflictBudget increments velox_conflict_escalations_total
// AND observes the histogram at the runup length. Called inside
// Record for ErrTransitionConflict observations that crossed the
// threshold; the same observation point records the runup shape.
func (c *Collector) EscalateConflictBudget(streak int) {
	if streak <= 0 {
		return
	}
	c.conflictEscalations.Inc([]string{}, 1)
	c.conflictStreakLength.Observe([]string{}, float64(streak))
}

// IncReconcile stamps one observation on the reconcile supervisor's
// {case, action} counter. Called from internal/completion's
// ReconcileSupervisor after every Coordinator.ReconcileAttempt
// dispatch (and once for every deadline-expired row that the
// coordinator couldn't reach in this tick). The case/action
// dimensions are exposed as strings on the metric labels.
//
// Compile-time guard: the *Collector satisfies
// completion.ReconcileMetrics — wiring mistakes break loudly at
// build time.
func (c *Collector) IncReconcile(caseLabel, actionLabel string) {
	if string(caseLabel) == "" || string(actionLabel) == "" {
		// Surface malformed labels loudly (a programming error in
		// the supervisor); an empty label would expose an invalid
		// series and make PromQL aggregations silently wrong.
		return
	}
	c.reconcileTotal.Inc([]string{string(caseLabel), string(actionLabel)}, 1)
}

// IncCommitDeadlineExceeded stamps one observation on the deadline
// counter. Called once per attempt whose commit_deadline_at has
// crossed without a terminal transition. Distinct from the
// {case,action} counter because a single tick can produce multiple
// deadline-expired rows and a single row can be observed across
// ticks (the seenIDs dedup map is bounded by seenCap).
func (c *Collector) IncCommitDeadlineExceeded() {
	c.commitDeadlineExceeded.Inc([]string{}, 1)
}

// RecordErrorClassification increments velox_error_classification_total
// for a single error observation. All three labels are low-cardinality
// closed enums — never pass job_id or free-form strings here.
// errorCode must be a CanonicalErrorCode; component must be from
// CanonicalErrorComponents; phase must be from CanonicalErrorPhases.
// Empty strings default to "unknown".
func (c *Collector) RecordErrorClassification(errorCode, component, phase string) {
	if errorCode == "" {
		errorCode = "UNKNOWN"
	}
	if component == "" {
		component = "unknown"
	}
	if phase == "" {
		phase = "unknown"
	}
	c.errorClassification.Inc([]string{errorCode, component, phase}, 1)
}
