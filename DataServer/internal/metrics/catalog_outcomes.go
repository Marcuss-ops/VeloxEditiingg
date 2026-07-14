// Package metrics / catalog_outcomes.go
//
// Outcomes family — the four "post-decision" metric families that
// describe what the system *did* with each task rather than how it
// processed it:
//   - scorecard.* : compute-outcome aggregates (render speed,
//     compute-seconds by outcome, failure reasons).
//   - placement.* : placement rejections bucketed by reason.
//   - reconcile.* : reconcile supervisor dispatch counts and
//     commit-deadline-exceeded incidents.
//   - conflict.*  : conflict-budget streak resets, escalations, and
//     streak-length distribution.
package metrics

// outcomesMetricDefinitions returns scorecard.* + placement.* +
// reconcile.* + conflict.* definitions in stable order: scorecard
// first (highest-level outcome rollups), then placement, then
// reconcile, then conflict.
func outcomesMetricDefinitions() []MetricDefinition {
	return []MetricDefinition{
		// ── Scorecard / compute outcomes ─────────────────────────────────
		{
			Name: "scorecard.render_speed_ratio", Unit: "ratio", Component: CompScorecard, Kind: KindGauge,
			Description: "Ratio of media duration to wall clock time (>1 means faster than realtime)",
		},
		{
			Name: "scorecard.compute_seconds_total", Unit: "seconds", Component: CompScorecard, Kind: KindCounter,
			Description: "Total compute seconds classified by outcome (useful, failed, cancelled, stale)",
		},
		{
			Name: "scorecard.failure_reasons_total", Unit: "count", Component: CompScorecard, Kind: KindCounter,
			Description: "Number of failed compute attempts by reason code",
		},
		// ── Placement metrics ────────────────────────────────────────────
		{
			Name: "placement.rejections_total", Unit: "count", Component: CompPlacement, Kind: KindCounter,
			Description: "Placement rejections by reason code (capacity_full, unsupported_executor, etc.)",
		},
		// ── Completion / reconcile ───────────────────────────────────────
		{
			Name: "reconcile.total", Unit: "count", Component: CompReconcile, Kind: KindCounter,
			Description: "Reconcile supervisor dispatch counts by case × action",
		},
		{
			Name: "reconcile.commit_deadline_exceeded", Unit: "count", Component: CompReconcile, Kind: KindCounter,
			Description: "Attempts whose commit_deadline_at crossed without a terminal transition",
		},
		// ── Conflict budget ──────────────────────────────────────────────
		{
			Name: "conflict.streak_reset_total", Unit: "count", Component: CompConflict, Kind: KindCounter,
			Description: "ConflictBudget streak resets on successful CAS operations",
		},
		{
			Name: "conflict.escalations_total", Unit: "count", Component: CompConflict, Kind: KindCounter,
			Description: "ConflictBudget escalations when the consecutive-conflict threshold is crossed",
		},
		{
			Name: "conflict.stayed_under_threshold_total", Unit: "count", Component: CompConflict, Kind: KindCounter,
			Description: "ConflictBudget observations that stayed under the escalation threshold",
		},
		{
			Name: "conflict.streak_length", Unit: "count", Component: CompConflict, Kind: KindHistogram,
			Description: "Distribution of consecutive-conflict streak lengths on the CAS path",
		},
	}
}
