package performance

// derive_telemetry.go owns the production wiring between the pipeline
// RenderMetrics the worker already collects and the single Deriver
// (derive.go): the executor's derived.* attempt-metric projection and
// the raw-fact producer for the useful-work KPI.
//
// Rule (shared with derive.go): this file NEVER computes a ratio. It
// only gathers RAW facts (wall clock, exclusive phases, tree CPU, byte
// totals, process counts) and hands them to Derive — so the derived.*
// telemetry, the receipt's derived section and Prometheus projections
// can never disagree about what a ratio means.

import (
	"strings"

	"velox-shared/telemetry"
	"velox-worker-agent/pkg/video/pipeline"
)

// DerivedFromRenderMetrics computes the full derived-KPI set for an
// attempt from the pipeline render metrics the worker already has. It is
// the executor-side twin of the assembler's rawMetricsFrom path: both
// feed the SAME Deriver with the SAME raw facts, so the derived.*
// attempt telemetry and the receipt's derived section can never
// disagree.
//
//   - wallMs is the attempt wall clock (pipeline.RunMetrics.TotalMs).
//   - clipCount is the render-plan clip inventory (the plan's timeline
//     item count — the raw proxy the executor has for the render-plan
//     clip inventory).
//   - outputBytes is the final artifact size: the engine-declared
//     TotalSize on the early (pre-manifest) projection, then the
//     verified artifact-manifest size on the success path. NOTE: this
//     is a deliberate, documented divergence from the receipt, whose
//     amplification denominator is always the engine-declared
//     IO.FinalBytesWritten (TotalSize); the executor re-verifies the
//     artifact with the manifest, so its amplification is the
//     publisher-authoritative value.
//
// Legacy sidecars without the detailed phases[] stream carry unstamped
// phases: accounted_ratio and useful_work_ratio read 0 ("not measured")
// for them — the same fail-closed behavior as the receipt's Derive.
func DerivedFromRenderMetrics(rm pipeline.RenderMetrics, wallMs int64, clipCount int, outputBytes int64) DerivedMetrics {
	return Derive(RawMetrics{
		WallMs:               wallMs,
		Phases:               assemblePhases(rm),
		CPUWallMS:            rm.CPUUserMs + rm.CPUSystemMs,
		TotalBytesRead:       rm.TotalBytesRead,
		TotalBytesWritten:    rm.TotalBytesWritten,
		OutputBytes:          outputBytes,
		ExternalProcessCount: rm.ExternalProcessCount,
		ClipCount:            clipCount,
		UsefulPipelineMS:     UsefulPipelineMSFromRenderMetrics(rm),
	})
}

// UsefulPipelineMSFromRenderMetrics is the directional producer of the
// "useful media work" observation that feeds Derive.UsefulWorkRatio.
//
// It sums the durations of the detailed sidecar phases that are BOTH
// classified TimingExclusive by the shared catalog AND belong to the
// media engine. Orchestration phases (worker.*/control.*/process/plan
// components: spawn, plan compile/write, queue waits) are excluded, as
// are span children whose parallel instances would double-count against
// the wall clock. The result is a directional observation, not an exact
// split (see DerivedMetrics.UsefulWorkRatio).
//
// A legacy sidecar without the detailed phases[] stream has no
// classification available: the observation stays 0 — "not measured",
// never a measured zero (see RawMetrics.UsefulPipelineMS).
func UsefulPipelineMSFromRenderMetrics(rm pipeline.RenderMetrics) int64 {
	if len(rm.DetailedPhases) == 0 {
		return 0
	}
	var useful int64
	for _, p := range rm.DetailedPhases {
		if classifyPhaseTiming(p) != telemetry.TimingExclusive {
			continue
		}
		if isOrchestrationComponent(p.Component) {
			continue
		}
		useful += p.DurationMS
	}
	return useful
}

// isOrchestrationComponent reports whether a sidecar phase component is
// worker/orchestration plumbing rather than media-engine work.
func isOrchestrationComponent(component string) bool {
	comp := strings.ToLower(strings.TrimSpace(component))
	switch {
	case comp == "" || comp == "unknown":
		// Unnamed components classify via the phase taxonomy; one that
		// reached the exclusive filter carries a media phase label
		// (e.g. "render") — treat it as media work.
		return false
	case strings.HasPrefix(comp, "worker."), strings.HasPrefix(comp, "control."):
		return true
	case comp == "process" || strings.HasPrefix(comp, "process."):
		return true
	case comp == "plan" || strings.HasPrefix(comp, "plan."):
		return true
	default:
		return false
	}
}

// AccountedRatioTarget is the Phase-1 performance budget for the
// explained wall clock (catalog accounted_ratio_rule): once exclusive
// phase collection is complete, accounted_ratio must stay >= 0.95.
// Below that, more than 5% of the wall clock escapes every timer and
// optimizing any measured phase is premature. It is a benchmark/CI
// budget, not a runtime failure: CheckDerivedBudgets reports it and the
// dedicated benchmark worker's fixture gate enforces it.
const AccountedRatioTarget = 0.95

// BudgetViolation is one KPI that missed its documented budget target.
type BudgetViolation struct {
	KPI     string  `json:"kpi"`
	Value   float64 `json:"value"`
	Target  float64 `json:"target"`
	Message string  `json:"message"`
}

// CheckDerivedBudgets compares the derived KPIs against the documented
// performance budgets. Only accounted_ratio has a global Phase-1 target
// (>= 95%); the remaining KPIs get fixture-specific budgets later with
// the BenchmarkFixtureRegistry. A zero value means "not measured", and
// "not measured" is never a violation.
func CheckDerivedBudgets(d DerivedMetrics) []BudgetViolation {
	if d.AccountedRatio > 0 && d.AccountedRatio < AccountedRatioTarget {
		return []BudgetViolation{{
			KPI:     "accounted_ratio",
			Value:   d.AccountedRatio,
			Target:  AccountedRatioTarget,
			Message: "explained wall clock is below the 95% Phase-1 target",
		}}
	}
	return nil
}
