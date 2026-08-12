package performance

// gate_tiers.go owns the TWO-TIER gate split (plan §17).
//
// TIER 1 — DETERMINISTIC (safe on ANY runner, runs in normal CI):
//   CheckFixtureGate. Fails hard on invariants that do not depend on
//   neighbor load, CPU model, storage speed or thermal state: execve /
//   encode / decode forbidden, unexpected temp files, unexpected
//   artifact SHA. This is what shared GitHub runners gate on.
//
// TIER 2 — PERFORMANCE (dedicated SELF-HOSTED benchmark worker ONLY):
//   CheckPerformanceBudgets. p50/p95 wall clock, throughput, CPU/wall
//   ratio and read/write amplification are TOO NOISY on shared runners
//   to gate on, so they are evaluated exclusively on the dedicated
//   benchmark worker, against the fixture's PerformanceBudget and
//   IOBudget (all TBD until the Phase-1 baseline is measured).
//
// The rule is structural: the shared-CI workflows never invoke the
// performance tier (scripts/ci/verify.sh + .github/workflows/test.yml
// run Go tests only), and the benchmark-worker workflow runs on
// [self-hosted, benchmark] labels — a self-hosted-only runner label
// that shared jobs cannot use.

import "fmt"

// GateTier names one of the two gate tiers.
type GateTier string

const (
	// TierDeterministic is the invariant gate, safe on any runner
	// (normal CI).
	TierDeterministic GateTier = "deterministic"
	// TierPerformance is the timing/throughput/CPU/amplification gate,
	// enforced ONLY on the dedicated self-hosted benchmark worker.
	TierPerformance GateTier = "performance"
)

// CheckPerformanceBudgets evaluates a whole BenchmarkRun (many
// observations) against the fixture's performance + I/O budgets. This
// is the dedicated benchmark worker's gate: every KPI here is a
// distribution aggregate (p50) or a run-level bound (p95), which is
// exactly why it must never run on shared CI runners.
//
// Aggregation mirrors the runner and the comparison report:
//   - wall p50/p95 from RunSummary;
//   - throughput = DurationSec / (wall p50 seconds) — content-seconds
//     rendered per wall-second (realtime factor), a LOWER bound;
//   - CPU/wall ratio and read amplification as p50 over successful
//     observations;
//   - write amplification from RunSummary p50.
//
// Only enforced (Set=true) budgets are checked; TBD thresholds are
// skipped until the Phase-1 baseline pins them. A nil run or a run
// with zero successful observations yields no violations (nothing
// measured is never a violation).
//
// The run MUST belong to the given fixture: evaluating one fixture's
// numbers against another fixture's budgets would produce an
// authoritative-looking verdict for the wrong track, so a fixture
// mismatch is an error (same guardrail as CompareBenchmarkRuns).
func CheckPerformanceBudgets(fixture BenchmarkFixture, run *BenchmarkRun) ([]BudgetViolation, error) {
	if run == nil {
		return nil, nil
	}
	if run.FixtureID != fixture.ID {
		return nil, fmt.Errorf("performance budgets: run fixture %s != gate fixture %s — only same-fixture runs are comparable",
			run.FixtureID, fixture.ID)
	}
	b := fixture.Budget
	var v []BudgetViolation

	v = evalThreshold(v, "performance.wall_p50", run.Summary.WallMSP50, b.Performance.P50WallMSMax,
		"p50 wall clock over the run must stay under the ceiling")
	v = evalThreshold(v, "performance.wall_p95", run.Summary.WallMSP95, b.Performance.P95WallMSMax,
		"p95 wall clock over the run must stay under the ceiling")
	if run.Summary.WallMSP50 > 0 {
		throughput := float64(fixture.DurationSec) / (run.Summary.WallMSP50 / 1000)
		v = evalThreshold(v, "performance.throughput", throughput, b.Performance.MinThroughput,
			"content-seconds rendered per wall-second must stay above the floor")
	}
	v = evalThreshold(v, "performance.cpu_wall_ratio", p50RatioMetric(run.Receipts, cpuWallRatioOf), b.Performance.MaxCPUWallRatio,
		"CPU/wall ratio must stay under the ceiling (not CPU-bound)")
	v = evalThreshold(v, "io.read_amplification", p50RatioMetric(run.Receipts, readAmpOf), b.IO.ReadAmplificationMax,
		"bytes read per output byte must stay under the ceiling")
	v = evalThreshold(v, "io.write_amplification", run.Summary.WriteAmplificationP50, b.IO.WriteAmplificationMax,
		"bytes written per output byte must stay under the ceiling")
	return v, nil
}

// cpuWallRatioOf extracts the CPU/wall ratio KPI from a receipt.
func cpuWallRatioOf(r *PerformanceReceiptV1) float64 { return r.Derived.CPUWallRatio }
