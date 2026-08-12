package performance

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// perfTestRun builds a run with the given wall samples and per-receipt
// CPU ratio + read amplification, computing the summary through the
// runner's own aggregation.
func perfTestRun(walls []float64, cpuRatio, readAmp float64) *BenchmarkRun {
	obs := make([]BenchmarkRunObservation, 0, len(walls))
	for i, w := range walls {
		r := NewPerformanceReceiptV1()
		r.Timing.WallMs = int64(w)
		r.Derived.CPUWallRatio = cpuRatio
		r.Derived.ReadAmplification = readAmp
		r.Derived.WriteAmplification = readAmp * 0.8
		obs = append(obs, BenchmarkRunObservation{RunIndex: i, WallMS: int64(w), Receipt: r})
	}
	return &BenchmarkRun{
		FixtureID: FixtureCopyOnlyCanonical5MV1,
		Receipts:  obs,
		Summary:   summarizeObservations(obs),
	}
}

// pinPerfBudgets sets every performance/IO budget on a fixture copy so
// a test can exercise the tier-2 gate end to end.
func pinPerfBudgets(f BenchmarkFixture, opts ...func(*BenchmarkFixture)) BenchmarkFixture {
	f.Budget.Performance.P50WallMSMax = MaxInt64(5000)
	f.Budget.Performance.P95WallMSMax = MaxInt64(6500)
	f.Budget.Performance.MinThroughput = MinFloat(40) // 300 s content in <= 7.5 s
	f.Budget.Performance.MaxCPUWallRatio = MaxFloat(0.5)
	f.Budget.IO.ReadAmplificationMax = MaxFloat(3.0)
	f.Budget.IO.WriteAmplificationMax = MaxFloat(1.5)
	for _, o := range opts {
		o(&f)
	}
	return f
}

// TestCheckPerformanceBudgets_Pass pins the happy path on the canonical
// fixture: a run inside every budget produces no violations.
func TestCheckPerformanceBudgets_Pass(t *testing.T) {
	reg := NewBenchmarkFixtureRegistry()
	f, _ := reg.Fixture(FixtureCopyOnlyCanonical5MV1)
	f = pinPerfBudgets(f)

	// wall p50 4000 ms → throughput = 300 / 4.0 = 75 (>= 40), CPU 0.3,
	// read amp 1.4, write amp 1.12 — all within budget.
	run := perfTestRun([]float64{4000, 4000, 4000}, 0.3, 1.4)
	v, err := CheckPerformanceBudgets(f, run)
	require.NoError(t, err)
	require.Empty(t, v)
}

// TestCheckPerformanceBudgets_Violations pins every KPI of the
// performance tier, including the Min direction of the throughput
// budget (a lower bound, violated when below).
func TestCheckPerformanceBudgets_Violations(t *testing.T) {
	reg := NewBenchmarkFixtureRegistry()
	f, _ := reg.Fixture(FixtureCopyOnlyCanonical5MV1)
	f = pinPerfBudgets(f)

	// wall p50 8000 > 5000, p95 9000 > 6500, throughput 300/8 = 37.5 <
	// 40, CPU 1.2 > 0.5, read amp 4.0 > 3.0, write amp p50 of
	// [4.0,3.0,3.0] = 3.0 > 1.5.
	run := perfTestRun([]float64{9000, 8000, 7000}, 1.2, 4.0)
	run.Receipts[0].Receipt.Derived.WriteAmplification = 4.0
	run.Receipts[1].Receipt.Derived.WriteAmplification = 3.0
	run.Receipts[2].Receipt.Derived.WriteAmplification = 3.0
	run.Summary = summarizeObservations(run.Receipts)

	v, err := CheckPerformanceBudgets(f, run)
	require.NoError(t, err)
	require.Len(t, v, 6)
	got := map[string]BudgetViolation{}
	for _, bv := range v {
		got[bv.KPI] = bv
	}
	require.Equal(t, "performance.wall_p50", got["performance.wall_p50"].KPI)
	require.InDelta(t, 8000, got["performance.wall_p50"].Value, 1e-9)
	require.InDelta(t, 5000, got["performance.wall_p50"].Target, 1e-9)
	require.Equal(t, "performance.wall_p95", got["performance.wall_p95"].KPI)
	// throughput: 37.5 < 40 (Min direction — violated below the floor)
	require.InDelta(t, 37.5, got["performance.throughput"].Value, 1e-9)
	require.InDelta(t, 40, got["performance.throughput"].Target, 1e-9)
	require.InDelta(t, 1.2, got["performance.cpu_wall_ratio"].Value, 1e-9)
	require.InDelta(t, 4.0, got["io.read_amplification"].Value, 1e-9)
	require.InDelta(t, 3.0, got["io.write_amplification"].Value, 1e-9)
}

// TestCheckPerformanceBudgets_TBDSkipped pins the fail-open behavior for
// unset budgets: the registry fixture keeps wall/throughput/CPU/read-amp
// TBD until the Phase-1 baseline, so those KPIs never produce
// violations — while the already-pinned write-amplification budget
// (1.5, plan §16) IS evaluated by the tier-2 gate.
func TestCheckPerformanceBudgets_TBDSkipped(t *testing.T) {
	reg := NewBenchmarkFixtureRegistry()
	f, _ := reg.Fixture(FixtureCopyOnlyCanonical5MV1)
	require.False(t, f.Budget.Performance.P50WallMSMax.Set)
	require.False(t, f.Budget.Performance.P95WallMSMax.Set)
	require.False(t, f.Budget.Performance.MinThroughput.Set)
	require.False(t, f.Budget.Performance.MaxCPUWallRatio.Set)
	require.False(t, f.Budget.IO.ReadAmplificationMax.Set)
	require.True(t, f.Budget.IO.WriteAmplificationMax.Set, "write amplification is pinned on copy-only fixtures")

	// Within the pinned write-amp budget: no violations from TBD KPIs.
	ok := perfTestRun([]float64{999999}, 9.9, 1.2) // write amp p50 = 0.96 <= 1.5
	v, err := CheckPerformanceBudgets(f, ok)
	require.NoError(t, err)
	require.Empty(t, v, "TBD budgets must never produce violations")
	v, err = CheckPerformanceBudgets(f, nil)
	require.NoError(t, err)
	require.Empty(t, v, "nil run is a no-op")

	// Above the pinned write-amp budget: exactly one violation.
	bad := perfTestRun([]float64{999999}, 9.9, 2.5) // write amp p50 = 2.0 > 1.5
	v, err = CheckPerformanceBudgets(f, bad)
	require.NoError(t, err)
	require.Len(t, v, 1)
	require.Equal(t, "io.write_amplification", v[0].KPI)
}

// TestCheckPerformanceBudgets_FixtureMismatch pins the guardrail: a run
// belonging to a different fixture is an error, not a wrong-fixture
// verdict.
func TestCheckPerformanceBudgets_FixtureMismatch(t *testing.T) {
	reg := NewBenchmarkFixtureRegistry()
	f, _ := reg.Fixture(FixtureCopyOnlyCanonical5MV1)
	run := perfTestRun([]float64{4000}, 0.3, 1.2)
	run.FixtureID = FixtureCopy5MHigh

	_, err := CheckPerformanceBudgets(f, run)
	require.ErrorContains(t, err, "fixture")
}

// TestCheckPerformanceBudgets_ThroughputDirection pins the direction
// boundary of the lower-bound budget: equal-to-floor passes, below
// fails. 300 s / 7.5 s = 40.0 exactly.
func TestCheckPerformanceBudgets_ThroughputDirection(t *testing.T) {
	reg := NewBenchmarkFixtureRegistry()
	f, _ := reg.Fixture(FixtureCopyOnlyCanonical5MV1)
	f = pinPerfBudgets(f)
	f.Budget.Performance.P50WallMSMax = UnsetMax()
	f.Budget.Performance.P95WallMSMax = UnsetMax()
	f.Budget.Performance.MaxCPUWallRatio = UnsetMax()
	f.Budget.IO.ReadAmplificationMax = UnsetMax()
	f.Budget.IO.WriteAmplificationMax = UnsetMax()

	atFloor := perfTestRun([]float64{7500}, 0, 0)
	v, err := CheckPerformanceBudgets(f, atFloor)
	require.NoError(t, err)
	require.Empty(t, v, "throughput at the floor passes")

	below := perfTestRun([]float64{8000}, 0, 0)
	v, err = CheckPerformanceBudgets(f, below)
	require.NoError(t, err)
	require.Len(t, v, 1)
	require.Equal(t, "performance.throughput", v[0].KPI)
}

// TestBudgetMax_MinDirection pins the BudgetMax Min flag semantics:
// MinFloat is a lower bound enforced by the shared helper.
func TestBudgetMax_MinDirection(t *testing.T) {
	m := MinFloat(10)
	require.True(t, m.Set)
	require.True(t, m.Min)

	violated, enforced := enforcedViolated(m, 9.9)
	require.True(t, enforced)
	require.True(t, violated)
	violated, _ = enforcedViolated(m, 10.0)
	require.False(t, violated)
	violated, _ = enforcedViolated(m, 42.0)
	require.False(t, violated)

	upper := MaxFloat(10)
	violated, _ = enforcedViolated(upper, 10.0)
	require.False(t, violated, "upper bound: equal passes")
	violated, _ = enforcedViolated(upper, 10.1)
	require.True(t, violated)

	unset := UnsetMax()
	violated, enforced = enforcedViolated(unset, 0)
	require.False(t, enforced)
	require.False(t, violated)
}

// TestGateTiersExist pins the two-tier vocabulary: deterministic (CI,
// any runner) and performance (dedicated worker only).
func TestGateTiersExist(t *testing.T) {
	require.Equal(t, GateTier("deterministic"), TierDeterministic)
	require.Equal(t, GateTier("performance"), TierPerformance)
	require.NotEqual(t, TierDeterministic, TierPerformance)
}
