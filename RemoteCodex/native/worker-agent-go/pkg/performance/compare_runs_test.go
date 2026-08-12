package performance

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// testRun builds a BenchmarkRun with the given per-observation wall
// times and architecture counters; the summary is computed through the
// runner's own summarizeObservations so the comparison consumes exactly
// what velox-benchmark would produce.
func testRun(id BenchmarkFixtureID, commit string, walls []float64, execve, encode int, readAmp float64) *BenchmarkRun {
	obs := make([]BenchmarkRunObservation, 0, len(walls))
	for i, w := range walls {
		r := NewPerformanceReceiptV1()
		r.Timing.WallMs = int64(w)
		r.Process.ExternalProcessCount = int64(execve)
		r.Media.EncodePasses = int64(encode)
		r.Derived.ReadAmplification = readAmp
		r.Derived.WriteAmplification = readAmp * 0.8
		obs = append(obs, BenchmarkRunObservation{RunIndex: i, WallMS: int64(w), Receipt: r})
	}
	return &BenchmarkRun{
		BenchmarkRunID: "bench-test",
		FixtureID:      id,
		GitCommit:      commit,
		CacheMode:      CacheModeWarm,
		WorkerID:       "worker-test",
		Receipts:       obs,
		Summary:        summarizeObservations(obs),
	}
}

// TestCompareBenchmarkRuns_DeltaMath pins the §22 arithmetic on the
// canonical copy-only track: wall p50 10000→2500 ms is -75%, execve is
// aggregated as the max across runs, read/write amplification use p50,
// and the improved/regressed flags follow lower-is-better.
func TestCompareBenchmarkRuns_DeltaMath(t *testing.T) {
	base := testRun(FixtureCopyOnlyCanonical5MV1, "commitA",
		[]float64{11000, 10000, 9000}, 64, 1, 5.1)
	cand := testRun(FixtureCopyOnlyCanonical5MV1, "commitB",
		[]float64{4500, 4000, 2000}, 0, 0, 1.4)

	// Nearest-rank percentiles (the runner's own aggregation):
	// baseline p50=10000 p95=11000; candidate p50=4000 p95=4500.

	c, err := CompareBenchmarkRuns(base, cand)
	require.NoError(t, err)
	require.Equal(t, FixtureCopyOnlyCanonical5MV1, c.FixtureID)
	require.Equal(t, "commitA", c.Baseline.GitCommit)
	require.Equal(t, "commitB", c.Candidate.GitCommit)

	// wall p50 (nearest-rank): baseline sorted [9000,10000,11000] ->
	// 10000; candidate sorted [2000,4000,4500] -> 4000 = -60%
	require.Equal(t, float64(10000), c.KPIs.WallP50.Baseline)
	require.Equal(t, float64(4000), c.KPIs.WallP50.Candidate)
	require.InDelta(t, -60, *c.KPIs.WallP50.DeltaPercent, 1e-9)
	require.True(t, c.KPIs.WallP50.Improved)

	// wall p95 (nearest-rank): 11000 -> 4500 = -59.09%
	require.InDelta(t, -59.09, *c.KPIs.WallP95.DeltaPercent, 1e-2)

	// execve and audio encode are max-aggregated invariants
	require.Equal(t, float64(64), c.KPIs.ExecveCount.Baseline)
	require.Equal(t, float64(0), c.KPIs.ExecveCount.Candidate)
	require.InDelta(t, -100, *c.KPIs.ExecveCount.DeltaPercent, 1e-9)
	require.Equal(t, float64(1), c.KPIs.AudioEncodePasses.Baseline)
	require.Equal(t, float64(0), c.KPIs.AudioEncodePasses.Candidate)

	// read amplification p50 (sorted 1.4,1.4,1.4 -> 1.4)
	require.InDelta(t, 5.1, c.KPIs.ReadAmplification.Baseline, 1e-9)
	require.InDelta(t, 1.4, c.KPIs.ReadAmplification.Candidate, 1e-9)
	require.InDelta(t, -72.55, *c.KPIs.ReadAmplification.DeltaPercent, 1e-2)

	// write amplification from the summary p50
	require.InDelta(t, 4.08, c.KPIs.WriteAmplification.Baseline, 1e-9)
	require.InDelta(t, 1.12, c.KPIs.WriteAmplification.Candidate, 1e-9)

	require.True(t, c.AnyRegression == false)
	require.Zero(t, c.CandidateGateFailures)
}

// TestCompareBenchmarkRuns_Regression pins that a slower candidate is
// flagged as regression while still producing a valid report.
func TestCompareBenchmarkRuns_Regression(t *testing.T) {
	base := testRun(FixtureCopyOnlyCanonical5MV1, "commitA", []float64{10000}, 0, 0, 1.4)
	cand := testRun(FixtureCopyOnlyCanonical5MV1, "commitB", []float64{15000}, 0, 0, 2.0)

	c, err := CompareBenchmarkRuns(base, cand)
	require.NoError(t, err)
	require.InDelta(t, +50, *c.KPIs.WallP50.DeltaPercent, 1e-9)
	require.False(t, c.KPIs.WallP50.Improved)
	require.True(t, c.KPIs.WallP50.Regressed())
	require.True(t, c.KPIs.ReadAmplification.Regressed())
	require.True(t, c.AnyRegression)
	require.Contains(t, c.FormatTable(), "REGRESSION")
}

// TestCompareBenchmarkRuns_ZeroBaseline pins the from-zero semantics:
// an invariant that was already zero in the baseline has no delta
// percent (undefined, not +inf), and going 0 -> N is a regression.
func TestCompareBenchmarkRuns_ZeroBaseline(t *testing.T) {
	base := testRun(FixtureCopyOnlyCanonical5MV1, "commitA", []float64{4000}, 0, 0, 1.2)
	steady := testRun(FixtureCopyOnlyCanonical5MV1, "commitB", []float64{4000}, 0, 0, 1.2)
	broken := testRun(FixtureCopyOnlyCanonical5MV1, "commitB", []float64{4000}, 2, 0, 1.2)

	s, err := CompareBenchmarkRuns(base, steady)
	require.NoError(t, err)
	require.Nil(t, s.KPIs.ExecveCount.DeltaPercent)
	require.Equal(t, "n/a (baseline 0)", s.KPIs.ExecveCount.DeltaLabel)
	require.False(t, s.KPIs.ExecveCount.Improved)
	require.False(t, s.KPIs.ExecveCount.Regressed())

	r, err := CompareBenchmarkRuns(base, broken)
	require.NoError(t, err)
	require.Equal(t, float64(2), r.KPIs.ExecveCount.Candidate)
	require.True(t, r.KPIs.ExecveCount.Regressed())
	require.True(t, r.AnyRegression)
}

// TestCompareBenchmarkRuns_Incomparable pins the guardrails: different
// fixtures and different cache modes are errors, not regressions.
func TestCompareBenchmarkRuns_Incomparable(t *testing.T) {
	base := testRun(FixtureCopyOnlyCanonical5MV1, "commitA", []float64{4000}, 0, 0, 1.2)
	otherFixture := testRun(FixtureCopy5MHigh, "commitB", []float64{4000}, 0, 0, 1.2)
	_, err := CompareBenchmarkRuns(base, otherFixture)
	require.ErrorContains(t, err, "fixture mismatch")

	cold := testRun(FixtureCopyOnlyCanonical5MV1, "commitB", []float64{4000}, 0, 0, 1.2)
	cold.CacheMode = CacheModeCold
	_, err = CompareBenchmarkRuns(base, cold)
	require.ErrorContains(t, err, "cache mode mismatch")

	_, err = CompareBenchmarkRuns(nil, base)
	require.ErrorContains(t, err, "required")
}

// TestCompareBenchmarkRuns_SkipsFailedObservations pins that failed
// runs and nil receipts are excluded from the aggregates, mirroring the
// runner's summary.
func TestCompareBenchmarkRuns_SkipsFailedObservations(t *testing.T) {
	base := testRun(FixtureCopyOnlyCanonical5MV1, "commitA", []float64{4000}, 0, 0, 1.2)
	cand := testRun(FixtureCopyOnlyCanonical5MV1, "commitB", []float64{2000}, 0, 0, 1.2)
	cand.Receipts = append(cand.Receipts,
		BenchmarkRunObservation{RunIndex: 99, Error: "render failed"},
		BenchmarkRunObservation{RunIndex: 100},
	)
	cand.Summary = summarizeObservations(cand.Receipts)

	c, err := CompareBenchmarkRuns(base, cand)
	require.NoError(t, err)
	require.Equal(t, float64(2000), c.KPIs.WallP50.Candidate)
	require.Equal(t, float64(0), c.KPIs.ExecveCount.Candidate)
}

// TestCompareBenchmarkRuns_GateFailuresAndArtifactSHA pins that a
// candidate violating deterministic invariants is surfaced, and that a
// divergent pinned artifact SHA is reported as nondeterminism.
func TestCompareBenchmarkRuns_GateFailuresAndArtifactSHA(t *testing.T) {
	base := testRun(FixtureCopyOnlyCanonical5MV1, "commitA", []float64{4000}, 0, 0, 1.2)
	cand := testRun(FixtureCopyOnlyCanonical5MV1, "commitB", []float64{2000}, 0, 0, 1.2)
	cand.ArtifactSHA256 = "aaa"
	base.ArtifactSHA256 = "bbb"

	c, err := CompareBenchmarkRuns(base, cand)
	require.NoError(t, err)
	require.True(t, c.ArtifactSHAChanged)
	require.Contains(t, c.FormatTable(), "nondeterministic artifact")

	same := testRun(FixtureCopyOnlyCanonical5MV1, "commitB", []float64{2000}, 0, 0, 1.2)
	same.ArtifactSHA256 = "aaa"
	base.ArtifactSHA256 = "aaa"
	c2, err := CompareBenchmarkRuns(base, same)
	require.NoError(t, err)
	require.False(t, c2.ArtifactSHAChanged)
}

// TestBenchmarkComparison_JSONRoundTrip pins the machine-readable wire
// format of the comparison.
func TestBenchmarkComparison_JSONRoundTrip(t *testing.T) {
	base := testRun(FixtureCopyOnlyCanonical5MV1, "commitA", []float64{10000}, 64, 1, 5.1)
	cand := testRun(FixtureCopyOnlyCanonical5MV1, "commitB", []float64{2500}, 0, 0, 1.4)
	c, err := CompareBenchmarkRuns(base, cand)
	require.NoError(t, err)

	data, err := json.Marshal(c)
	require.NoError(t, err)
	var back BenchmarkComparison
	require.NoError(t, json.Unmarshal(data, &back))
	require.Equal(t, *c, back)
	require.InDelta(t, -75, *back.KPIs.WallP50.DeltaPercent, 1e-9)
	require.Equal(t, "commitB", back.Candidate.GitCommit)
}
