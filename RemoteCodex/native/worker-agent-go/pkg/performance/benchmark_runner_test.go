package performance

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeRenderer is a deterministic, concurrency-safe RenderRunner for
// tests. Walls/SHAs cycle per call; failAt marks call indexes that fail.
type fakeRenderer struct {
	mu      sync.Mutex
	calls   int
	walls   []int64
	shas    []string
	failAt  map[int]bool
	decode  int64
	process int64
	// fsync/rename/dirFsync cycle per call like walls; used to pin the
	// output-durability p95 summary aggregation.
	fsync    []int64
	rename   []int64
	dirFsync []int64
}

func (f *fakeRenderer) Render(_ context.Context, fixture BenchmarkFixture) (BenchmarkRenderResult, error) {
	f.mu.Lock()
	idx := f.calls
	f.calls++
	wall := f.walls[idx%len(f.walls)]
	sha := ""
	if len(f.shas) > 0 {
		sha = f.shas[idx%len(f.shas)]
	}
	fail := f.failAt[idx]
	f.mu.Unlock()

	if fail {
		return BenchmarkRenderResult{}, errors.New("render failed")
	}
	receipt := NewPerformanceReceiptV1()
	receipt.Identity.BenchmarkFixtureID = string(fixture.ID)
	receipt.Timing.WallMs = wall
	receipt.Media.FramesDecoded = f.decode
	receipt.Process.ExternalProcessCount = f.process
	receipt.Derived.AccountedRatio = 0.96
	receipt.Derived.WriteAmplification = 1.2
	receipt.Derived.ProcessesPerClip = 0
	if len(f.fsync) > 0 {
		receipt.IO.FileFsyncMS = f.fsync[idx%len(f.fsync)]
	}
	if len(f.rename) > 0 {
		receipt.IO.OutputRenameMS = f.rename[idx%len(f.rename)]
	}
	if len(f.dirFsync) > 0 {
		receipt.IO.DirectoryFsyncMS = f.dirFsync[idx%len(f.dirFsync)]
	}
	return BenchmarkRenderResult{
		Receipt:        receipt,
		ArtifactSHA256: sha,
		Evidence:       GateEvidence{ArtifactSHA256: sha},
	}, nil
}

func newRunnerForTest(f *fakeRenderer) *BenchmarkRunner {
	reg := NewBenchmarkFixtureRegistry()
	fixture, _ := reg.Fixture(FixtureCopy5MHigh)
	return &BenchmarkRunner{
		Fixture:     fixture,
		Runs:        5,
		Concurrency: 2,
		CacheMode:   CacheModeWarm,
		WorkerID:    "bench-worker-1",
		GitCommit:   "deadbeef",
		Renderer:    f,
	}
}

// TestBenchmarkRunner_ProducesMachineReadableRun pins the full report:
// identity fields, host fingerprint, cache mode, concurrency, per-run
// receipts and the deterministic artifact SHA.
func TestBenchmarkRunner_ProducesMachineReadableRun(t *testing.T) {
	f := &fakeRenderer{walls: []int64{1000, 2000, 3000, 4000, 5000}, shas: []string{"abc123"}}
	runner := newRunnerForTest(f)

	run, err := runner.Run(context.Background())
	require.NoError(t, err)

	require.True(t, strings.HasPrefix(run.BenchmarkRunID, "bench-"))
	require.Equal(t, FixtureCopy5MHigh, run.FixtureID)
	require.Equal(t, "deadbeef", run.GitCommit)
	require.Equal(t, "bench-worker-1", run.WorkerID)
	require.NotEmpty(t, run.HostFingerprint)
	require.NotEmpty(t, run.Kernel)
	require.Equal(t, CacheModeWarm, run.CacheMode)
	require.Equal(t, 2, run.Concurrency)
	require.Equal(t, 5, run.Runs)
	require.False(t, run.CreatedAt.IsZero())

	require.Len(t, run.Receipts, 5)
	// With concurrency the goroutine-to-slot order is not deterministic,
	// so assert the RUN INDEX order and the wall MULTISET (every fake
	// wall must appear exactly once across the observations).
	gotWalls := map[int64]int{}
	for i, obs := range run.Receipts {
		require.Equal(t, i, obs.RunIndex, "observations must keep run order")
		require.NotNil(t, obs.Receipt)
		require.Empty(t, obs.Error)
		gotWalls[obs.WallMS]++
	}
	require.Len(t, gotWalls, len(f.walls), "every distinct wall must be observed")
	for _, w := range f.walls {
		require.Equal(t, 1, gotWalls[w], "wall %d must appear exactly once", w)
	}
	require.Equal(t, "abc123", run.ArtifactSHA256, "all runs share one artifact SHA → deterministic")

	// The report is machine-readable: round-trips through JSON.
	data, err := json.Marshal(run)
	require.NoError(t, err)
	var back BenchmarkRun
	require.NoError(t, json.Unmarshal(data, &back))
	require.Equal(t, run.BenchmarkRunID, back.BenchmarkRunID)
	require.Equal(t, run.WorkerID, back.WorkerID)
	require.Equal(t, run.HostFingerprint, back.HostFingerprint)
	require.Equal(t, run.ArtifactSHA256, back.ArtifactSHA256)
	require.Len(t, back.Receipts, 5)
}

// TestBenchmarkRunner_Percentiles pins the nearest-rank summary over the
// five walls [1000,2000,3000,4000,5000]: p50=3000, p95=5000.
func TestBenchmarkRunner_Percentiles(t *testing.T) {
	f := &fakeRenderer{walls: []int64{1000, 2000, 3000, 4000, 5000}}
	run, err := newRunnerForTest(f).Run(context.Background())
	require.NoError(t, err)

	require.InDelta(t, 3000, run.Summary.WallMSP50, 1e-9)
	require.InDelta(t, 5000, run.Summary.WallMSP95, 1e-9)
	require.InDelta(t, 0.96, run.Summary.AccountedRatioP50, 1e-9)
	require.InDelta(t, 1.2, run.Summary.WriteAmplificationP50, 1e-9)
	require.Zero(t, run.Summary.GateFailures)
	require.Zero(t, run.Summary.FailedRuns)
}

// TestBenchmarkRunner_OutputDurabilityP95 pins the p95 aggregation of
// the publishAtomic timings (file fsync / rename / directory fsync)
// over the five runs [10,20,30,40,50]: p95=50 (nearest-rank), and a
// run without the engine-side io_counters block yields 0 (never NaN).
func TestBenchmarkRunner_OutputDurabilityP95(t *testing.T) {
	f := &fakeRenderer{
		walls:    []int64{1000},
		fsync:    []int64{10, 20, 30, 40, 50},
		rename:   []int64{1, 2, 3, 4, 5},
		dirFsync: []int64{5, 6, 7, 8, 9},
	}
	run, err := newRunnerForTest(f).Run(context.Background())
	require.NoError(t, err)

	require.InDelta(t, 50, run.Summary.FileFsyncMSP95, 1e-9)
	require.InDelta(t, 5, run.Summary.OutputRenameMSP95, 1e-9)
	require.InDelta(t, 9, run.Summary.DirectoryFsyncMSP95, 1e-9)

	// Legacy engines (no io_counters block) → all-zero sample → 0.
	legacy := &fakeRenderer{walls: []int64{1000}}
	run, err = newRunnerForTest(legacy).Run(context.Background())
	require.NoError(t, err)
	require.Zero(t, run.Summary.FileFsyncMSP95)
	require.Zero(t, run.Summary.OutputRenameMSP95)
	require.Zero(t, run.Summary.DirectoryFsyncMSP95)
}

// TestBenchmarkRunner_GateViolationsReported pins that each observation
// carries its deterministic-gate violations (here: decode forbidden).
func TestBenchmarkRunner_GateViolationsReported(t *testing.T) {
	f := &fakeRenderer{walls: []int64{2000}, decode: 42}
	run, err := newRunnerForTest(f).Run(context.Background())
	require.NoError(t, err)

	// Every render violates the decode invariant → 5 gate failures total,
	// and each observation carries its own violation list.
	require.Equal(t, 5, run.Summary.GateFailures)
	for _, obs := range run.Receipts {
		require.Len(t, obs.GateViolations, 1)
		require.Equal(t, "decode", obs.GateViolations[0].Invariant)
	}
}

// TestBenchmarkRunner_PartialFailures pins that a failed render is
// recorded per observation (never aborting the run), and an all-failed
// run is an error.
func TestBenchmarkRunner_PartialFailures(t *testing.T) {
	f := &fakeRenderer{walls: []int64{2000}, failAt: map[int]bool{1: true, 3: true}}
	run, err := newRunnerForTest(f).Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, run.Summary.FailedRuns)
	// With concurrency the failing call slots land in arbitrary
	// observations, so count rather than index.
	var failed, withReceipt int
	for _, obs := range run.Receipts {
		if obs.Error != "" {
			failed++
			require.Nil(t, obs.Receipt, "a failed observation carries no receipt")
		} else {
			withReceipt++
		}
	}
	require.Equal(t, 2, failed)
	require.Equal(t, 3, withReceipt)

	all := &fakeRenderer{walls: []int64{2000}, failAt: map[int]bool{0: true, 1: true, 2: true, 3: true, 4: true}}
	_, err = newRunnerForTest(all).Run(context.Background())
	require.Error(t, err, "an all-failed run must return an error")
}

// TestBenchmarkRunner_ArtifactSHADeterminism pins that divergent
// artifact SHAs across runs break the deterministic SHA ("" is the
// "determinism broken" sentinel).
func TestBenchmarkRunner_ArtifactSHADeterminism(t *testing.T) {
	// Divergent SHAs → not deterministic.
	f := &fakeRenderer{walls: []int64{2000}, shas: []string{"shaA", "shaB"}}
	run, err := newRunnerForTest(f).Run(context.Background())
	require.NoError(t, err)
	require.Empty(t, run.ArtifactSHA256)

	// No SHA produced → not measured, empty.
	none := &fakeRenderer{walls: []int64{2000}}
	run, err = newRunnerForTest(none).Run(context.Background())
	require.NoError(t, err)
	require.Empty(t, run.ArtifactSHA256)
}

// TestBenchmarkRunner_HostFingerprintStable pins that the fingerprint
// is stable across runs on the same host.
func TestBenchmarkRunner_HostFingerprintStable(t *testing.T) {
	f1 := &fakeRenderer{walls: []int64{2000}}
	r1, err := newRunnerForTest(f1).Run(context.Background())
	require.NoError(t, err)
	f2 := &fakeRenderer{walls: []int64{2000}}
	r2, err := newRunnerForTest(f2).Run(context.Background())
	require.NoError(t, err)

	require.Equal(t, r1.HostFingerprint, r2.HostFingerprint)
	require.NotEqual(t, r1.BenchmarkRunID, r2.BenchmarkRunID, "each run gets a fresh id")
}

// TestBenchmarkRunner_NoRendererIsError pins the configuration guard.
func TestBenchmarkRunner_NoRendererIsError(t *testing.T) {
	reg := NewBenchmarkFixtureRegistry()
	fixture, _ := reg.Fixture(FixtureCopy5MLow)
	_, err := (&BenchmarkRunner{Fixture: fixture, Runs: 1, Renderer: nil}).Run(context.Background())
	require.Error(t, err)
}

// TestPercentile pins the nearest-rank edge cases.
func TestPercentile(t *testing.T) {
	require.Zero(t, percentile(nil, 0.5))
	require.InDelta(t, 2, percentile([]float64{1, 2, 3}, 0.5), 1e-9, "nearest-rank p50 of 3 elements is the 2nd")
	require.InDelta(t, 3, percentile([]float64{1, 2, 3}, 0.95), 1e-9)
	require.InDelta(t, 1, percentile([]float64{1, 2, 3}, 0.05), 1e-9)
	require.InDelta(t, 2, percentile([]float64{3, 1, 2}, 0.5), 1e-9, "percentile sorts internally")
}
