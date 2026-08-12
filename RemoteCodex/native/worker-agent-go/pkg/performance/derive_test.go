package performance

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"velox-worker-agent/pkg/video/pipeline"
)

// TestDerive_AccountedRatioSumsOnlyExclusivePhases pins the catalog
// accounted_ratio_rule: Derive sums EXACTLY the exclusive top-level phase
// durations it is given and nothing else. It never double-counts parent
// spans and never invents phases — the caller pre-filters span children at
// the phase collection boundary.
func TestDerive_AccountedRatioSumsOnlyExclusivePhases(t *testing.T) {
	d := Derive(RawMetrics{
		WallMs: 1000,
		ExclusivePhases: []PhaseTiming{
			{Name: "engine.render", DurationMS: 400},
			{Name: "engine.concat", DurationMS: 30},
		},
	})
	require.Equal(t, int64(570), d.UnaccountedMS)
	require.InDelta(t, 0.43, d.AccountedRatio, 1e-9)
}

// TestDerive_Amplification pins read/write amplification over the final
// output bytes (the /proc/<pid>/io process-tree totals are the numerators).
func TestDerive_Amplification(t *testing.T) {
	d := Derive(RawMetrics{
		TotalBytesRead:    600_000_000,
		TotalBytesWritten: 400_000_000,
		OutputBytes:       100_000_000,
	})
	require.InDelta(t, 6.0, d.ReadAmplification, 1e-9)
	require.InDelta(t, 4.0, d.WriteAmplification, 1e-9)
}

// TestDerive_ProcessesPerClip pins external processes ÷ render-plan clips.
func TestDerive_ProcessesPerClip(t *testing.T) {
	d := Derive(RawMetrics{ExternalProcessCount: 64, ClipCount: 25})
	require.InDelta(t, 2.56, d.ProcessesPerClip, 1e-9)
}

// TestDerive_CPUSchedulingPins pins the CPU and useful-work wall ratios.
func TestDerive_CPUSchedulingPins(t *testing.T) {
	d := Derive(RawMetrics{WallMs: 10_000, CPUWallMS: 5000, UsefulPipelineMS: 3500})
	require.InDelta(t, 0.5, d.CPUWallRatio, 1e-9)
	require.InDelta(t, 0.35, d.UsefulWorkRatio, 1e-9)
}

// TestDerive_ZeroGuards pins the fail-safe behavior: missing raw facts
// yield zero ratios (never +Inf/NaN) and a zero derived section means
// "not measured", never a measured zero. An empty input derives all zeros.
func TestDerive_ZeroGuards(t *testing.T) {
	d := Derive(RawMetrics{})
	require.Zero(t, d.UnaccountedMS)
	require.Zero(t, d.AccountedRatio)
	require.Zero(t, d.ReadAmplification)
	require.Zero(t, d.WriteAmplification)
	require.Zero(t, d.ProcessesPerClip)
	require.Zero(t, d.UsefulWorkRatio)
	require.Zero(t, d.CPUWallRatio)
	require.False(t, math.IsInf(d.AccountedRatio, 0) || math.IsNaN(d.AccountedRatio))

	// Wall clock with zero phases: everything unaccounted, ratio 0.
	d = Derive(RawMetrics{WallMs: 19310, TotalBytesRead: 1, OutputBytes: 0})
	require.Equal(t, int64(19310), d.UnaccountedMS)
	require.Zero(t, d.AccountedRatio)
	require.Zero(t, d.ReadAmplification, "zero output bytes must not divide")
	require.Zero(t, d.ProcessesPerClip, "zero clip count must not divide")
}

// TestDerive_MatchesReceiptAssembly pins the wiring: Assemble must produce
// exactly the same DerivedMetrics as a direct Derive over the same raw
// facts — the assembler never recomputes a ratio itself.
func TestDerive_MatchesReceiptAssembly(t *testing.T) {
	run := sampleRun()
	run.RenderMetrics.DetailedPhases = []pipeline.DetailedPhaseTiming{
		{EventName: "engine.render", DurationMS: 400},
		{EventName: "engine.concat", DurationMS: 30},
	}
	ctx := AssemblyContext{
		WallMs:           1000,
		CPUWallMS:        500,
		UsefulPipelineMS: 350,
		Workload:         WorkloadProfile{ClipCount: 25},
	}

	receipt := NewAssembler().Assemble(run, ctx)

	// The pin runs the SAME RawMetrics construction the production path
	// uses (rawMetricsFrom), so the wiring can never drift: if Assemble
	// stops handing Derive a raw fact, this test fails.
	require.Equal(t, Derive(rawMetricsFrom(ctx, receipt)), receipt.Derived)

	// Sanity on the numbers: sampleRun carries the full raw set, so every
	// ratio that has a denominator here is derived.
	require.InDelta(t, 0.43, receipt.Derived.AccountedRatio, 1e-9)
	require.InDelta(t, 0.5, receipt.Derived.CPUWallRatio, 1e-9)
	require.InDelta(t, 0.35, receipt.Derived.UsefulWorkRatio, 1e-9)
	require.InDelta(t, 2.56, receipt.Derived.ProcessesPerClip, 1e-9)
	require.InDelta(t, 6.0, receipt.Derived.ReadAmplification, 1e-9)
	require.InDelta(t, 4.0, receipt.Derived.WriteAmplification, 1e-9)
}
