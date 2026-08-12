package performance

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	sharedtelemetry "velox-shared/telemetry"
	"velox-worker-agent/pkg/video/pipeline"
)

// TestDerive_AccountedRatioSumsOnlyExclusiveTopLevelPhases pins the
// catalog accounted_ratio_rule in the Deriver itself: ONLY rows stamped
// TimingExclusive are summed. Span children (parallel work that would
// double-count against the wall clock), span parents and unclassified rows
// are carried for diagnostics but NEVER enter the accounted sum — the rule
// is enforced here, not trusted to the caller.
func TestDerive_AccountedRatioSumsOnlyExclusiveTopLevelPhases(t *testing.T) {
	d := Derive(RawMetrics{
		WallMs: 1000,
		Phases: []PhaseTiming{
			{Name: "engine.render", DurationMS: 400, TimingMode: sharedtelemetry.TimingExclusive},
			// Parallel segment spans: summed naively they would triple the
			// accounted budget against a 1000ms wall clock.
			{Name: "engine.video.decode", DurationMS: 300, TimingMode: sharedtelemetry.TimingSpanChild},
			{Name: "engine.composite", DurationMS: 200, TimingMode: sharedtelemetry.TimingSpanChild},
			// A span parent overlaps its children — also never summed.
			{Name: "engine.render.tree", DurationMS: 100, TimingMode: sharedtelemetry.TimingSpanParent},
			// Unclassified (no catalog key): quarantined, never exclusive.
			{Name: "engine.concat", DurationMS: 30},
		},
	})
	require.Equal(t, int64(600), d.UnaccountedMS)
	require.InDelta(t, 0.4, d.AccountedRatio, 1e-9)
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

// TestDerive_MatchesReceiptAssembly pins the wiring end to end: Assemble
// classifies the sidecar phase events through the shared catalog and Derive
// sums only the exclusive rows. A span_child row present in the stream must
// never inflate accounted_ratio, and the assembler never recomputes a ratio
// itself (rawMetricsFrom is the shared construction).
func TestDerive_MatchesReceiptAssembly(t *testing.T) {
	run := sampleRun()
	run.RenderMetrics.DetailedPhases = []pipeline.DetailedPhaseTiming{
		{Component: "engine", Action: "render", DurationMS: 400},                  // exclusive → summed
		{Component: "engine", Action: "composite", DurationMS: 200},               // span_child → never summed
		{Component: "engine", Action: "concat", DurationMS: 30, Scope: "attempt"}, // unclassified → quarantined
	}
	ctx := AssemblyContext{
		WallMs:           1000,
		CPUWallMS:        500,
		UsefulPipelineMS: 350,
		Workload:         WorkloadProfile{ClipCount: 25},
	}

	receipt := NewAssembler().Assemble(run, ctx)

	require.Len(t, receipt.Phases, 3)
	require.Equal(t, sharedtelemetry.TimingExclusive, receipt.Phases[0].TimingMode)
	require.Equal(t, sharedtelemetry.TimingSpanChild, receipt.Phases[1].TimingMode)
	require.Empty(t, receipt.Phases[2].TimingMode, "unknown sidecar events stay unclassified (quarantined)")

	// The pin runs the SAME RawMetrics construction the production path
	// uses (rawMetricsFrom), so the wiring can never drift.
	require.Equal(t, Derive(rawMetricsFrom(ctx, receipt)), receipt.Derived)

	// Sanity on the numbers: only engine.render (400ms) is accounted out
	// of a 1000ms wall clock; the composite span and the unknown concat
	// row never enter the sum.
	require.InDelta(t, 0.4, receipt.Derived.AccountedRatio, 1e-9)
	require.Equal(t, int64(600), receipt.Derived.UnaccountedMS)
	require.InDelta(t, 0.5, receipt.Derived.CPUWallRatio, 1e-9)
	require.InDelta(t, 0.35, receipt.Derived.UsefulWorkRatio, 1e-9)
	require.InDelta(t, 2.56, receipt.Derived.ProcessesPerClip, 1e-9)
	require.InDelta(t, 6.0, receipt.Derived.ReadAmplification, 1e-9)
	require.InDelta(t, 4.0, receipt.Derived.WriteAmplification, 1e-9)
}
