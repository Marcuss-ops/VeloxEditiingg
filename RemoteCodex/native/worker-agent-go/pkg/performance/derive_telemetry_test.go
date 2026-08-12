package performance

import (
	"testing"

	"github.com/stretchr/testify/require"

	sharedtelemetry "velox-shared/telemetry"
	"velox-worker-agent/pkg/video/pipeline"
)

// TestDerivedFromRenderMetrics_EndToEnd pins the executor's single
// derived-KPI entry point: exclusive media phases feed accounted_ratio
// and useful_work_ratio, span children are never summed, unclassified
// rows are quarantined, and the amplification/process ratios come from
// the raw byte and process counts.
func TestDerivedFromRenderMetrics_EndToEnd(t *testing.T) {
	d := DerivedFromRenderMetrics(pipeline.RenderMetrics{
		CPUUserMs:            500,
		CPUSystemMs:          100,
		TotalBytesRead:       600_000_000,
		TotalBytesWritten:    400_000_000,
		ExternalProcessCount: 64,
		DetailedPhases: []pipeline.DetailedPhaseTiming{
			{Component: "engine", Action: "render", DurationMS: 400}, // exclusive → accounted + useful
			{Component: "engine", Action: "composite", DurationMS: 200},
			{Component: "unknown.component", Action: "unknown.action", DurationMS: 30, Scope: "attempt"},
		},
	}, 1000, 16, 100_000_000)

	require.Equal(t, int64(600), d.UnaccountedMS)
	require.InDelta(t, 0.4, d.AccountedRatio, 1e-9)
	require.InDelta(t, 6.0, d.ReadAmplification, 1e-9)
	require.InDelta(t, 4.0, d.WriteAmplification, 1e-9)
	require.InDelta(t, 4.0, d.ProcessesPerClip, 1e-9)
	require.InDelta(t, 0.4, d.UsefulWorkRatio, 1e-9)
	require.InDelta(t, 0.6, d.CPUWallRatio, 1e-9)
}

// TestUsefulPipelineMSFromRenderMetrics pins the useful-work producer:
// only EXCLUSIVE media-engine phases count; exclusive orchestration
// (control.grpc.reconnect is cataloged exclusive) and span children are
// never useful media work. A legacy sidecar without the phases[] stream
// stays "not measured" (0).
func TestUsefulPipelineMSFromRenderMetrics(t *testing.T) {
	useful := UsefulPipelineMSFromRenderMetrics(pipeline.RenderMetrics{
		DetailedPhases: []pipeline.DetailedPhaseTiming{
			{Component: "engine", Action: "render", DurationMS: 400},         // exclusive media → counted
			{Component: "control.grpc", Action: "reconnect", DurationMS: 25}, // exclusive orchestration → excluded
			{Component: "engine", Action: "composite", DurationMS: 200},      // span_child → excluded
		},
	})
	require.Equal(t, int64(400), useful)

	// Legacy sidecar: no detailed stream → the observation stays 0 (the
	// documented "not measured" sentinel that yields a zero ratio).
	require.Zero(t, UsefulPipelineMSFromRenderMetrics(pipeline.RenderMetrics{
		PhaseMS: map[string]float64{"render": 400},
	}))
}

// TestDerivedFromRenderMetrics_ZeroOutputBytesGuards pins that a
// pre-manifest projection (outputBytes == 0) never divides by zero: the
// amplifications stay 0 while the accounting/CPU ratios remain valid.
func TestDerivedFromRenderMetrics_ZeroOutputBytesGuards(t *testing.T) {
	d := DerivedFromRenderMetrics(pipeline.RenderMetrics{
		CPUUserMs:      300,
		CPUSystemMs:    100,
		TotalBytesRead: 9_000,
		DetailedPhases: []pipeline.DetailedPhaseTiming{
			{Component: "engine", Action: "render", DurationMS: 700},
		},
	}, 1000, 4, 0)

	require.Zero(t, d.ReadAmplification)
	require.Zero(t, d.WriteAmplification)
	require.InDelta(t, 0.7, d.AccountedRatio, 1e-9)
	require.InDelta(t, 0.4, d.CPUWallRatio, 1e-9)
}

// TestCheckDerivedBudgets pins the Phase-1 accounted_ratio target: a
// measured ratio below 95% is a violation, at/above the target is clean,
// and "not measured" (0) is never a violation.
func TestCheckDerivedBudgets(t *testing.T) {
	violations := CheckDerivedBudgets(DerivedMetrics{AccountedRatio: 0.90})
	require.Len(t, violations, 1)
	require.Equal(t, "accounted_ratio", violations[0].KPI)
	require.InDelta(t, AccountedRatioTarget, violations[0].Target, 1e-9)

	require.Empty(t, CheckDerivedBudgets(DerivedMetrics{AccountedRatio: 0.96}))
	require.Empty(t, CheckDerivedBudgets(DerivedMetrics{AccountedRatio: 0.0}))
	require.Empty(t, CheckDerivedBudgets(DerivedMetrics{}))
}

// TestDerivedFromRenderMetrics_UsesAssemblerClassification pins that the
// executor projection classifies through the SAME path as the receipt:
// assemblePhases stamps the catalog timing mode, so the two consumers
// can never disagree about what is exclusive.
func TestDerivedFromRenderMetrics_UsesAssemblerClassification(t *testing.T) {
	rm := pipeline.RenderMetrics{
		DetailedPhases: []pipeline.DetailedPhaseTiming{
			{Component: "engine", Action: "render", DurationMS: 400},
			{Component: "engine", Action: "composite", DurationMS: 200},
		},
	}
	// The phases fed to Derive carry the stamped timing modes.
	phases := assemblePhases(rm)
	require.Len(t, phases, 2)
	require.Equal(t, sharedtelemetry.TimingExclusive, phases[0].TimingMode)
	require.Equal(t, sharedtelemetry.TimingSpanChild, phases[1].TimingMode)

	d := DerivedFromRenderMetrics(rm, 1000, 0, 0)
	require.InDelta(t, 0.4, d.AccountedRatio, 1e-9)
}
