package executors

import (
	"testing"

	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/video/pipeline"
)

// TestProjectDetailedPhasesProjectsEnginePhaseMS pins the projection that makes
// the engine's internal phase_ms ledger operator-visible.
//
// The copy-only packet mux is the dominant render cost and the engine records it
// only as the packet_mux_ms timer: it is not part of the sidecar phases[]
// stream, and before this projection it was dropped at the report boundary, so
// the Master showed an opaque aggregate render span and nothing about the mux.
//
// The projected row must stay catalog-legal (packet_mux is a declared engine
// event) and must not disturb the sidecar event index sequence, because the
// importer treats a duplicate origin/index as a conflicting duplicate.
func TestProjectDetailedPhasesProjectsEnginePhaseMS(t *testing.T) {
	rm := pipeline.RenderMetrics{
		PhaseMS: map[string]float64{
			"packet_mux_ms":         12_700.4,
			"unregistered_timer_ms": 5,
		},
		DetailedPhases: []pipeline.DetailedPhaseTiming{
			{
				Origin: telemetry.OriginEngine, Component: "engine", Action: "render",
				EventIndex: 3, DurationMS: 12_784, Status: telemetry.StatusOK,
			},
		},
	}

	got := projectDetailedPhases(rm)

	var mux *int
	for i := range got {
		if got[i].Action == "packet_mux" {
			mux = &i
		}
		if got[i].Action == "unregistered_timer" {
			t.Fatalf("timer without a catalog entry must not be projected: %#v", got[i])
		}
	}
	if mux == nil {
		t.Fatalf("engine packet_mux timer not projected: %#v", got)
	}
	projected := got[*mux]
	if projected.Component != "engine" || projected.Origin != telemetry.OriginEngine {
		t.Errorf("projected identity = %s/%s/%s; want engine origin/component",
			projected.Origin, projected.Component, projected.Action)
	}
	if projected.DurationMS != 12_700 {
		t.Errorf("packet_mux duration = %d ms; want 12700", projected.DurationMS)
	}
	if projected.Phase == "" {
		t.Error("projected phase is empty; the catalog owns the phase attribute")
	}
	if projected.EventIndex <= 3 {
		t.Errorf("projected event_index = %d; must continue the engine origin sequence (last sidecar index 3)",
			projected.EventIndex)
	}
}

// TestProjectDetailedPhasesWithoutTimersKeepsSidecarPhases is the no-op
// direction: a legacy sidecar with no phase_ms block must not gain rows.
func TestProjectDetailedPhasesWithoutTimersKeepsSidecarPhases(t *testing.T) {
	rm := pipeline.RenderMetrics{
		DetailedPhases: []pipeline.DetailedPhaseTiming{
			{
				Origin: telemetry.OriginEngine, Component: "engine", Action: "render",
				EventIndex: 1, DurationMS: 10, Status: telemetry.StatusOK,
			},
		},
	}
	got := projectDetailedPhases(rm)
	if len(got) != 1 || got[0].Action != "render" {
		t.Fatalf("projection added rows for an empty phase_ms ledger: %#v", got)
	}
}
