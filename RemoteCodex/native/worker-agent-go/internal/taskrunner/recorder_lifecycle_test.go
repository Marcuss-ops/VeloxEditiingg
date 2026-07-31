package taskrunner

import (
	"testing"

	"velox-worker-agent/internal/telemetry"
)

func TestAppendDetailedPhasesDrainsOnlyPostRunEvents(t *testing.T) {
	rec := telemetry.NewEventRecorder()
	rec.Emit(telemetry.EventSpec{
		Origin: telemetry.OriginWorker, Scope: telemetry.ScopeAttempt,
		Component: "runner", Action: "execute",
	}, telemetry.StatusOK, "", "")

	report := &TaskExecutionReport{
		ExecutorID:            "scene.composite.v1",
		ExecutorKey:           "scene.composite.v1@1",
		AttemptRecorder:       rec,
		AttemptRecorderOffset: 1,
	}
	report.DetailedPhases = []DetailedPhaseTiming{{
		PhaseOrder: 1, Origin: telemetry.OriginWorker, Scope: telemetry.ScopeAttempt,
		Component: "runner", Action: "execute", EventIndex: 0,
	}}

	rec.Emit(telemetry.EventSpec{
		Origin: telemetry.OriginUpload, Scope: telemetry.ScopeArtifact,
		Component: "worker.upload", Action: "transfer",
	}, telemetry.StatusOK, "", "")

	AppendDetailedPhases(report, rec)
	if len(report.DetailedPhases) != 2 {
		t.Fatalf("detailed phases = %d, want 2", len(report.DetailedPhases))
	}
	if got := report.DetailedPhases[1]; got.Component != "worker.upload" || got.Action != "transfer" {
		t.Fatalf("post-run event = %+v, want worker.upload/transfer", got)
	}
	if report.DetailedPhases[1].PhaseOrder != 2 {
		t.Fatalf("post-run phase order = %d, want 2", report.DetailedPhases[1].PhaseOrder)
	}
	if remaining := rec.Snapshot(); len(remaining) != 0 {
		t.Fatalf("recorder retained %d events after final drain", len(remaining))
	}
}
