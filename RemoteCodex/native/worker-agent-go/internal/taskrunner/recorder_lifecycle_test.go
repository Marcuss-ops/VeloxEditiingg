package taskrunner

import (
	"testing"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/telemetry"
)

func TestImportExecutorDetailedPhasesUsesCanonicalRecorder(t *testing.T) {
	rec := telemetry.NewEventRecorder()
	err := importExecutorDetailedPhases(rec, []executor.DetailedPhaseTiming{{
		Origin: "engine", Scope: "segment", Component: "engine.video", Action: "decode",
		EventIndex: 4, Status: telemetry.StatusOK, DurationMS: 3,
	}})
	if err != nil {
		t.Fatalf("importExecutorDetailedPhases: %v", err)
	}
	events := rec.Snapshot()
	if len(events) != 1 || events[0].Component != "engine.video" || events[0].EventIndex != 4 {
		t.Fatalf("canonical recorder events = %+v, want imported engine event at index 4", events)
	}
}

func TestAppendDetailedPhasesSnapshotsOnlyPostRunEvents(t *testing.T) {
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
	if remaining := rec.Snapshot(); len(remaining) != 2 {
		t.Fatalf("append-only recorder retained %d events, want 2", len(remaining))
	}
}
