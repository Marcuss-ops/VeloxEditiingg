package pb

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestPhaseTimingDetailedExtendedFieldsRoundTrip(t *testing.T) {
	input := &PhaseTimingDetailed{
		Origin:                 "engine",
		Scope:                  "segment",
		EventType:              "completed",
		EventName:              "engine.encode",
		EventIndex:             7,
		Phase:                  "encode",
		ExecutorId:             "scene.composite.v1",
		ExecutorVersion:        3,
		LeaseId:                "lease-7",
		WorkerSnapshotId:       "snapshot-7",
		WorkerSessionId:        "session-7",
		SegmentIndex:           3,
		TrackKind:              "voiceover",
		TrackIndex:             1,
		StartedOffsetMs:        1250.5,
		FinishedOffsetMs:       8420.25,
		CpuMs:                  3799.75,
		QueueWaitMs:            12.5,
		FramesIn:               900,
		FramesOut:              897,
		ArtifactId:             "artifact-7",
		TelemetrySchemaVersion: 1,
	}

	data, err := proto.Marshal(input)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var output PhaseTimingDetailed
	if err := proto.Unmarshal(data, &output); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if output.GetOrigin() != input.GetOrigin() || output.GetScope() != input.GetScope() || output.GetEventType() != input.GetEventType() || output.GetEventName() != input.GetEventName() || output.GetEventIndex() != input.GetEventIndex() || output.GetPhase() != input.GetPhase() || output.GetTelemetrySchemaVersion() != input.GetTelemetrySchemaVersion() {
		t.Fatalf("taxonomy fields lost: got origin=%q scope=%q type=%q name=%q index=%d phase=%q", output.GetOrigin(), output.GetScope(), output.GetEventType(), output.GetEventName(), output.GetEventIndex(), output.GetPhase())
	}
	if output.GetExecutorId() != input.GetExecutorId() || output.GetExecutorVersion() != input.GetExecutorVersion() || output.GetLeaseId() != input.GetLeaseId() || output.GetWorkerSnapshotId() != input.GetWorkerSnapshotId() || output.GetWorkerSessionId() != input.GetWorkerSessionId() {
		t.Fatalf("identity fields lost: got executor=%q@%d lease=%q snapshot=%q session=%q", output.GetExecutorId(), output.GetExecutorVersion(), output.GetLeaseId(), output.GetWorkerSnapshotId(), output.GetWorkerSessionId())
	}
	if output.GetSegmentIndex() != input.GetSegmentIndex() || output.GetTrackKind() != input.GetTrackKind() || output.GetTrackIndex() != input.GetTrackIndex() {
		t.Fatalf("segment/track fields lost: got segment=%d track=%q/%d", output.GetSegmentIndex(), output.GetTrackKind(), output.GetTrackIndex())
	}
	if output.GetStartedOffsetMs() != input.GetStartedOffsetMs() || output.GetFinishedOffsetMs() != input.GetFinishedOffsetMs() {
		t.Fatalf("offset fields lost: got %v/%v", output.GetStartedOffsetMs(), output.GetFinishedOffsetMs())
	}
	if output.GetCpuMs() != input.GetCpuMs() || output.GetQueueWaitMs() != input.GetQueueWaitMs() {
		t.Fatalf("resource wait fields lost: got cpu=%v queue=%v", output.GetCpuMs(), output.GetQueueWaitMs())
	}
	if output.GetFramesIn() != input.GetFramesIn() || output.GetFramesOut() != input.GetFramesOut() || output.GetArtifactId() != input.GetArtifactId() || output.GetTelemetrySchemaVersion() != input.GetTelemetrySchemaVersion() {
		t.Fatalf("frame/artifact fields lost: got in=%d out=%d artifact=%q", output.GetFramesIn(), output.GetFramesOut(), output.GetArtifactId())
	}
}

func TestTaskResultKeepsLegacyPartialPhaseMetricsField(t *testing.T) {
	input := &TaskResult{
		PartialPhaseMetrics: []*PhaseTimingDetailed{{
			PhaseOrder: 1,
			Component:  "engine",
			Action:     "encode",
			Status:     "failed",
		}},
		PhaseTimings: []*PhaseTimingDetailed{{
			EventIndex:   1,
			SegmentIndex: 2,
		}},
	}

	data, err := proto.Marshal(input)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var output TaskResult
	if err := proto.Unmarshal(data, &output); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(output.GetPartialPhaseMetrics()) != 1 || output.GetPartialPhaseMetrics()[0].GetAction() != "encode" {
		t.Fatalf("legacy partial_phase_metrics not preserved: %+v", output.GetPartialPhaseMetrics())
	}
	if len(output.GetPhaseTimings()) != 1 || output.GetPhaseTimings()[0].GetSegmentIndex() != 2 {
		t.Fatalf("phase_timings not preserved: %+v", output.GetPhaseTimings())
	}
}
