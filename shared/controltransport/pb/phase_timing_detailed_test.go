package pb

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestPhaseTimingDetailedExtendedFieldsRoundTrip(t *testing.T) {
	input := &PhaseTimingDetailed{
		EventIndex:       7,
		Scope:            "segment",
		SegmentIndex:     3,
		TrackKind:        "voiceover",
		TrackIndex:       1,
		StartedOffsetMs:  1250.5,
		FinishedOffsetMs: 8420.25,
		CpuMs:            3799.75,
		QueueWaitMs:      12.5,
		FramesIn:         900,
		FramesOut:        897,
	}

	data, err := proto.Marshal(input)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var output PhaseTimingDetailed
	if err := proto.Unmarshal(data, &output); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if output.GetEventIndex() != input.GetEventIndex() || output.GetScope() != input.GetScope() {
		t.Fatalf("existing taxonomy fields lost: got event=%d scope=%q", output.GetEventIndex(), output.GetScope())
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
	if output.GetFramesIn() != input.GetFramesIn() || output.GetFramesOut() != input.GetFramesOut() {
		t.Fatalf("frame fields lost: got in=%d out=%d", output.GetFramesIn(), output.GetFramesOut())
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
