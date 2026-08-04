package taskrunner

import "testing"

func TestDetailedPhaseTimingToProtoMapsExtendedFields(t *testing.T) {
	input := DetailedPhaseTiming{
		PhaseOrder:             4,
		Component:              "engine.encode",
		Action:                 "frame_submit",
		Origin:                 "engine",
		Scope:                  "segment",
		TelemetrySchemaVersion: 1,
		EventIndex:             17,
		SegmentIndex:           3,
		TrackKind:              "voiceover",
		TrackIndex:             1,
		StartedOffsetMS:        1250.5,
		FinishedOffsetMS:       8420.25,
		CPUMS:                  3799.75,
		QueueWaitMS:            12.5,
		FramesIn:               900,
		FramesOut:              897,
	}

	output := input.ToProto()

	if int(output.GetPhaseOrder()) != input.PhaseOrder || output.GetEventIndex() != input.EventIndex {
		t.Fatalf("identity/order fields = phase:%d event:%d, want phase:%d event:%d", output.GetPhaseOrder(), output.GetEventIndex(), input.PhaseOrder, input.EventIndex)
	}
	if output.GetSegmentIndex() != input.SegmentIndex || output.GetTrackKind() != input.TrackKind || output.GetTrackIndex() != input.TrackIndex {
		t.Fatalf("segment/track fields = %d/%q/%d, want %d/%q/%d", output.GetSegmentIndex(), output.GetTrackKind(), output.GetTrackIndex(), input.SegmentIndex, input.TrackKind, input.TrackIndex)
	}
	if output.GetStartedOffsetMs() != input.StartedOffsetMS || output.GetFinishedOffsetMs() != input.FinishedOffsetMS {
		t.Fatalf("offset fields = %v/%v, want %v/%v", output.GetStartedOffsetMs(), output.GetFinishedOffsetMs(), input.StartedOffsetMS, input.FinishedOffsetMS)
	}
	if output.GetCpuMs() != input.CPUMS || output.GetQueueWaitMs() != input.QueueWaitMS {
		t.Fatalf("resource timing fields = cpu:%v queue:%v, want cpu:%v queue:%v", output.GetCpuMs(), output.GetQueueWaitMs(), input.CPUMS, input.QueueWaitMS)
	}
	if output.GetFramesIn() != input.FramesIn || output.GetFramesOut() != input.FramesOut {
		t.Fatalf("frame fields = %d/%d, want %d/%d", output.GetFramesIn(), output.GetFramesOut(), input.FramesIn, input.FramesOut)
	}
}
