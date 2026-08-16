package taskrunner

import (
	"testing"

	pb "velox-shared/controltransport/pb"

	"google.golang.org/protobuf/proto"
)

func TestDetailedPhaseTimingToProtoRoundTripPreservesTelemetryContract(t *testing.T) {
	input := DetailedPhaseTiming{
		Component: "engine.encode", Action: "setup",
		Origin: "engine", Scope: "segment", SchemaVersion: 1,
		EventType: "completed", EventIndex: 7,
	}
	wire := input.ToProto()
	data, err := proto.Marshal(wire)
	if err != nil {
		t.Fatalf("marshal phase timing: %v", err)
	}
	var roundTrip pb.PhaseTimingDetailed
	if err := proto.Unmarshal(data, &roundTrip); err != nil {
		t.Fatalf("unmarshal phase timing: %v", err)
	}
	if roundTrip.GetOrigin() != input.Origin || roundTrip.GetScope() != input.Scope ||
		roundTrip.GetComponent() != input.Component || roundTrip.GetAction() != input.Action ||
		roundTrip.GetTelemetrySchemaVersion() != input.SchemaVersion || roundTrip.GetEventIndex() != input.EventIndex {
		t.Fatalf("telemetry contract changed across protobuf: got origin=%q scope=%q component=%q action=%q schema=%d index=%d",
			roundTrip.GetOrigin(), roundTrip.GetScope(), roundTrip.GetComponent(), roundTrip.GetAction(), roundTrip.GetTelemetrySchemaVersion(), roundTrip.GetEventIndex())
	}
}

func TestTypedMetricsFromMapDerivesFinalStreamCopyFromConcatMode(t *testing.T) {
	metrics := TypedMetricsFromMap(map[string]interface{}{
		"engine.concat_mode": "stream_copy",
	})
	if metrics == nil || !metrics.FinalConcatStreamCopy || metrics.ConcatMode != "stream_copy" {
		t.Fatalf("stream-copy projection = %#v, want final_concat_stream_copy=true and concat_mode=stream_copy", metrics)
	}
}

func TestTypedMetricsFromMapDerivesFinalStreamCopyFromPacketCopy(t *testing.T) {
	metrics := TypedMetricsFromMap(map[string]interface{}{
		"engine.concat_mode": "packet_copy",
	})
	if metrics == nil || !metrics.FinalConcatStreamCopy || metrics.ConcatMode != "packet_copy" {
		t.Fatalf("packet-copy projection = %#v, want final_concat_stream_copy=true and concat_mode=packet_copy", metrics)
	}
}

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
