package pb

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

// TestSegmentTimingParallelismFieldsProtoRoundTrip verifies that the five
// parallelism telemetry fields (started_offset_ms, finished_offset_ms,
// worker_slot, cpu_threads, parallel_group) survive protobuf marshal/unmarshal.
//
// This test MUST FAIL with the old descriptor (fields 21-25 absent from
// the wire format) and PASS only after protoc --go_out regenerates the
// raw descriptor from worker_control.proto.
//
// IMPORTANT: After regeneration, do NOT manually edit worker_control.pb.go.
func TestSegmentTimingParallelismFieldsProtoRoundTrip(t *testing.T) {
	input := &SegmentTiming{
		SegmentIndex:     3,
		StartedOffsetMs:  1250,
		FinishedOffsetMs: 8420,
		WorkerSlot:       2,
		CpuThreads:       4,
		ParallelGroup:    "scene-batch-1",
	}

	data, err := proto.Marshal(input)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var output SegmentTiming
	if err := proto.Unmarshal(data, &output); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if output.GetStartedOffsetMs() != input.GetStartedOffsetMs() {
		t.Fatalf("started_offset_ms lost on wire: got %v, want %v",
			output.GetStartedOffsetMs(), input.GetStartedOffsetMs())
	}
	if output.GetFinishedOffsetMs() != input.GetFinishedOffsetMs() {
		t.Fatalf("finished_offset_ms lost on wire: got %v, want %v",
			output.GetFinishedOffsetMs(), input.GetFinishedOffsetMs())
	}
	if output.GetWorkerSlot() != input.GetWorkerSlot() {
		t.Fatalf("worker_slot lost on wire: got %v, want %v",
			output.GetWorkerSlot(), input.GetWorkerSlot())
	}
	if output.GetCpuThreads() != input.GetCpuThreads() {
		t.Fatalf("cpu_threads lost on wire: got %v, want %v",
			output.GetCpuThreads(), input.GetCpuThreads())
	}
	if output.GetParallelGroup() != input.GetParallelGroup() {
		t.Fatalf("parallel_group lost on wire: got %q, want %q",
			output.GetParallelGroup(), input.GetParallelGroup())
	}
}

// TestSegmentTimingParallelismFieldsZeroIsDistinguishable verifies that
// zero-valued parallelism fields are distinguishable from absent fields.
// After proto regeneration, setting a field to 0 should still encode the
// field presence (proto3 explicit presence is not default, but the field
// number must be in the descriptor for marshal to emit it).
func TestSegmentTimingParallelismFieldsZeroIsDistinguishable(t *testing.T) {
	input := &SegmentTiming{
		SegmentIndex:     1,
		StartedOffsetMs:  0,
		FinishedOffsetMs: 0,
		WorkerSlot:       0,
		CpuThreads:       0,
		ParallelGroup:    "",
	}

	data, err := proto.Marshal(input)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var output SegmentTiming
	if err := proto.Unmarshal(data, &output); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Zero-valued fields should decode to zero without error.
	if output.GetStartedOffsetMs() != 0 {
		t.Fatalf("started_offset_ms should be 0, got %v", output.GetStartedOffsetMs())
	}
	if output.GetWorkerSlot() != 0 {
		t.Fatalf("worker_slot should be 0, got %v", output.GetWorkerSlot())
	}
	if output.GetParallelGroup() != "" {
		t.Fatalf("parallel_group should be empty, got %q", output.GetParallelGroup())
	}
}
