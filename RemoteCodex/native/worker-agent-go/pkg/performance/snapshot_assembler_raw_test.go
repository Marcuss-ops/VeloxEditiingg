package performance

import (
	"testing"

	attempttelemetry "velox-worker-agent/internal/telemetry"
)

func TestAssembleFromSnapshot_RawTypedOutputFactWins(t *testing.T) {
	snapshot := &attempttelemetry.AttemptSnapshot{
		WallMs: 1000,
		Resources: attempttelemetry.RawExecutionMetrics{
			CpuTimeMs:     100,
			DiskReadBytes: 1000,
			OutputBytes:   200,
		},
		// MediaFacts is retained as a typed journal projection. It must not
		// override the publisher-owned output fact already present in the
		// canonical raw envelope.
		Media: attempttelemetry.MediaFacts{BytesOut: 999, FramesOut: 30},
	}

	receipt := AssembleFromSnapshot(snapshot)
	if receipt.IO.FinalBytesWritten != 200 {
		t.Fatalf("final output bytes = %d, want raw publisher value 200", receipt.IO.FinalBytesWritten)
	}
	if receipt.Media.OutputBytes != 999 {
		t.Fatalf("media bytes = %d, want media-owner observation 999", receipt.Media.OutputBytes)
	}
	if receipt.Derived.ReadAmplification != 5 {
		t.Fatalf("read amplification = %v, want 5 from raw output denominator", receipt.Derived.ReadAmplification)
	}
}
