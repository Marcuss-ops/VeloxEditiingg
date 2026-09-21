package worker

import (
	"testing"

	"velox-worker-agent/internal/publisher"
	"velox-worker-agent/internal/telemetry"
)

func TestRecordUploadMetricsDoesNotDoubleCountRendererOutputBytes(t *testing.T) {
	raw := &telemetry.RawExecutionMetrics{OutputBytes: 26_825_233}
	recordUploadMetrics(raw, publisher.UploadBreakdown{
		UploadBytes:                  26_825_233,
		UploadMbps:                   42.5,
		FirstPartStartedMS:           120,
		PartsUploadedBeforeRenderEnd: 3,
		BytesUploadedBeforeRenderEnd: 8_388_608,
		OverlapMS:                    900,
		TrailerToOpenMS:              30,
		MuxToOpenUS:                  45,
	}, 0)

	if raw.OutputBytes != 26_825_233 {
		t.Fatalf("OutputBytes = %d, want renderer-owned file size 26825233", raw.OutputBytes)
	}
	if raw.ProgressiveOverlapBytesBeforeRender != 8_388_608 || raw.ProgressiveOverlapMs != 900 {
		t.Fatalf("progressive metrics = %+v", *raw)
	}
}

func TestRecordUploadMetricsAggregatesSecondaryArtifactOverlapOnly(t *testing.T) {
	raw := &telemetry.RawExecutionMetrics{OutputBytes: 1000}
	recordUploadMetrics(raw, publisher.UploadBreakdown{
		UploadBytes:                  1000,
		PartsUploadedBeforeRenderEnd: 2,
		BytesUploadedBeforeRenderEnd: 200,
		OverlapMS:                    50,
	}, 0)
	recordUploadMetrics(raw, publisher.UploadBreakdown{
		UploadBytes:                  500,
		PartsUploadedBeforeRenderEnd: 1,
		BytesUploadedBeforeRenderEnd: 100,
		OverlapMS:                    80,
	}, 1)

	if raw.OutputBytes != 1000 {
		t.Fatalf("secondary artifact changed OutputBytes to %d", raw.OutputBytes)
	}
	if raw.ProgressiveOverlapPartsBeforeRender != 3 || raw.ProgressiveOverlapBytesBeforeRender != 300 || raw.ProgressiveOverlapMs != 80 {
		t.Fatalf("aggregated overlap = %+v", *raw)
	}
}
