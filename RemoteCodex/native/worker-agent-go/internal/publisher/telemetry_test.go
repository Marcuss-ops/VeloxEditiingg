package publisher

import (
	"testing"
	"time"
)

func TestUploadTelemetrySnapshotCalculatesBytesThroughputAndChunks(t *testing.T) {
	m := &uploadTelemetry{started: time.Now().Add(-2 * time.Second)}
	m.ChunkCompleted(1_000_000)
	m.ChunkCompleted(500_000)
	m.FinalizeCompleted(25 * time.Millisecond)

	got := m.Snapshot().UploadBreakdown
	if got.UploadBytes != 1_500_000 || got.ChunkCount != 2 || got.RetryCount != 0 {
		t.Fatalf("breakdown counters = %+v", got)
	}
	if got.UploadMbps <= 0 || got.UploadMS < 1900 {
		t.Fatalf("breakdown timing/throughput = %+v", got)
	}
	if got.RemoteFinalizeMS != 25 {
		t.Fatalf("remote finalize = %d, want 25", got.RemoteFinalizeMS)
	}
}

func TestUploadTelemetryRetryCounter(t *testing.T) {
	m := &uploadTelemetry{started: time.Now()}
	m.Retry()
	m.Retry()
	if got := m.Snapshot().RetryCount; got != 2 {
		t.Fatalf("retry count = %d, want 2", got)
	}
}
