package publisher

import (
	"sync"
	"time"
)

// UploadBreakdown records the measured data-plane work for one artifact
// upload. Durations are wall-clock milliseconds; bytes and counts are totals.
//
// The progressive fields are zero on the legacy non-progressive path and on
// engines/transports that predate the artifact_write_progress stream.
type UploadBreakdown struct {
	UploadMS         int64
	UploadBytes      int64
	UploadMbps       float64
	ChunkCount       int64
	RetryCount       int64
	RemoteFinalizeMS int64
	// FirstPartStartedMS is the time between the upload run start and the
	// first part actually sent. Zero when no part was uploaded.
	FirstPartStartedMS int64
	// PartsUploadedBeforeRenderEnd/BytesUploadedBeforeRenderEnd count the
	// parts (and bytes) whose UploadPart completed while the engine was
	// still rendering (before the output was declared finalized).
	PartsUploadedBeforeRenderEnd int64
	BytesUploadedBeforeRenderEnd int64
	// OverlapMS is the render/upload overlap window: render end minus
	// first-part start, zero when the upload began after finalization.
	OverlapMS int64
}

// UploadTelemetry receives upload progress and terminal measurements.
type UploadTelemetry interface {
	ChunkCompleted(bytes int64)
	Retry()
	FinalizeCompleted(duration time.Duration)
}

// UploadTelemetrySnapshot is the immutable result exposed by the transport.
type UploadTelemetrySnapshot struct {
	UploadBreakdown
}

type uploadTelemetry struct {
	mu         sync.Mutex
	started    time.Time
	bytes      int64
	chunks     int64
	retries    int64
	finalizeMS int64
}

func (t *uploadTelemetry) ChunkCompleted(bytes int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.bytes += bytes
	t.chunks++
}

func (t *uploadTelemetry) Retry() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.retries++
}

func (t *uploadTelemetry) FinalizeCompleted(duration time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.finalizeMS = duration.Milliseconds()
}

func NewUploadTelemetry(started time.Time) UploadTelemetrySnapshot {
	if started.IsZero() {
		started = time.Now()
	}
	m := &uploadTelemetry{started: started}
	return m.Snapshot()
}

func (t *uploadTelemetry) Snapshot() UploadTelemetrySnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	ms := time.Since(t.started).Milliseconds()
	mbps := 0.0
	if ms > 0 {
		mbps = float64(t.bytes*8) / (float64(ms) / 1000) / 1_000_000
	}
	return UploadTelemetrySnapshot{UploadBreakdown: UploadBreakdown{
		UploadMS: ms, UploadBytes: t.bytes, UploadMbps: mbps,
		ChunkCount: t.chunks, RetryCount: t.retries,
		RemoteFinalizeMS: t.finalizeMS,
	}}
}
