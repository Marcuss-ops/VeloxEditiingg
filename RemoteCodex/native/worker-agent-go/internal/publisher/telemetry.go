package publisher

import "time"

// UploadBreakdown records the measured data-plane work for one artifact
// upload. Durations are wall-clock milliseconds; bytes and counts are totals.
type UploadBreakdown struct {
	UploadMS         int64
	UploadBytes      int64
	UploadMbps       float64
	ChunkCount       int64
	RetryCount       int64
	RemoteFinalizeMS int64
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
	started    time.Time
	bytes      int64
	chunks     int64
	retries    int64
	finalizeMS int64
}

func (t *uploadTelemetry) ChunkCompleted(bytes int64) {
	t.bytes += bytes
	t.chunks++
}

func (t *uploadTelemetry) Retry() { t.retries++ }

func (t *uploadTelemetry) FinalizeCompleted(duration time.Duration) {
	t.finalizeMS = duration.Milliseconds()
}

func NewUploadTelemetry(started time.Time) UploadTelemetrySnapshot {
	if started.IsZero() { started = time.Now() }
	m := &uploadTelemetry{started: started}
	return m.Snapshot()
}

func (t *uploadTelemetry) Snapshot() UploadTelemetrySnapshot {
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
