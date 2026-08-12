package telemetry

import (
	"context"
	"testing"
	"time"
)

type fakeCacheSource struct{ facts CacheFacts }

func (f *fakeCacheSource) CacheFacts() CacheFacts { return f.facts }

func TestCollectorRegistry_CollectInRegistrationOrder(t *testing.T) {
	registry := NewCollectorRegistry()
	registry.Register(&ProcessCollector{})
	registry.Register(&MediaCollector{})
	if registry.Len() != 2 {
		t.Fatalf("registry len=%d, want 2", registry.Len())
	}
	names := registry.Names()
	if names[0] != "process" || names[1] != "media" {
		t.Fatalf("names=%v, want [process media]", names)
	}
	snapshot := &AttemptSnapshot{}
	registry.Collect(context.Background(), snapshot)
}

func TestCollectorRegistry_IgnoresNilCollector(t *testing.T) {
	registry := NewCollectorRegistry()
	registry.Register(nil)
	if registry.Len() != 0 {
		t.Fatalf("nil collector registered, len=%d", registry.Len())
	}
}

func TestAttemptResourceCollector_CopiesSessionResult(t *testing.T) {
	session := NewAttemptTelemetrySession(nil)
	started := time.Now().Add(-2 * time.Second).UTC()
	completed := time.Now().UTC()
	session.result = &AttemptTelemetry{
		Metrics: TypedExecutionMetrics{
			CpuTimeMs:        1234,
			PeakRssBytes:     4096,
			WallClockSeconds: 2,
			DiskReadBytes:    1000,
			DiskWriteBytes:   500,
		},
		StartedAt:   started,
		CompletedAt: completed,
	}
	collector := &AttemptResourceCollector{Session: session}
	snapshot := &AttemptSnapshot{}
	collector.Collect(context.Background(), snapshot)
	if snapshot.Resources.CpuTimeMs != 1234 || snapshot.Resources.PeakRssBytes != 4096 {
		t.Fatalf("resources = %+v, want cpu=1234 rss=4096", snapshot.Resources)
	}
	if snapshot.WallMs != 2000 {
		t.Fatalf("wall_ms=%d, want 2000 (2s wall clock)", snapshot.WallMs)
	}
	if !snapshot.StartedAt.Equal(started) || !snapshot.CompletedAt.Equal(completed) {
		t.Fatalf("times not copied: %v / %v", snapshot.StartedAt, snapshot.CompletedAt)
	}
}

func TestProcessCollector_CountsSpawnEventsOnly(t *testing.T) {
	collector := &ProcessCollector{}
	snapshot := &AttemptSnapshot{Events: []RecordedPhase{
		{Component: "worker.engine", Action: "spawn"},
		{Component: "worker.engine", Action: "spawn"},
		// A failed launch is NOT a spawn: the producer only emits the
		// canonical PROCESS_STARTED fact when cmd.Start() succeeded.
		{Component: "worker.engine", Action: "spawn_failed"},
		{Component: "downloader", Action: "fetch"},
	}}
	collector.Collect(context.Background(), snapshot)
	if snapshot.Process.EngineSpawnCount != 2 {
		t.Fatalf("engine_spawn_count=%d, want 2", snapshot.Process.EngineSpawnCount)
	}
}

func TestMediaCollector_SumsMediaProducerEvents(t *testing.T) {
	collector := &MediaCollector{}
	snapshot := &AttemptSnapshot{Events: []RecordedPhase{
		{Component: "engine.video", Action: "decode", BytesIn: 10, BytesOut: 20, FramesIn: 100, FramesOut: 90},
		{Component: "ffmpeg", Action: "encode", BytesIn: 5, FramesOut: 30},
		// Non-media producers must not leak into media facts.
		{Component: "downloader", Action: "fetch", BytesIn: 999, BytesOut: 999, FramesOut: 999},
	}}
	collector.Collect(context.Background(), snapshot)
	if snapshot.Media.BytesIn != 15 || snapshot.Media.BytesOut != 20 {
		t.Fatalf("media bytes = in:%d out:%d, want in:15 out:20", snapshot.Media.BytesIn, snapshot.Media.BytesOut)
	}
	if snapshot.Media.FramesIn != 100 || snapshot.Media.FramesOut != 120 {
		t.Fatalf("media frames = in:%d out:%d, want in:100 out:120", snapshot.Media.FramesIn, snapshot.Media.FramesOut)
	}
}

func TestCacheCollector_DiffsAgainstStartBaseline(t *testing.T) {
	source := &fakeCacheSource{facts: CacheFacts{Hits: 10, Misses: 3, Evictions: 1, Corruptions: 0, BytesUsed: 100, Entries: 5}}
	collector := &CacheCollector{Source: source}
	collector.StartBaseline()

	// The cache is process-global: lifetime counters moved on between
	// Start and Stop. The collector must expose the per-attempt delta.
	source.facts = CacheFacts{Hits: 14, Misses: 5, Evictions: 2, Corruptions: 1, BytesUsed: 150, Entries: 7}
	snapshot := &AttemptSnapshot{}
	collector.Collect(context.Background(), snapshot)

	if snapshot.Cache.Hits != 4 || snapshot.Cache.Misses != 2 || snapshot.Cache.Evictions != 1 || snapshot.Cache.Corruptions != 1 {
		t.Fatalf("cache deltas = %+v, want hits:4 misses:2 evictions:1 corruptions:1", snapshot.Cache)
	}
	// Gauges are point-in-time: current values, not deltas.
	if snapshot.Cache.BytesUsed != 150 || snapshot.Cache.Entries != 7 {
		t.Fatalf("cache gauges = bytes:%d entries:%d, want bytes:150 entries:7", snapshot.Cache.BytesUsed, snapshot.Cache.Entries)
	}
	if raw := snapshot.RawMetrics(); raw.CacheLookups != 6 || raw.AssetCacheHitCount != 4 || raw.AssetCacheMissCount != 2 {
		t.Fatalf("raw cache facts = %+v, want lookups:6 hits:4 misses:2", raw)
	}
}

func TestCacheCollector_WithoutBaselineStillPublishesCurrent(t *testing.T) {
	source := &fakeCacheSource{facts: CacheFacts{Hits: 7, Misses: 1, Evictions: 0, Corruptions: 0, BytesUsed: 42, Entries: 3}}
	collector := &CacheCollector{Source: source}
	// StartBaseline never called (legacy flow): diff against zero baseline.
	snapshot := &AttemptSnapshot{}
	collector.Collect(context.Background(), snapshot)
	if snapshot.Cache.Hits != 7 || snapshot.Cache.Misses != 1 {
		t.Fatalf("cache = %+v, want hits:7 misses:1", snapshot.Cache)
	}
}
