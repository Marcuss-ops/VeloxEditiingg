package telemetry

import (
	"context"
	"encoding/json"
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

func TestCompleteRawEnvelope_PreservesJSONSectionsAndTypedOwners(t *testing.T) {
	snapshot := &AttemptSnapshot{
		Resources: RawExecutionMetrics{CpuTimeMs: 12},
		Process:   ProcessFacts{EngineSpawnCount: 1},
		Media:     MediaFacts{FramesOut: 24},
		Cache:     CacheFacts{Hits: 3},
	}
	envelope := snapshot.RawMetrics()
	if envelope.Resources.CpuTimeMs != 12 || envelope.Process.EngineSpawnCount != 1 ||
		envelope.Media.FramesOut != 24 || envelope.Cache.Hits != 3 {
		t.Fatalf("complete raw envelope = %+v", envelope)
	}
	data, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal complete raw envelope: %v", err)
	}
	for _, key := range []string{`"resources"`, `"process"`, `"media"`, `"cache"`} {
		if !containsJSONKey(data, key) {
			t.Fatalf("complete raw envelope missing %s: %s", key, data)
		}
	}
	// The historical snapshot JSON remains unchanged: the four sections are
	// still top-level wire fields, not a new nested "raw" field.
	snapshotJSON, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	if containsJSONKey(snapshotJSON, `"raw"`) {
		t.Fatalf("snapshot acquired an incompatible raw wire field: %s", snapshotJSON)
	}
}

func containsJSONKey(data []byte, key string) bool {
	for i := 0; i+len(key) <= len(data); i++ {
		if string(data[i:i+len(key)]) == key {
			return true
		}
	}
	return false
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
	collector.Collect(context.Background(), snapshot)
	if snapshot.Process.EngineSpawnCount != 2 {
		t.Fatalf("engine_spawn_count=%d, want 2 after repeated collection", snapshot.Process.EngineSpawnCount)
	}
	// The usage event's absent metadata must not disturb the process facts.
	if snapshot.Process.EngineExternalSpawnCount != 0 {
		t.Fatalf("engine_external_spawn_count=%d, want 0", snapshot.Process.EngineExternalSpawnCount)
	}
}

func TestProcessCollector_ProjectsEngineUsageEvent(t *testing.T) {
	collector := &ProcessCollector{}
	snapshot := &AttemptSnapshot{Events: []RecordedPhase{
		{Component: "worker.engine", Action: "usage", MetadataJSON: EngineUsageMetadataJSON(EngineUsageFacts{
			ExternalSpawnCount:         2,
			FfmpegSpawnCount:           1,
			FfprobeSpawnCount:          1,
			CPUUserMs:                  1420,
			CPUSystemMs:                310,
			VoluntaryContextSwitches:   841,
			InvoluntaryContextSwitches: 23,
			MinorPageFaults:            4021,
			MajorPageFaults:            0,
		})},
		// A malformed usage payload is skipped without corrupting facts.
		{Component: "worker.engine", Action: "usage", MetadataJSON: "{not json"},
	}}
	collector.Collect(context.Background(), snapshot)
	p := snapshot.Process
	if p.EngineExternalSpawnCount != 2 || p.EngineFfmpegSpawnCount != 1 || p.EngineFfprobeSpawnCount != 1 {
		t.Fatalf("engine spawn ledger = %+v", p)
	}
	if p.EngineCPUUserMs != 1420 || p.EngineCPUSystemMs != 310 ||
		p.EngineVoluntaryContextSwitches != 841 || p.EngineInvoluntaryContextSwitches != 23 ||
		p.EngineMinorPageFaults != 4021 || p.EngineMajorPageFaults != 0 {
		t.Fatalf("engine usage facts = %+v", p)
	}
	// All-zero usage facts produce no metadata payload at all.
	if got := EngineUsageMetadataJSON(EngineUsageFacts{}); got != "" {
		t.Fatalf("all-zero usage marshaled to %q, want empty", got)
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
	raw := snapshot.RawMetrics()
	if raw.Resources.CacheLookups != 6 || raw.Resources.AssetCacheHitCount != 4 || raw.Resources.AssetCacheMissCount != 2 {
		t.Fatalf("raw cache facts = %+v, want lookups:6 hits:4 misses:2", raw)
	}
	if raw.Cache.Hits != 4 || raw.Cache.Misses != 2 || raw.Cache.Evictions != 1 {
		t.Fatalf("complete raw cache envelope = %+v", raw.Cache)
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
