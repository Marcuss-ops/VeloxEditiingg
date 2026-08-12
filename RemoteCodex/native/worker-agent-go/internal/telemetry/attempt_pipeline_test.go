package telemetry

import (
	"context"
	"testing"
)

func TestAttemptPipeline_CollectAndPublishRoundTrip(t *testing.T) {
	pipeline := NewAttemptPipeline()
	pipeline.SetIdentity(AttemptIdentity{JobID: "job-1", AttemptID: "attempt-1", WorkerID: "worker-1"})

	rec := NewEventRecorder()
	rec.Emit(EventSpec{Origin: OriginWorker, Scope: ScopeAttempt, Component: "worker.engine", Action: "spawn"}, StatusOK, "", "")
	rec.Emit(EventSpec{Origin: OriginWorker, Scope: ScopeAttempt, Component: "worker.engine", Action: "spawn"}, StatusOK, "", "")
	rec.Emit(EventSpec{Origin: OriginEngine, Scope: ScopeSegment, Component: "engine.video", Action: "decode", FramesIn: 10, FramesOut: 9, MetadataJSON: ""}, StatusOK, "", "")

	source := &fakeCacheSource{facts: CacheFacts{Hits: 10, Misses: 1, Evictions: 0, Corruptions: 0, BytesUsed: 64, Entries: 2}}
	pipeline.BindRecorder(rec)
	pipeline.BindCacheFactsSource(source)
	pipeline.AddCollector(&ProcessCollector{})
	pipeline.AddCollector(&MediaCollector{})
	pipeline.AddCollector(&CacheCollector{Source: source})

	capture := &captureSink{}
	pipeline.AddSink(capture)

	// The registries expose their ordered names for diagnostics: pin the
	// registration order (collectors run before sinks publish).
	if names := pipeline.CollectorNames(); len(names) != 3 || names[0] != "process" || names[2] != "cache" {
		t.Fatalf("collector names=%v", names)
	}
	if names := pipeline.SinkNames(); len(names) != 1 || names[0] != "capture" {
		t.Fatalf("sink names=%v", names)
	}

	pipeline.StartBaseline()
	source.facts = CacheFacts{Hits: 13, Misses: 2, Evictions: 1, Corruptions: 0, BytesUsed: 128, Entries: 3}

	if err := pipeline.Run(context.Background()); err != nil {
		t.Fatalf("pipeline run: %v", err)
	}
	if !capture.published {
		t.Fatal("sink was not published")
	}
	snapshot := capture.snapshot
	if snapshot.Identity.AttemptID != "attempt-1" {
		t.Fatalf("identity = %+v", snapshot.Identity)
	}
	raw := snapshot.RawMetrics()
	if raw.Process.EngineSpawnCount != 2 {
		t.Fatalf("process = %+v, want 2 spawns", raw.Process)
	}
	if raw.Media.FramesIn != 10 || raw.Media.FramesOut != 9 {
		t.Fatalf("media = %+v, want frames in:10 out:9", raw.Media)
	}
	if raw.Cache.Hits != 3 || raw.Cache.Misses != 1 || raw.Cache.Evictions != 1 {
		t.Fatalf("cache = %+v, want hits:3 misses:1 evictions:1 (baseline diff)", raw.Cache)
	}
	if len(snapshot.Events) != 3 {
		t.Fatalf("events=%d, want 3 (journal snapshot is non-destructive)", len(snapshot.Events))
	}
	// The journal must remain intact: the recorder still owns all events.
	if got := len(rec.Snapshot()); got != 3 {
		t.Fatalf("recorder events after run = %d, want 3", got)
	}
}

func TestAttemptPipelineProjectsDroppedEventCount(t *testing.T) {
	pipeline := NewAttemptPipeline()
	rec := NewEventRecorder()
	for i := 0; i < MaxAttemptEvents+1; i++ {
		rec.Emit(EventSpec{Origin: OriginFFmpeg, Scope: ScopeSegment, Component: "ffmpeg", Action: "progress"}, StatusOK, "", "")
	}
	pipeline.BindRecorder(rec)
	capture := &captureSink{}
	pipeline.AddSink(capture)
	if err := pipeline.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if capture.snapshot.DroppedEvents != 1 {
		t.Fatalf("snapshot dropped_events=%d, want 1", capture.snapshot.DroppedEvents)
	}
}

func TestAttemptPipeline_StartBaselineRunsOnce(t *testing.T) {
	pipeline := NewAttemptPipeline()
	source := &fakeCacheSource{facts: CacheFacts{Hits: 5}}
	collector := &CacheCollector{Source: source}
	pipeline.AddCollector(collector)

	pipeline.StartBaseline()
	source.facts = CacheFacts{Hits: 9}
	// A second StartBaseline (never happens via the session) must not
	// re-baseline: the session calls StartBaseline exactly once.
	pipeline.StartBaseline()

	capture := &captureSink{}
	pipeline.AddSink(capture)
	if err := pipeline.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if capture.snapshot.Cache.Hits != 4 {
		t.Fatalf("cache hits=%d, want 4 (baseline captured once)", capture.snapshot.Cache.Hits)
	}
}

// TestAttemptTelemetrySession_DrivesPipeline pins the single entry point:
// Start/Stop drive collect → publish, and producers never call a sink.
func TestAttemptTelemetrySession_DrivesPipeline(t *testing.T) {
	session := NewAttemptTelemetrySession(nil)
	pipeline := NewAttemptPipeline()
	rec := NewEventRecorder()
	rec.Emit(EventSpec{Origin: OriginWorker, Scope: ScopeAttempt, Component: "worker.engine", Action: "spawn"}, StatusOK, "", "")
	pipeline.BindRecorder(rec)
	pipeline.AddCollector(&AttemptResourceCollector{Session: session})
	pipeline.AddCollector(&ProcessCollector{})
	capture := &captureSink{}
	pipeline.AddSink(capture)
	session.BindPipeline(pipeline)

	ctx := context.Background()
	session.Start(ctx)
	result := session.Stop(ctx)
	if result.Metrics.WallClockSeconds < 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if !capture.published {
		t.Fatal("pipeline did not run at Stop")
	}
	if capture.snapshot.Resources.CpuTimeMs != result.Metrics.CpuTimeMs {
		t.Fatalf("snapshot resources %+v != session result %+v", capture.snapshot.Resources, result.Metrics)
	}
	if raw := capture.snapshot.RawMetrics(); raw.Process.EngineSpawnCount != 1 {
		t.Fatalf("process spawns=%d, want 1", raw.Process.EngineSpawnCount)
	}
	// The resource collector stamped the attempt window from the session.
	if capture.snapshot.StartedAt.IsZero() || capture.snapshot.CompletedAt.IsZero() {
		t.Fatal("snapshot times not stamped by the resource collector")
	}
	if !capture.snapshot.CompletedAt.After(capture.snapshot.StartedAt) {
		t.Fatal("snapshot window must be ordered")
	}
	if capture.snapshot.WallMs < 0 {
		t.Fatalf("wall_ms=%d, want >= 0", capture.snapshot.WallMs)
	}
}

func TestAttemptTelemetrySession_NilPipelineKeepsLegacyBehavior(t *testing.T) {
	session := NewAttemptTelemetrySession(nil)
	ctx := context.Background()
	session.Start(ctx)
	result := session.Stop(ctx)
	if result.Metrics.WallClockSeconds < 0 {
		t.Fatalf("legacy session broke: %+v", result)
	}
}
