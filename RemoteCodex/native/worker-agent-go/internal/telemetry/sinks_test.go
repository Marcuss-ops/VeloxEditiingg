package telemetry

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSinkRegistry_PublishesAllSinksAndAggregatesErrors(t *testing.T) {
	registry := NewSinkRegistry()
	calls := 0
	registry.Register(&captureSink{name: "first", fn: func(*AttemptSnapshot) error { calls++; return errors.New("first boom") }})
	registry.Register(&captureSink{name: "second", fn: func(*AttemptSnapshot) error { calls++; return nil }})
	if registry.Len() != 2 {
		t.Fatalf("len=%d, want 2", registry.Len())
	}
	err := registry.Publish(context.Background(), &AttemptSnapshot{})
	if err == nil || !strings.Contains(err.Error(), "first") {
		t.Fatalf("aggregated error = %v, want the first sink's error", err)
	}
	if calls != 2 {
		t.Fatalf("sink calls=%d, want 2 (a failing sink must not stop the others)", calls)
	}
}

func TestSinkRegistry_IsolatesSnapshotMutationBetweenSinks(t *testing.T) {
	registry := NewSinkRegistry()
	var observed string
	registry.Register(&captureSink{name: "mutator", fn: func(snapshot *AttemptSnapshot) error {
		snapshot.Events[0].Action = "mutated"
		return nil
	}})
	registry.Register(&captureSink{name: "observer", fn: func(snapshot *AttemptSnapshot) error {
		observed = snapshot.Events[0].Action
		return nil
	}})
	snapshot := &AttemptSnapshot{Events: []RecordedPhase{{Action: "canonical"}}}
	if err := registry.Publish(context.Background(), snapshot); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if observed != "canonical" || snapshot.Events[0].Action != "canonical" {
		t.Fatalf("snapshot mutation escaped sink boundary: observed=%q original=%q", observed, snapshot.Events[0].Action)
	}
}

func TestPrometheusSink_PublishesPerAttemptCacheDeltas(t *testing.T) {
	metrics := GetPrometheusMetrics()
	beforeHits := metrics.CacheRequestCount("hit")
	beforeMisses := metrics.CacheRequestCount("miss")
	beforeEvict := metrics.CacheEvictionCount("pressure")

	sink := &PrometheusSink{Metrics: metrics}
	snapshot := &AttemptSnapshot{
		Resources: RawExecutionMetrics{AssetCacheHitCount: 3, AssetCacheMissCount: 2},
		Cache:     CacheFacts{Hits: 3, Misses: 2, Evictions: 1, BytesUsed: 500, Entries: 4},
	}
	if err := sink.Publish(context.Background(), snapshot); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if got := metrics.CacheRequestCount("hit") - beforeHits; got != 3 {
		t.Fatalf("hit delta=%v, want 3", got)
	}
	if got := metrics.CacheRequestCount("miss") - beforeMisses; got != 2 {
		t.Fatalf("miss delta=%v, want 2", got)
	}
	if got := metrics.CacheEvictionCount("pressure") - beforeEvict; got != 1 {
		t.Fatalf("eviction delta=%v, want 1", got)
	}
	// Point-in-time gauges must reach the export surface.
	exported := metrics.ExportPrometheus()
	if !strings.Contains(exported, `velox_cache_size_bytes{label="total"} 500`) {
		t.Fatalf("cache size gauge missing:\n%s", exported)
	}
	if !strings.Contains(exported, `velox_cache_entries{label="total"} 4`) {
		t.Fatalf("cache entries gauge missing:\n%s", exported)
	}
}

func TestPrometheusSink_ProjectsTypedAttemptMetrics(t *testing.T) {
	metrics := NewPrometheusMetrics()
	sink := &PrometheusSink{Metrics: metrics}
	snapshot := &AttemptSnapshot{
		Resources: RawExecutionMetrics{
			CpuTimeMs:        1200,
			PeakRssBytes:     8192,
			DiskReadBytes:    1000,
			DiskWriteBytes:   400,
			NetworkRxBytes:   300,
			NetworkTxBytes:   200,
			FramesDecoded:    90,
			FramesComposited: 80,
			FramesEncoded:    70,
		},
		Media: MediaFacts{FramesIn: 60, FramesOut: 50},
		Process: ProcessFacts{
			EngineSpawnCount:         1,
			EngineExternalSpawnCount: 2,
			EngineFfmpegSpawnCount:   1,
			EngineFfprobeSpawnCount:  1,
			EngineShellSpawnCount:    0,
			EngineCurlSpawnCount:     1,
		},
	}
	if err := sink.Publish(context.Background(), snapshot); err != nil {
		t.Fatalf("publish: %v", err)
	}
	exported := metrics.ExportPrometheus()
	for _, series := range []string{
		`velox_attempt_cpu_time_ms_total{label="total"} 1200`,
		`velox_attempt_peak_rss_bytes{label="total"} 8192`,
		`velox_attempt_io_bytes_total{direction="disk_read"} 1000`,
		`velox_attempt_io_bytes_total{direction="disk_write"} 400`,
		`velox_attempt_io_bytes_total{direction="network_rx"} 300`,
		`velox_attempt_io_bytes_total{direction="network_tx"} 200`,
		`velox_attempt_frames_total{kind="decoded"} 90`,
		`velox_attempt_frames_total{kind="composited"} 80`,
		`velox_attempt_frames_total{kind="encoded"} 70`,
		`velox_attempt_frames_total{kind="media_in"} 60`,
		`velox_attempt_frames_total{kind="media_out"} 50`,
		`velox_attempt_processes_total{kind="engine_spawn"} 1`,
		`velox_attempt_processes_total{kind="engine_external_spawn"} 2`,
		`velox_attempt_processes_total{kind="ffmpeg_spawn"} 1`,
		`velox_attempt_processes_total{kind="ffprobe_spawn"} 1`,
		`velox_attempt_processes_total{kind="curl_spawn"} 1`,
	} {
		if !strings.Contains(exported, series) {
			t.Fatalf("typed attempt series missing %q:\n%s", series, exported)
		}
	}
	for _, forbidden := range []string{"attempt-", "job-", "asset-", "pid="} {
		if strings.Contains(exported, forbidden) {
			t.Fatalf("typed attempt metrics contain high-cardinality value %q:\n%s", forbidden, exported)
		}
	}
}

func TestPrometheusSink_ProjectsLegacyCacheEventsFromSnapshotJournal(t *testing.T) {
	metrics := NewPrometheusMetrics()
	sink := &PrometheusSink{Metrics: metrics}
	snapshot := &AttemptSnapshot{Events: []RecordedPhase{
		{Component: "worker.cache", Action: "hash_verify", DurationMS: 17, Status: StatusOK},
		{Component: "worker.cache", Action: "eviction", MetadataJSON: `{"reason":"invalid"}`, Status: StatusOK},
		{Component: "worker.asset", Action: "transfer", DurationMS: 23, BytesIn: 42, Status: StatusOK},
	}}
	if err := sink.Publish(context.Background(), snapshot); err != nil {
		t.Fatalf("publish: %v", err)
	}
	exported := metrics.ExportPrometheus()
	if !strings.Contains(exported, `velox_cache_sha_verify_duration_seconds_count{label="asset"} 1`) {
		t.Fatalf("hash verification observation missing from snapshot projection:\n%s", exported)
	}
	if !strings.Contains(exported, `velox_cache_evictions_total{reason="invalid"} 1`) {
		t.Fatalf("invalid eviction missing from snapshot projection:\n%s", exported)
	}
	if !strings.Contains(exported, `velox_cache_downloads_total{label="asset"} 1`) || !strings.Contains(exported, `velox_cache_download_bytes_total{label="asset"} 42`) {
		t.Fatalf("download observation missing from snapshot projection:\n%s", exported)
	}
}

func TestPrometheusSink_ProjectsRenderFromSnapshotJournal(t *testing.T) {
	metrics := NewPrometheusMetrics()
	sink := &PrometheusSink{Metrics: metrics}
	snapshot := &AttemptSnapshot{Events: []RecordedPhase{{Component: "runner", Action: "execute", DurationMS: 123}}}
	if err := sink.Publish(context.Background(), snapshot); err != nil {
		t.Fatalf("publish: %v", err)
	}
	exported := metrics.ExportPrometheus()
	if !strings.Contains(exported, `velox_render_seconds_count{label="total"} 1`) {
		t.Fatalf("render observation missing from snapshot projection:\n%s", exported)
	}
	if !strings.Contains(exported, `velox_render_seconds_sum{label="total"} 0.123`) {
		t.Fatalf("render duration missing from snapshot projection:\n%s", exported)
	}
}

func TestReceiptSink_WritesAssembledJSON(t *testing.T) {
	dir := t.TempDir()
	sink := &ReceiptSink{
		Build: func(snapshot *AttemptSnapshot) ([]byte, error) {
			return []byte(`{"version":1,"attempt":"` + snapshot.Identity.AttemptID + `"}`), nil
		},
		OutDir: dir,
	}
	if err := sink.Publish(context.Background(), &AttemptSnapshot{Identity: AttemptIdentity{AttemptID: "a1"}}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "performance_receipt.json"))
	if err != nil {
		t.Fatalf("read receipt: %v", err)
	}
	if !strings.Contains(string(data), `"version":1`) {
		t.Fatalf("receipt content = %s", data)
	}
}

func TestBenchmarkSink_WritesRawFacts(t *testing.T) {
	dir := t.TempDir()
	sink := &BenchmarkSink{OutDir: dir}
	snapshot := &AttemptSnapshot{
		Identity: AttemptIdentity{AttemptID: "b1"},
		WallMs:   1234,
		Cache:    CacheFacts{Hits: 5},
	}
	if err := sink.Publish(context.Background(), snapshot); err != nil {
		t.Fatalf("publish: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "benchmark_b1.json"))
	if err != nil {
		t.Fatalf("read benchmark: %v", err)
	}
	if !strings.Contains(string(data), `"wall_ms": 1234`) {
		t.Fatalf("benchmark content = %s", data)
	}
}

func TestDiagnosticJSONSink_DumpsFullSnapshot(t *testing.T) {
	dir := t.TempDir()
	sink := &DiagnosticJSONSink{OutDir: dir}
	snapshot := &AttemptSnapshot{
		Identity: AttemptIdentity{AttemptID: "d1"},
		Events:   []RecordedPhase{{Component: "worker.engine", Action: "spawn"}},
	}
	if err := sink.Publish(context.Background(), snapshot); err != nil {
		t.Fatalf("publish: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "diagnostic_d1.json"))
	if err != nil {
		t.Fatalf("read diagnostic: %v", err)
	}
	if !strings.Contains(string(data), `"worker.engine"`) {
		t.Fatalf("diagnostic content = %s", data)
	}
}

func TestReceiptSink_NoOutDirIsNoop(t *testing.T) {
	sink := &ReceiptSink{Build: func(*AttemptSnapshot) ([]byte, error) { return []byte("x"), nil }}
	if err := sink.Publish(context.Background(), &AttemptSnapshot{}); err != nil {
		t.Fatalf("publish without out dir must be a noop, got %v", err)
	}
}

// captureSink records the snapshot it received; used by registry and
// pipeline tests.
type captureSink struct {
	name      string
	snapshot  *AttemptSnapshot
	fn        func(*AttemptSnapshot) error
	published bool
}

func (c *captureSink) Name() string {
	if c.name != "" {
		return c.name
	}
	return "capture"
}

func (c *captureSink) Publish(_ context.Context, snapshot *AttemptSnapshot) error {
	c.published = true
	c.snapshot = snapshot
	if c.fn != nil {
		return c.fn(snapshot)
	}
	return nil
}
