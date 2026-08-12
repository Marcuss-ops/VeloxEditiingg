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
