package telemetry

import (
	"strings"
	"testing"
	"time"
)

func TestPrometheusCacheMetricsExportWithLowCardinalityLabels(t *testing.T) {
	metrics := NewPrometheusMetrics()
	metrics.RecordAssetCacheHit("asset")
	metrics.RecordAssetCacheMiss("asset")
	metrics.RecordCacheRequest("hit")
	metrics.RecordCacheRequest("miss")
	metrics.RecordCacheRequest("job-should-normalize")
	metrics.RecordCacheDownload(128, 250*time.Millisecond)
	metrics.RecordCacheVerify(4 * time.Millisecond)
	metrics.RecordCacheCleanup(12 * time.Millisecond)
	metrics.RecordCacheEvictions("ttl", 2)
	metrics.RecordCacheEviction("worker-specific-reason")
	metrics.RecordCacheCleanupSkips("protected", 3)
	metrics.SetCacheSize(7, 4096)

	export := metrics.ExportPrometheus()
	for _, name := range []string{
		"velox_cache_requests_total",
		"velox_cache_downloads_total",
		"velox_cache_download_bytes_total",
		"velox_cache_download_duration_seconds",
		"velox_cache_sha_verify_duration_seconds",
		"velox_cache_cleanup_duration_seconds",
		"velox_cache_evictions_total",
		"velox_cache_cleanup_skipped_total",
		"velox_cache_size_bytes",
		"velox_cache_entries",
	} {
		if !strings.Contains(export, name) {
			t.Errorf("Prometheus export missing %s:\n%s", name, export)
		}
	}
	for _, forbidden := range []string{"asset-", "job-", "worker-", "sha256:"} {
		if strings.Contains(export, forbidden) {
			t.Errorf("Prometheus export contains high-cardinality value %q:\n%s", forbidden, export)
		}
	}
	for _, want := range []string{
		`velox_cache_requests_total{result="other"}`,
		`velox_cache_evictions_total{reason="other"}`,

		`velox_cache_requests_total{result="hit"}`,
		`velox_cache_requests_total{result="miss"}`,
		`velox_cache_evictions_total{reason="ttl"}`,
		`velox_cache_cleanup_skipped_total{reason="protected"}`,
	} {
		if !strings.Contains(export, want) {
			t.Errorf("Prometheus export missing canonical label %q:\n%s", want, export)
		}
	}
	if !strings.Contains(export, `velox_cache_entries{label="total"} 7`) {
		t.Errorf("entry gauge missing expected value:\n%s", export)
	}
	if !strings.Contains(export, `velox_cache_size_bytes{label="total"} 4096`) {
		t.Errorf("size gauge missing expected value:\n%s", export)
	}
}

func TestPrometheusPrefetchTimingMetricsUseOnlyDistanceLabels(t *testing.T) {
	metrics := NewPrometheusMetrics()
	metrics.RecordPrefetchQueueWait(1, 250*time.Millisecond)
	metrics.RecordPrefetchResolve(2, 1500*time.Millisecond)
	metrics.RecordPrefetchReadyLead(1, -2*time.Second)
	metrics.SetPrefetchOperational(2, 7)

	export := metrics.ExportPrometheus()
	for _, name := range []string{
		"velox_prefetch_queue_wait_seconds",
		"velox_prefetch_resolve_seconds",
		"velox_prefetch_ready_lead_seconds",
		"velox_prefetch_active",
		"velox_prefetch_queue_depth",
	} {
		if !strings.Contains(export, name) {
			t.Errorf("Prometheus export missing %s:\n%s", name, export)
		}
	}
	for _, label := range []string{`label="d1"`, `label="d2"`, `label="total"`} {
		if !strings.Contains(export, label) {
			t.Errorf("Prometheus export missing low-cardinality label %s:\n%s", label, export)
		}
	}
	for _, forbidden := range []string{"job-", "asset-", "sha256:"} {
		if strings.Contains(export, forbidden) {
			t.Errorf("Prometheus export contains forbidden high-cardinality value %q:\n%s", forbidden, export)
		}
	}
	if !strings.Contains(export, `velox_prefetch_active{label="total"} 2`) || !strings.Contains(export, `velox_prefetch_queue_depth{label="total"} 7`) {
		t.Fatalf("prefetch operational gauges missing expected values:\n%s", export)
	}
}
