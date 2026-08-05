package telemetry

import (
	"strings"
	"testing"
	"time"
)

func TestPrometheusWorkerMetricsExportRequiredSurfaceAndNoAssetIDs(t *testing.T) {
	metrics := NewPrometheusMetrics()
	metrics.RecordAssetCacheHit("asset-id-must-not-be-a-label")
	metrics.RecordAssetCacheMiss("another-asset-id-must-not-be-a-label")
	metrics.RecordCacheDownload(2048, 250*time.Millisecond)
	metrics.RecordRender(3 * time.Second)
	metrics.RecordArtifactUpload(120 * time.Millisecond)
	metrics.RecordTaskResultSubmit(40 * time.Millisecond)
	metrics.RecordTaskResultAck(60 * time.Millisecond)
	metrics.RecordCacheCleanup(15 * time.Millisecond)
	metrics.RecordCacheCleanupSkips("protected", 2)
	metrics.RecordCacheEvictions("ttl", 1)
	metrics.SetWorkerActiveJobs("worker-secret-id", 3)
	metrics.SetWorkerStatus("worker-secret-id", 2)
	metrics.SetAssetDownloadOperational(2, 3, 4, 1, 2, 4096, 8192, 1024, 7, 9)

	export := metrics.ExportPrometheus()
	for _, name := range []string{
		"velox_asset_cache_hits_total",
		"velox_asset_cache_misses_total",
		"velox_asset_download_seconds",
		"velox_render_seconds",
		"velox_artifact_upload_seconds",
		"velox_task_result_submit_seconds",
		"velox_task_result_ack_seconds",
		"velox_telemetry_invalid_events_total",
		"velox_cache_cleanup_duration_seconds",
		"velox_cache_cleanup_skipped_total",
		"velox_cache_evictions_total",
		"velox_cache_protected_skips_total",
		"velox_worker_active_jobs",
		"velox_worker_status",
		"velox_cache_duplicate_downloads_total",
		"velox_cache_duplicate_download_bytes_total",
		"velox_worker_errors_total",
		"velox_asset_download_transfers_active",
		"velox_asset_download_transfers_queued",
		"velox_asset_download_transfers_ready",
		"velox_asset_download_transfers_failed",
		"velox_asset_download_cache_hits",
		"velox_asset_download_bytes_downloaded",
		"velox_asset_download_bytes_total",
		"velox_asset_download_throughput_bytes_per_second",
		"velox_asset_download_eta_seconds",
		"velox_asset_download_coalesced_requests_total",
	} {
		if !strings.Contains(export, name) {
			t.Errorf("Prometheus export missing %s:\n%s", name, export)
		}
	}
	for _, forbidden := range []string{
		"asset-id-must-not-be-a-label",
		"another-asset-id-must-not-be-a-label",
		"worker-secret-id",
	} {
		if strings.Contains(export, forbidden) {
			t.Errorf("Prometheus export contains forbidden high-cardinality value %q:\n%s", forbidden, export)
		}
	}
	if !strings.Contains(export, `velox_worker_active_jobs{label="total"} 3`) {
		t.Errorf("active-job gauge must use the static total label:\n%s", export)
	}
	if !strings.Contains(export, `velox_task_result_submit_seconds_count{label="total"}`) {
		t.Errorf("TaskResult submit histogram must use the static total label:\n%s", export)
	}
	for _, series := range []string{
		`velox_asset_download_transfers_active{label="total"} 2`,
		`velox_asset_download_bytes_downloaded{label="total"} 4096`,
		`velox_asset_download_throughput_bytes_per_second{label="total"} 1024`,
		`velox_asset_download_eta_seconds{label="total"} 7`,
		`velox_asset_download_coalesced_requests_total{label="total"} 9`,
	} {
		if !strings.Contains(export, series) {
			t.Errorf("asset operational series missing %q:\n%s", series, export)
		}
	}
}

// TestPrometheusWorker_ParallelismCertificationCounters verifies the
// counters the parallelism certification harness consumes: pre-seeded
// zero series (so a before-scrape never looks missing), the
// singleflight duplicate-download counters, and the worker error
// counter. All stay on low-cardinality static labels.
func TestPrometheusWorker_AssetDownloadOperationalSetIsMonotonicAndLowCardinality(t *testing.T) {
	metrics := NewPrometheusMetrics()
	metrics.SetAssetDownloadOperational(2, 1, 3, 0, 2, 100, 200, 50, 4, 8)
	metrics.SetAssetDownloadOperational(0, 0, 3, 1, 2, 200, 300, 0, 0, 4)
	export := metrics.ExportPrometheus()
	for _, series := range []string{
		`velox_asset_download_transfers_active{label="total"} 0`,
		`velox_asset_download_transfers_failed{label="total"} 1`,
		`velox_asset_download_bytes_downloaded{label="total"} 200`,
		`velox_asset_download_coalesced_requests_total{label="total"} 8`,
	} {
		if !strings.Contains(export, series) {
			t.Errorf("operational series missing %q:\n%s", series, export)
		}
	}
	if strings.Contains(export, "asset-id") || strings.Contains(export, "job-") {
		t.Fatalf("operational metrics must not expose asset/job identifiers:\n%s", export)
	}
}

func TestPrometheusWorker_ParallelismCertificationCounters(t *testing.T) {
	metrics := NewPrometheusMetrics()

	// Pre-seeded series are exported even before any cache access,
	// so the certification harness's before/after delta is defined.
	fresh := metrics.ExportPrometheus()
	for _, series := range []string{
		`velox_cache_requests_total{result="hit"} 0`,
		`velox_cache_requests_total{result="miss"} 0`,
		`velox_cache_downloads_total{label="asset"} 0`,
		`velox_cache_duplicate_downloads_total{label="asset"} 0`,
		`velox_cache_duplicate_download_bytes_total{label="asset"} 0`,
		`velox_worker_errors_total{label="total"} 0`,
	} {
		if !strings.Contains(fresh, series) {
			t.Errorf("fresh export missing pre-seeded series %q:\n%s", series, fresh)
		}
	}

	// Recording semantics: count-only (bytes 0) still increments the
	// request counter; positive bytes accumulate the byte counter.
	metrics.RecordCacheDuplicateDownload(0)
	metrics.RecordCacheDuplicateDownload(4096)
	metrics.RecordWorkerError()
	if got := metrics.DuplicateDownloadCount(); got != 2 {
		t.Errorf("DuplicateDownloadCount = %v, want 2", got)
	}
	if got := metrics.DuplicateDownloadBytes(); got != 4096 {
		t.Errorf("DuplicateDownloadBytes = %v, want 4096", got)
	}
	if got := metrics.WorkerErrorCount(); got != 1 {
		t.Errorf("WorkerErrorCount = %v, want 1", got)
	}

	export := metrics.ExportPrometheus()
	if !strings.Contains(export, `velox_cache_duplicate_downloads_total{label="asset"} 2`) {
		t.Errorf("duplicate counter not exported with delta:\n%s", export)
	}
	if !strings.Contains(export, `velox_worker_errors_total{label="total"} 1`) {
		t.Errorf("worker error counter not exported with delta:\n%s", export)
	}
}
