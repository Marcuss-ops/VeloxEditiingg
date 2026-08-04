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

	export := metrics.ExportPrometheus()
	for _, name := range []string{
		"velox_asset_cache_hits_total",
		"velox_asset_cache_misses_total",
		"velox_asset_download_seconds",
		"velox_render_seconds",
		"velox_artifact_upload_seconds",
		"velox_task_result_submit_seconds",
		"velox_task_result_ack_seconds",
		"velox_cache_cleanup_duration_seconds",
		"velox_cache_cleanup_skipped_total",
		"velox_cache_evictions_total",
		"velox_cache_protected_skips_total",
		"velox_worker_active_jobs",
		"velox_worker_status",
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
}
