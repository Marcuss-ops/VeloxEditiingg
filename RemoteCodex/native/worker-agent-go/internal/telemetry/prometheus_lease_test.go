package telemetry

import (
	"strings"
	"testing"
)

func TestPrometheusLeaseMetricsExportLowCardinalityLifecycle(t *testing.T) {
	metrics := NewPrometheusMetrics()
	fresh := metrics.ExportPrometheus()
	for _, series := range []string{
		`velox_cache_lease_acquires_total{result="success"} 0`,
		`velox_cache_lease_acquires_total{result="failure"} 0`,
		`velox_cache_lease_releases_total{result="success"} 0`,
		`velox_cache_lease_releases_total{result="failure"} 0`,
		`velox_cache_lease_releases_total{result="not_found"} 0`,
		`velox_cache_lease_renewals_total{result="success"} 0`,
		`velox_cache_lease_renewals_total{result="failure"} 0`,
		`velox_cache_lease_renewals_total{result="not_found"} 0`,
		`velox_cache_lease_retries_total{source="release_all"} 0`,
		`velox_cache_lease_retries_total{source="reconciler"} 0`,
		`velox_cache_lease_cleanup_failures_total{stage="release"} 0`,
		`velox_cache_lease_cleanup_failures_total{stage="reconcile_list"} 0`,
	} {
		if !strings.Contains(fresh, series) {
			t.Errorf("fresh export missing lease series %q:\n%s", series, fresh)
		}
	}

	metrics.RecordLeaseAcquire("success")
	metrics.RecordLeaseAcquire("database_locked")
	metrics.RecordLeaseRelease("success")
	metrics.RecordLeaseRelease("failure")
	metrics.RecordLeaseRelease("not_found")
	metrics.RecordLeaseRenew("success")
	metrics.RecordLeaseRenew("failure")
	metrics.RecordLeaseRenew("not_found")
	metrics.RecordLeaseRetry("release_all")
	metrics.RecordLeaseRetry("reconciler")
	metrics.RecordLeaseRetry("unexpected-source")
	metrics.RecordLeaseCleanupFailure("release")
	metrics.RecordLeaseCleanupFailure("enqueue")
	metrics.RecordLeaseCleanupFailure("reconcile_release")
	metrics.RecordLeaseCleanupFailure("reconcile_retry_persist")
	metrics.RecordLeaseCleanupFailure("reconcile_delete")
	metrics.RecordLeaseCleanupFailure("reconcile_list")
	metrics.RecordLeaseCleanupFailure("unexpected-stage")

	export := metrics.ExportPrometheus()
	for _, series := range []string{
		`velox_cache_lease_acquires_total{result="success"} 1`,
		`velox_cache_lease_acquires_total{result="failure"} 1`,
		`velox_cache_lease_releases_total{result="success"} 1`,
		`velox_cache_lease_releases_total{result="failure"} 1`,
		`velox_cache_lease_releases_total{result="not_found"} 1`,
		`velox_cache_lease_renewals_total{result="success"} 1`,
		`velox_cache_lease_renewals_total{result="failure"} 1`,
		`velox_cache_lease_renewals_total{result="not_found"} 1`,
		`velox_cache_lease_retries_total{source="release_all"} 1`,
		`velox_cache_lease_retries_total{source="reconciler"} 1`,
		`velox_cache_lease_retries_total{source="other"} 1`,
		`velox_cache_lease_cleanup_failures_total{stage="release"} 1`,
		`velox_cache_lease_cleanup_failures_total{stage="enqueue"} 1`,
		`velox_cache_lease_cleanup_failures_total{stage="reconcile_release"} 1`,
		`velox_cache_lease_cleanup_failures_total{stage="reconcile_retry_persist"} 1`,
		`velox_cache_lease_cleanup_failures_total{stage="reconcile_delete"} 1`,
		`velox_cache_lease_cleanup_failures_total{stage="reconcile_list"} 1`,
		`velox_cache_lease_cleanup_failures_total{stage="other"} 1`,
	} {
		if !strings.Contains(export, series) {
			t.Errorf("export missing lease series %q:\n%s", series, export)
		}
	}
	for _, forbidden := range []string{"asset-123", "job-456", "database_locked"} {
		if strings.Contains(export, forbidden) {
			t.Errorf("lease metrics exposed unbounded value %q:\n%s", forbidden, export)
		}
	}
}
