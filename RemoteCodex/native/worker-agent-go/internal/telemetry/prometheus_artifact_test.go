package telemetry

import (
	"strings"
	"testing"
)

func TestPrometheusCachePressureEvictionMetrics(t *testing.T) {
	m := NewPrometheusMetrics()
	m.RecordCacheEvictions("pressure", 3)
	m.RecordCacheEvictedBytes(4096)
	m.SetCacheDiskUsagePercent(85)

	export := m.ExportPrometheus()
	for _, want := range []string{
		`velox_cache_evictions_total{reason="pressure"} 3`,
		`velox_cache_evicted_bytes_total{label="total"} 4096`,
		`velox_cache_disk_usage_percent{label="total"} 85`,
	} {
		if !strings.Contains(export, want) {
			t.Errorf("export missing %q:\n%s", want, export)
		}
	}
}

func TestPrometheusArtifactTmpfsMetrics(t *testing.T) {
	m := NewPrometheusMetrics()
	m.SetArtifactTmpfsReservedBytes(1024)
	m.RecordArtifactTmpfsSpill(512)

	export := m.ExportPrometheus()
	for _, want := range []string{
		`velox_artifact_tmpfs_reserved_bytes{label="total"} 1024`,
		`velox_artifact_tmpfs_spill_total{label="total"} 1`,
		`velox_artifact_tmpfs_spill_bytes_total{label="total"} 512`,
	} {
		if !strings.Contains(export, want) {
			t.Errorf("export missing %q:\n%s", want, export)
		}
	}
}

func TestPrometheusArtifactNvmeFallbackNormalizesReason(t *testing.T) {
	m := NewPrometheusMetrics()
	m.RecordArtifactNvmeFallback("no_space")
	m.RecordArtifactNvmeFallback("unknown_size")
	m.RecordArtifactNvmeFallback("something-unexpected")

	export := m.ExportPrometheus()
	for _, want := range []string{
		`velox_artifact_nvme_fallback_total{reason="no_space"} 1`,
		`velox_artifact_nvme_fallback_total{reason="unknown_size"} 1`,
		`velox_artifact_nvme_fallback_total{reason="other"} 1`,
	} {
		if !strings.Contains(export, want) {
			t.Errorf("export missing %q:\n%s", want, export)
		}
	}
}

func TestNormalizeFallbackReasonTable(t *testing.T) {
	cases := map[string]string{
		"tmpfs_disabled":     "tmpfs_disabled",
		"unknown_size":       "unknown_size",
		"no_space":           "no_space",
		"statfs_error":       "statfs_error",
		"reservation_failed": "reservation_failed",
		"":                   "other",
		"boom":               "other",
	}
	for in, want := range cases {
		if got := normalizeFallbackReason(in); got != want {
			t.Errorf("normalizeFallbackReason(%q) = %q, want %q", in, got, want)
		}
	}
}
