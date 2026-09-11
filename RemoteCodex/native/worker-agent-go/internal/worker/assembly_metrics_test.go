package worker

import (
	"strings"
	"testing"

	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/internal/telemetry"
)

func TestRecordExecutionDownloadMetricsUsesForegroundMissesOnly(t *testing.T) {
	metrics := telemetry.NewPrometheusMetrics()

	report := &taskrunner.TaskExecutionReport{AssetOperations: []AssetOperationRecord{
		{CacheStatus: "hit", DownloadMS: 900},
		{CacheStatus: "miss", DownloadMS: 120},
		{CacheStatus: "miss", DownloadMS: 80},
	}}
	recordExecutionDownloadMetricsWith(metrics, report)

	export := metrics.ExportPrometheus()
	if !strings.Contains(export, `velox_assembly_execution_download_ms_sum{label="total"} 200`) {
		t.Fatalf("foreground download histogram =\n%s", export)
	}
	if !strings.Contains(export, `velox_assembly_execution_download_ms_count{label="total"} 1`) {
		t.Fatalf("foreground download histogram count =\n%s", export)
	}
}
