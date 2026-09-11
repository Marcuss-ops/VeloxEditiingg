package worker

import (
	"time"

	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/internal/telemetry"
)

// recordExecutionDownloadMetrics projects only foreground asset-transfer
// time from the canonical per-task asset operation records. Prefetch has its
// own event path, so this cannot double-count preparation latency.
func recordExecutionDownloadMetrics(report *taskrunner.TaskExecutionReport) {
	recordExecutionDownloadMetricsWith(telemetry.GetPrometheusMetrics(), report)
}

func recordExecutionDownloadMetricsWith(metrics *telemetry.PrometheusMetrics, report *taskrunner.TaskExecutionReport) {
	if metrics == nil || report == nil {
		return
	}
	var downloadMS int64
	for _, record := range report.AssetOperations {
		if record.DownloadMS > 0 && record.CacheStatus == "miss" {
			downloadMS += record.DownloadMS
		}
	}
	if downloadMS > 0 {
		metrics.RecordAssemblyExecutionDownload(time.Duration(downloadMS) * time.Millisecond)
	}
}
