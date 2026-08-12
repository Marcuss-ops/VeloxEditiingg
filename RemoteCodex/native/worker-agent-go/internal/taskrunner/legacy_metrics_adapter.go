package taskrunner

import (
	"encoding/json"

	"velox-worker-agent/internal/telemetry"
)

// LegacyMetricsAdapter is the single compatibility boundary for the historical
// dotted-key metric map. New producers write RawExecutionMetrics directly;
// this adapter is used only for unmigrated producers and old report consumers.
type LegacyMetricsAdapter struct{}

// ProjectAttempt projects attempt-owned raw facts into a fresh legacy map.
// It never mutates or aliases the typed source and never computes a KPI.
func (LegacyMetricsAdapter) ProjectAttempt(result telemetry.AttemptTelemetry) map[string]interface{} {
	t := result.Metrics
	legacy := map[string]interface{}{
		"cpu.ms":                   t.CpuTimeMs,
		"rss.peak.bytes":           t.PeakRssBytes,
		"cpu.percent.peak":         t.CpuPercentPeak,
		"disk.read.bytes":          t.DiskReadBytes,
		"disk.write.bytes":         t.DiskWriteBytes,
		"network.rx.bytes":         t.NetworkRxBytes,
		"network.tx.bytes":         t.NetworkTxBytes,
		"temp.bytes.written":       t.TempBytesWritten,
		"iowait.ms":                t.IowaitMs,
		"open.fds.peak":            t.OpenFdsPeak,
		"wall.clock.seconds":       t.WallClockSeconds,
		"telemetry.schema.version": telemetry.AttemptTelemetrySchemaVersion,
		"telemetry.complete":       result.Complete,
		"telemetry.cpu.source":     t.TelemetryCPUSource,
	}
	coverage, _ := json.Marshal(result.Coverage)
	legacy["telemetry.coverage.json"] = string(coverage)
	return legacy
}

// FromMap adapts an unmigrated executor's legacy map into canonical raw typed
// metrics. The existing merge logic remains here as the one reverse direction
// of this compatibility adapter.
func (LegacyMetricsAdapter) FromMap(m map[string]interface{}) *telemetry.RawExecutionMetrics {
	copyMap := make(map[string]interface{}, len(m))
	for key, value := range m {
		copyMap[key] = value
	}
	report := &TaskExecutionReport{}
	(&TaskRunner{}).mergeStatsInto(report, copyMap)
	return report.RawMetrics
}
