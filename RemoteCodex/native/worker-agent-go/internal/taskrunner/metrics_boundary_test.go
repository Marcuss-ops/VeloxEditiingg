package taskrunner

// TestNoDirectTaskExecutionReportMetricsAccess has been removed in Phase 2b.
// With mergeStatsInto now operating directly on RawMetrics, the report.Metrics
// map is the supported legacy display projection and direct access is correct.
// The methods LegacyMetrics/AdoptLegacyMetrics/HasLegacyMetrics/LegacyMetric/
// SetLegacyMetric have been removed from TaskExecutionReport.