// Package metrics / catalog_parallelism.go
//
// Parallelism family — metrics derived from per-segment timing offsets
// to measure actual concurrency, speedup, and efficiency of the render
// pipeline. Computed by the master during IngestTaskResultAtomic from
// the raw segment timing rows.
//
// KIND: every entry here is a GAUGE, not a histogram. They used to be
// declared KindHistogram while collector_engine.go registered them with
// NewGaugeFamily (velox_taskrunner_serial_work_ms, …_parallel_peak,
// velox_resource_cpu_oversubscription_ratio) — a catalog/exposure
// disagreement that made the catalog claim a distribution the endpoint
// never served. The declaration now matches the wire: set-to-current
// value per attempt, read as a level (avg by worker_id (...)) rather
// than observed into buckets.
package metrics

// parallelismMetricDefinitions returns taskrunner.* parallelism metrics.
func parallelismMetricDefinitions() []MetricDefinition {
	return []MetricDefinition{
		{
			Name: "taskrunner.serial_work_ms", Unit: "ms", Component: CompTaskRunner, Kind: KindGauge,
			Description: "Sum of all segment durations (serial work baseline)",
		},
		{
			Name: "taskrunner.render_window_ms", Unit: "ms", Component: CompTaskRunner, Kind: KindGauge,
			Description: "Wall-clock span from first segment start to last segment end",
		},
		{
			Name: "taskrunner.union_busy_ms", Unit: "ms", Component: CompTaskRunner, Kind: KindGauge,
			Description: "Total wall-clock time during which at least one segment was active",
		},
		{
			Name: "taskrunner.overlap_ms", Unit: "ms", Component: CompTaskRunner, Kind: KindGauge,
			Description: "Wall-clock time during which >1 segment was active simultaneously",
		},
		{
			Name: "taskrunner.idle_gap_ms", Unit: "ms", Component: CompTaskRunner, Kind: KindGauge,
			Description: "Time within render window where no segment was active (gaps)",
		},
		{
			Name: "taskrunner.parallel_peak", Unit: "count", Component: CompTaskRunner, Kind: KindGauge,
			Description: "Maximum number of segments active simultaneously",
		},
		{
			Name: "taskrunner.parallel_average", Unit: "ratio", Component: CompTaskRunner, Kind: KindGauge,
			Description: "Average concurrency (serial_work / union_busy)",
		},
		{
			Name: "taskrunner.parallel_efficiency_ratio", Unit: "ratio", Component: CompTaskRunner, Kind: KindGauge,
			Description: "Parallelism efficiency (average_concurrency / peak_concurrency)",
		},
		{
			Name: "taskrunner.speedup_vs_serial", Unit: "ratio", Component: CompTaskRunner, Kind: KindGauge,
			Description: "Speedup over serial execution (serial_work / render_window)",
		},
		{
			Name: "resource.cpu_oversubscription_ratio", Unit: "ratio", Component: CompResource, Kind: KindGauge,
			Description: "CPU oversubscription (total_ffmpeg_threads / logical_cpu_count)",
		},
	}
}
