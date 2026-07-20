// Package metrics / catalog_parallelism.go
//
// Parallelism family — metrics derived from per-segment timing offsets
// to measure actual concurrency, speedup, and efficiency of the render
// pipeline. Computed by the master during IngestTaskResultAtomic from
// the raw segment timing rows.
package metrics

// parallelismMetricDefinitions returns taskrunner.* parallelism metrics.
func parallelismMetricDefinitions() []MetricDefinition {
	return []MetricDefinition{
		{
			Name: "taskrunner.serial_work_ms", Unit: "ms", Component: CompTaskRunner, Kind: KindHistogram,
			Description: "Sum of all segment durations (serial work baseline)",
		},
		{
			Name: "taskrunner.render_window_ms", Unit: "ms", Component: CompTaskRunner, Kind: KindHistogram,
			Description: "Wall-clock span from first segment start to last segment end",
		},
		{
			Name: "taskrunner.union_busy_ms", Unit: "ms", Component: CompTaskRunner, Kind: KindHistogram,
			Description: "Total wall-clock time during which at least one segment was active",
		},
		{
			Name: "taskrunner.overlap_ms", Unit: "ms", Component: CompTaskRunner, Kind: KindHistogram,
			Description: "Wall-clock time during which >1 segment was active simultaneously",
		},
		{
			Name: "taskrunner.idle_gap_ms", Unit: "ms", Component: CompTaskRunner, Kind: KindHistogram,
			Description: "Time within render window where no segment was active (gaps)",
		},
		{
			Name: "taskrunner.parallel_peak", Unit: "count", Component: CompTaskRunner, Kind: KindHistogram,
			Description: "Maximum number of segments active simultaneously",
		},
		{
			Name: "taskrunner.parallel_average", Unit: "ratio", Component: CompTaskRunner, Kind: KindHistogram,
			Description: "Average concurrency (serial_work / union_busy)",
		},
		{
			Name: "taskrunner.parallel_efficiency_ratio", Unit: "ratio", Component: CompTaskRunner, Kind: KindHistogram,
			Description: "Parallelism efficiency (average_concurrency / peak_concurrency)",
		},
		{
			Name: "taskrunner.speedup_vs_serial", Unit: "ratio", Component: CompTaskRunner, Kind: KindHistogram,
			Description: "Speedup over serial execution (serial_work / render_window)",
		},
		{
			Name: "resource.cpu_oversubscription_ratio", Unit: "ratio", Component: CompResource, Kind: KindHistogram,
			Description: "CPU oversubscription (total_ffmpeg_threads / logical_cpu_count)",
		},
	}
}
