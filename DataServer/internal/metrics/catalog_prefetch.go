package metrics

func prefetchMetricDefinitions() []MetricDefinition {
	return []MetricDefinition{
		{Name: "prefetch.jobs_total", Unit: "count", Component: CompWorker, Kind: KindCounter, Description: "Prefetch lifecycle milestones by worker and outcome"},
		{Name: "prefetch.bytes_total", Unit: "bytes", Component: CompWorker, Kind: KindCounter, Description: "Asset bytes observed by prefetch origin and worker"},
		{Name: "prefetch.failures_total", Unit: "count", Component: CompWorker, Kind: KindCounter, Description: "Prefetch failures by worker and closed reason"},
		{Name: "prefetch.duration_seconds", Unit: "seconds", Component: CompWorker, Kind: KindHistogram, Description: "Asset prefetch duration from download start to ready"},
		{Name: "prefetch.cache_events_total", Unit: "count", Component: CompWorker, Kind: KindCounter, Description: "Prefetch asset hit/miss events by worker and origin"},
	}
}
