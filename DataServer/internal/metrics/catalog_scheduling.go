// Package metrics / catalog_scheduling.go
//
// Scheduling family — metrics for the dispatch / lease / taskrunner
// path. Two layers:
//   - queue.* / lease_wait.* / time_to_first_worker.* : how long
//     a task waited before being assigned to a worker (queue + lease
//     acquire + first-worker latency).
//   - taskrunner.* : per-phase timing inside the worker-side
//     taskrunner (cache_lookup, prefetch, execute, upload, report).
package metrics

// schedulingMetricDefinitions returns queue.* + lease_wait.* +
// time_to_first_worker.* + taskrunner.* definitions. Queue/lease
// metrics first (dispatch side), then taskrunner phases (worker side).
func schedulingMetricDefinitions() []MetricDefinition {
	return []MetricDefinition{
		// ── Queue / wait-time metrics ────────────────────────────────────
		{
			Name: "queue.ms", Unit: "ms", Component: CompQueue, Kind: KindHistogram,
			Description: "Time the task spent in the queue before being dispatched to a worker",
		},
		{
			Name: "lease_wait.ms", Unit: "ms", Component: CompLease, Kind: KindHistogram,
			Description: "Time spent waiting for a lease to be granted",
		},
		{
			Name: "time_to_first_worker.ms", Unit: "ms", Component: CompQueue, Kind: KindHistogram,
			Description: "Time from job submission to first worker assignment",
		},
		// ── TaskRunner phases ────────────────────────────────────────────
		{
			Name: "taskrunner.cache_lookup_ms", Unit: "ms", Component: CompTaskRunner, Kind: KindHistogram,
			Description: "Time spent in the TaskRunner cache_lookup phase",
		},
		{
			Name: "taskrunner.prefetch_ms", Unit: "ms", Component: CompTaskRunner, Kind: KindHistogram,
			Description: "Time spent in the TaskRunner prefetch phase",
		},
		{
			Name: "taskrunner.execute_ms", Unit: "ms", Component: CompTaskRunner, Kind: KindHistogram,
			Description: "Time spent in the TaskRunner execute phase (pipeline + engine)",
		},
		{
			Name: "taskrunner.upload_ms", Unit: "ms", Component: CompTaskRunner, Kind: KindHistogram,
			Description: "Time spent in the TaskRunner upload phase",
		},
		{
			Name: "taskrunner.report_ms", Unit: "ms", Component: CompTaskRunner, Kind: KindHistogram,
			Description: "Time spent in the TaskRunner report phase",
		},
	}
}
