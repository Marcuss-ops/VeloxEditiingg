// Package metrics / catalog_scheduling.go
//
// Scheduling family — metrics for the dispatch / lease / taskrunner
// path. Three layers, each with an EXPLICIT exposure statement so the
// catalog and the endpoint cannot drift apart again:
//
//  1. queue.* / lease.* — how long a task waited before a worker took
//     it. Declared as histograms AND really exposed as histograms
//     (recorded by RecordAttempt from task_attempt_metrics):
//
//     queue.wait_ms                → velox_queue_wait_ms
//     lease.wait_ms                → velox_lease_wait_ms
//     queue.time_to_first_worker_ms → velox_queue_time_to_first_worker_ms
//
//     They used to be declared KindHistogram while NO family existed
//     at all (DB/rollup only), and their keys (`queue.ms`,
//     `lease_wait.ms`, `time_to_first_worker.ms`) violated the
//     convention in catalog.go (unit in the suffix, after the metric
//     name) — a name like `queue.ms` has a component and a unit but no
//     metric. The keys are now convention-shaped; the persisted daily
//     rollup metric_name stays `queue_ms` for history continuity.
//
//  2. taskrunner.* — per-phase timing inside the worker-side
//     taskrunner. These remain histograms, but they are NOT exposed
//     under their own family names: the same facts ride the canonical
//     phase histograms (velox_task_phase_duration_seconds{phase} for
//     the durable task_phase_timings rows, velox_engine_phase_
//     duration_seconds{phase} for the detailed component.action rows).
//     Declaring a family that the endpoint never serves is exactly the
//     disagreement this header exists to prevent — read them there.
//
//  3. The parallelism gauges live in catalog_parallelism.go.
package metrics

// schedulingMetricDefinitions returns queue.* + lease.* + taskrunner.*
// definitions. Dispatch-side wait metrics first (now exposed), then
// the worker-side taskrunner phase concepts (exposed via the phase
// histograms — see the header).
func schedulingMetricDefinitions() []MetricDefinition {
	return []MetricDefinition{
		// ── Queue / wait-time metrics (Prometheus histograms) ─────────
		{
			Name: "queue.wait_ms", Unit: "ms", Component: CompQueue, Kind: KindHistogram,
			Description: "Time the task spent in the READY queue before being claimed by a worker (velox_queue_wait_ms)",
		},
		{
			Name: "lease.wait_ms", Unit: "ms", Component: CompLease, Kind: KindHistogram,
			Description: "Time between claim and worker acceptance (velox_lease_wait_ms)",
		},
		{
			Name: "queue.time_to_first_worker_ms", Unit: "ms", Component: CompQueue, Kind: KindHistogram,
			Description: "End-to-end scheduling latency, submit to first worker (velox_queue_time_to_first_worker_ms)",
		},
		// ── TaskRunner phases (concept-level; ride the phase histograms) ─
		{
			Name: "taskrunner.cache_lookup_ms", Unit: "ms", Component: CompTaskRunner, Kind: KindHistogram,
			Description: "Time spent in the TaskRunner cache_lookup phase (exposed via velox_task_phase_duration_seconds{phase=\"cache_lookup\"})",
		},
		{
			Name: "taskrunner.prefetch_ms", Unit: "ms", Component: CompTaskRunner, Kind: KindHistogram,
			Description: "Time spent in the TaskRunner prefetch phase (durable rows: task_phase_timings)",
		},
		{
			Name: "taskrunner.execute_ms", Unit: "ms", Component: CompTaskRunner, Kind: KindHistogram,
			Description: "Time spent in the TaskRunner execute phase (pipeline + engine) (exposed via velox_engine_phase_duration_seconds{phase})",
		},
		{
			Name: "taskrunner.upload_ms", Unit: "ms", Component: CompTaskRunner, Kind: KindHistogram,
			Description: "Time spent in the TaskRunner upload phase (exposed via velox_task_phase_duration_seconds{phase=\"upload\"})",
		},
		{
			Name: "taskrunner.report_ms", Unit: "ms", Component: CompTaskRunner, Kind: KindHistogram,
			Description: "Time spent in the TaskRunner report phase (durable rows: task_phase_timings)",
		},
	}
}
