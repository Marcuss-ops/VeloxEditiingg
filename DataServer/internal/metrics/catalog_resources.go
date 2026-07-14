// Package metrics / catalog_resources.go
//
// Resources family — system-level resource usage and heartbeat
// metrics. Three logical layers, all describing the same kind of
// observability surface (CPU / memory / disk / network / load):
//   - resource.* : per-attempt resource snapshot reported with the
//     attempt outcome.
//   - worker.*    : worker heartbeat (CPU/RSS/disk/load/network/active
//     tasks/slots/heartbeat-age).
//   - master.*    : master process health (RSS/goroutines/outbox
//     pending).
package metrics

// resourcesMetricDefinitions returns resource.* + worker.* + master.*
// definitions in stable order: per-attempt resource first, then worker
// heartbeat, then master health.
func resourcesMetricDefinitions() []MetricDefinition {
	return []MetricDefinition{
		// ── Per-attempt resource snapshot ────────────────────────────────
		{
			Name: "resource.cpu_percent_peak", Unit: "percent", Component: CompResource, Kind: KindGauge,
			Description: "Peak CPU utilization during the attempt (0-100)",
		},
		{
			Name: "resource.rss_peak_bytes", Unit: "bytes", Component: CompResource, Kind: KindGauge,
			Description: "Peak RSS memory usage during the attempt",
		},
		{
			Name: "resource.disk_read_bytes", Unit: "bytes", Component: CompResource, Kind: KindCounter,
			Description: "Total disk bytes read during the attempt",
		},
		{
			Name: "resource.disk_write_bytes", Unit: "bytes", Component: CompResource, Kind: KindCounter,
			Description: "Total disk bytes written during the attempt",
		},
		{
			Name: "resource.network_rx_bytes", Unit: "bytes", Component: CompResource, Kind: KindCounter,
			Description: "Total network bytes received during the attempt",
		},
		{
			Name: "resource.network_tx_bytes", Unit: "bytes", Component: CompResource, Kind: KindCounter,
			Description: "Total network bytes transmitted during the attempt",
		},
		{
			Name: "resource.iowait_ms", Unit: "ms", Component: CompResource, Kind: KindCounter,
			Description: "Total IO wait time during the attempt",
		},
		{
			Name: "resource.open_fds_peak", Unit: "count", Component: CompResource, Kind: KindGauge,
			Description: "Peak number of open file descriptors during the attempt",
		},
		// ── Worker resource gauges (heartbeat) ───────────────────────────
		{
			Name: "worker.cpu_utilization_ratio", Unit: "ratio", Component: CompWorker, Kind: KindGauge,
			Description: "Worker CPU utilization ratio (0-1)",
		},
		{
			Name: "worker.cpu_iowait_ratio", Unit: "ratio", Component: CompWorker, Kind: KindGauge,
			Description: "Worker CPU iowait ratio (0-1)",
		},
		{
			Name: "worker.cpu_steal_ratio", Unit: "ratio", Component: CompWorker, Kind: KindGauge,
			Description: "Worker CPU steal time ratio (virtualized env)",
		},
		{
			Name: "worker.load1", Unit: "ratio", Component: CompWorker, Kind: KindGauge,
			Description: "Worker 1-minute load average",
		},
		{
			Name: "worker.run_queue", Unit: "count", Component: CompWorker, Kind: KindGauge,
			Description: "Worker OS run queue depth",
		},
		{
			Name: "worker.process_rss_bytes", Unit: "bytes", Component: CompWorker, Kind: KindGauge,
			Description: "Worker agent process resident set size",
		},
		{
			Name: "worker.process_rss_peak_bytes", Unit: "bytes", Component: CompWorker, Kind: KindGauge,
			Description: "Worker agent process peak RSS",
		},
		{
			Name: "worker.memory_used_bytes", Unit: "bytes", Component: CompWorker, Kind: KindGauge,
			Description: "Worker system memory used",
		},
		{
			Name: "worker.disk_free_bytes", Unit: "bytes", Component: CompWorker, Kind: KindGauge,
			Description: "Worker free disk space on the working volume",
		},
		{
			Name: "worker.temp_bytes", Unit: "bytes", Component: CompWorker, Kind: KindGauge,
			Description: "Worker temp space used at heartbeat time",
		},
		{
			Name: "worker.active_tasks", Unit: "count", Component: CompWorker, Kind: KindGauge,
			Description: "Number of currently active tasks on the worker",
		},
		{
			Name: "worker.task_slots", Unit: "count", Component: CompWorker, Kind: KindGauge,
			Description: "Total task slots available on the worker",
		},
		{
			Name: "worker.network_receive_bytes", Unit: "bytes", Component: CompWorker, Kind: KindCounter,
			Description: "Worker total network bytes received (cumulative delta per heartbeat)",
		},
		{
			Name: "worker.network_transmit_bytes", Unit: "bytes", Component: CompWorker, Kind: KindCounter,
			Description: "Worker total network bytes transmitted (cumulative delta per heartbeat)",
		},
		{
			Name: "worker.heartbeat_age_seconds", Unit: "seconds", Component: CompWorker, Kind: KindGauge,
			Description: "Seconds since last worker heartbeat",
		},
		// ── Master-side health ───────────────────────────────────────────
		{
			Name: "master.memory_rss_bytes", Unit: "bytes", Component: CompMaster, Kind: KindGauge,
			Description: "Master process RSS memory",
		},
		{
			Name: "master.goroutines", Unit: "count", Component: CompMaster, Kind: KindGauge,
			Description: "Number of active goroutines on the master",
		},
		{
			Name: "master.outbox_pending", Unit: "count", Component: CompMaster, Kind: KindGauge,
			Description: "Number of pending outbox events",
		},
	}
}
