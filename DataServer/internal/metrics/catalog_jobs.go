// Package metrics / catalog_jobs.go
//
// Master job family — canonical names for the job-queue depth/age and
// the end-to-end job latency surface (the two operator questions that
// used to need ad-hoc SQL: "are jobs arriving / how deep is the
// queue?" and "how long does submit → delivered take?").
//
// Catalog key → Prometheus family mapping is the regular one
// (`velox_` + key):
//
//	jobs.pending                    → velox_jobs_pending
//	jobs.running                    → velox_jobs_running
//	jobs.oldest_pending_age_seconds → velox_jobs_oldest_pending_age_seconds
//	job.succeeded_total             → velox_job_succeeded_total
//	job.failed_total                → velox_job_failed_total
//	job.e2e_duration_seconds        → velox_job_e2e_duration_seconds
//
// `velox_job_succeeded_total` is also asserted by the workload e2e
// harness (tests/e2e/workload-mtls/run.sh step 5), so the singular
// `job.*` keys are load-bearing, not stylistic.
package metrics

// jobsMetricDefinitions returns the job-queue + job-completion catalog
// entries. Queue depth/age first (the live projection), then the
// terminal-outcome counters and the phase-attributed e2e histogram.
func jobsMetricDefinitions() []MetricDefinition {
	return []MetricDefinition{
		// ── Live queue projection (supervisor tick) ────────────────────
		{
			Name: "jobs.pending", Unit: "count", Component: CompJobs, Kind: KindGauge,
			Description: "Jobs submitted but not yet leased to a worker (PENDING) at the last tick",
		},
		{
			Name: "jobs.running", Unit: "count", Component: CompJobs, Kind: KindGauge,
			Description: "Jobs leased to a worker or actively running (LEASED + RUNNING) at the last tick",
		},
		{
			Name: "jobs.oldest_pending_age_seconds", Unit: "seconds", Component: CompJobs, Kind: KindGauge,
			Description: "Age of the oldest PENDING job; 0 when the queue is empty (stuck-job signal)",
		},
		// ── Terminal outcomes + end-to-end latency ─────────────────────
		{
			Name: "job.succeeded_total", Unit: "count", Component: CompJob, Kind: KindCounter,
			Description: "Jobs that reached the SUCCEEDED terminal state",
		},
		{
			Name: "job.failed_total", Unit: "count", Component: CompJob, Kind: KindCounter,
			Description: "Jobs that reached the FAILED terminal state (CANCELLED excluded)",
		},
		{
			Name: "job.e2e_duration_seconds", Unit: "seconds", Component: CompJob, Kind: KindHistogram,
			Description: "Submit-to-terminal job latency by phase label (queue = submit→start, execute = start→terminal, total = submit→terminal)",
		},
	}
}
