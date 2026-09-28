// Package metrics / collector_jobs.go
//
// Master job-queue + end-to-end job latency families. Before these
// existed the only queue depth on the metrics surface was
// velox_forwarding_queue_depth (the creator-forwarding subsystem); the
// REAL job queue on the master had no Prometheus projection at all, so
// the operator question "are jobs arriving? how deep is the queue? how
// long has the oldest one been waiting?" required ad-hoc SQL against
// the jobs table.
//
// Families (all bounded-label; no job_id/task_id ever becomes a label):
//
//	velox_jobs_pending                       gauge  — PENDING jobs right now
//	velox_jobs_running                       gauge  — LEASED + RUNNING jobs right now
//	velox_jobs_oldest_pending_age_seconds    gauge  — age of the oldest PENDING job (0 = empty queue)
//	velox_job_succeeded_total                counter — jobs that reached SUCCEEDED
//	velox_job_failed_total                   counter — jobs that reached FAILED (CANCELLED excluded)
//	velox_job_e2e_duration_seconds{phase}    histogram — queue | execute | total
//
// The queue gauges answer "is work arriving / is the queue growing";
// the oldest-pending gauge is the stuck-job signal (a depth of 1 that
// has waited 7 minutes is the incident, not the depth). The e2e
// histogram carries the phase attribution the waterfall has per attempt
// but Prometheus never had per job:
//
//	phase=queue   created_at → started_at   (submit → first execution start)
//	phase=execute started_at → completed_at (execution + delivery)
//	phase=total   created_at → completed_at (submit → terminal)
//
// `phase` is part of the dashboard label allowlist
// (scripts/ci/check-observability-dashboards.sh), so every panel can
// slice these histograms. Never sum across phases — total already
// includes the other two; filter phase="total" (or phase!="total") in
// PromQL.
package metrics

import (
	"time"

	"velox-server/internal/jobs"
)

// jobE2ESecondsBuckets covers the observed job envelope: sub-minute
// fast paths up to multi-hour renders (the documented ~5min Milton
// case sits in the 300–600s band).
var jobE2ESecondsBuckets = []float64{30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 14400, 28800}

// initJobFamilies creates the master job-queue + job-completion
// families. Called once from NewCollector at boot.
func (c *Collector) initJobFamilies() {
	c.jobsPending = NewGaugeFamily(
		"velox_jobs_pending",
		"Jobs in PENDING state (submitted, not yet leased to a worker) at the last supervisor tick",
		[]string{},
	)
	c.jobsRunning = NewGaugeFamily(
		"velox_jobs_running",
		"Jobs leased to a worker or actively running (LEASED + RUNNING) at the last supervisor tick",
		[]string{},
	)
	c.jobsOldestPendingAge = NewGaugeFamily(
		"velox_jobs_oldest_pending_age_seconds",
		"Age in seconds of the oldest PENDING job; 0 when no job is waiting",
		[]string{},
	)
	c.jobSucceededTotal = NewCounterFamily(
		"velox_job_succeeded_total",
		"Jobs that reached the SUCCEEDED terminal state",
		[]string{},
	)
	c.jobFailedTotal = NewCounterFamily(
		"velox_job_failed_total",
		"Jobs that reached the FAILED terminal state (CANCELLED jobs are excluded)",
		[]string{},
	)
	c.jobE2EDuration = NewHistogramFamily(
		"velox_job_e2e_duration_seconds",
		"Job end-to-end duration by lifecycle phase: queue (submit to first execution start), execute (execution start to terminal), total (submit to terminal)",
		[]string{"phase"},
		jobE2ESecondsBuckets,
	)
}

// jobFamilies returns the job-queue subset registered by NewCollector
// via allFamilies.
func (c *Collector) jobFamilies() []*Family {
	return []*Family{
		c.jobsPending, c.jobsRunning, c.jobsOldestPendingAge,
		c.jobSucceededTotal, c.jobFailedTotal, c.jobE2EDuration,
	}
}

// RecordJobQueue stamps the queue gauges from one supervisor snapshot.
// Set-to-current-value semantics: repeat calls with the same snapshot
// are idempotent, and an empty queue writes an honest 0 instead of
// leaving the previous depth on screen.
func (c *Collector) RecordJobQueue(snapshot jobs.QueueSnapshot) {
	c.jobsPending.GaugeSet([]string{}, snapshot.Pending)
	c.jobsRunning.GaugeSet([]string{}, snapshot.Running)
	c.jobsOldestPendingAge.GaugeSet([]string{}, int64(snapshot.OldestPendingAge.Seconds()))
}

// RecordJobCompletion stamps one terminal job: the outcome counter
// (succeeded / failed) and every e2e stage whose two boundary
// timestamps are both known. A missing timestamp leaves that stage
// unobserved — absence stays honest, a fabricated zero does not.
//
// The caller (metrics supervisor) dedups by job id, so each terminal
// job is counted once per process.
func (c *Collector) RecordJobCompletion(record jobs.CompletionRecord) {
	switch record.Status {
	case jobs.StatusSucceeded:
		c.jobSucceededTotal.Inc([]string{}, 1)
	case jobs.StatusFailed:
		c.jobFailedTotal.Inc([]string{}, 1)
	}
	c.observeJobStage("queue", record.CreatedAt, record.StartedAt)
	c.observeJobStage("execute", record.StartedAt, record.CompletedAt)
	c.observeJobStage("total", record.CreatedAt, record.CompletedAt)
}

// observeJobStage records one e2e stage when both boundaries exist and
// move forward in time (an inverted pair is clock corruption, never a
// duration).
func (c *Collector) observeJobStage(phase string, start, end time.Time) {
	if start.IsZero() || end.IsZero() || !end.After(start) {
		return
	}
	c.jobE2EDuration.Observe([]string{phase}, end.Sub(start).Seconds())
}
