// Package metrics / collector_scheduling.go
//
// Dispatch-side wait histograms. These close the exposure gap the
// metric review called out: `queue.ms`, `lease_wait.ms` and
// `time_to_first_worker_ms` were collected per attempt (migration 074)
// and rolled up daily, but had NO Prometheus family — the "1 minute of
// attesa" half of a job's latency was only visible through ad-hoc SQL.
//
//	velox_queue_wait_ms                   READY-queue wait before claim
//	velox_lease_wait_ms                   claim → worker acceptance
//	velox_queue_time_to_first_worker_ms   submit → first worker
//
// All three are in MILLISECONDS to match their catalog keys and the
// underlying columns (the sibling velox_delivery_queue_ms family sets
// the same precedent; seconds-family histograms stay seconds). No
// labels: the distribution itself is the question, and per-job /
// per-task labels are banned by the cardinality contract.
package metrics

import "velox-server/internal/taskattempts"

// schedulingWaitBuckets spans sub-frame dispatch latency up to a
// 5-minute queue stall — the band where the Milton-type incident
// lives (job waiting ~60s+ before any worker accepts).
var schedulingWaitBuckets = []float64{10, 50, 100, 500, 1000, 5000, 15000, 60000, 300000}

// initSchedulingFamilies creates the dispatch wait histograms. Called
// once from NewCollector at boot.
func (c *Collector) initSchedulingFamilies() {
	c.queueWaitMS = NewHistogramFamily(
		"velox_queue_wait_ms",
		"Time the task waited in the READY queue before being claimed, in milliseconds",
		[]string{},
		schedulingWaitBuckets,
	)
	c.leaseWaitMS = NewHistogramFamily(
		"velox_lease_wait_ms",
		"Time between claim and worker acceptance, in milliseconds",
		[]string{},
		schedulingWaitBuckets,
	)
	c.timeToFirstWorkerMS = NewHistogramFamily(
		"velox_queue_time_to_first_worker_ms",
		"End-to-end scheduling latency (submit to first worker), in milliseconds",
		[]string{},
		schedulingWaitBuckets,
	)
}

// schedulingFamilies returns the dispatch-wait subset registered by
// NewCollector via allFamilies.
func (c *Collector) schedulingFamilies() []*Family {
	return []*Family{c.queueWaitMS, c.leaseWaitMS, c.timeToFirstWorkerMS}
}

// recordSchedulingWaits observes the three wait histograms for one
// attempt. Zero means "not reported" (older workers, or a row that
// predates migration 074) — never an observation of a 0ms wait.
func (c *Collector) recordSchedulingWaits(am taskattempts.AttemptMetrics) {
	if am.QueueMS > 0 {
		c.queueWaitMS.Observe([]string{}, float64(am.QueueMS))
	}
	if am.LeaseWaitMS > 0 {
		c.leaseWaitMS.Observe([]string{}, float64(am.LeaseWaitMS))
	}
	if am.TimeToFirstWorkerMS > 0 {
		c.timeToFirstWorkerMS.Observe([]string{}, float64(am.TimeToFirstWorkerMS))
	}
}
