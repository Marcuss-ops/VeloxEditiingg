// Package jobs / queue_snapshot.go
//
// Read-only projections consumed by the master's metrics supervisor to
// answer the two operator questions that previously required ad-hoc SQL:
// "are jobs arriving and how deep is the queue?" and "how long does a
// job take end-to-end (submit → delivered)?".
//
// Both types are pure data: the queries live in the store layer
// (store/jobs_repository_metrics.go), the consumers in internal/metrics.
package jobs

import "time"

// QueueSnapshot is the instantaneous master job-queue projection.
//
// Pending counts jobs in PENDING (submitted, not yet leased to a
// worker). Running counts jobs already claimed by a worker
// (LEASED + RUNNING) — the two states an operator reads as "work in
// flight". OldestPendingAge is the age of the OLDEST PENDING job
// (zero when nothing is pending); it is the "stuck job" signal that a
// depth-only gauge cannot express: a queue of 1 job that has waited 7
// minutes is the incident, not the depth.
type QueueSnapshot struct {
	Pending          int64
	Running          int64
	OldestPendingAge time.Duration
}

// CompletionRecord is one terminal job's lifecycle timestamps on the
// MASTER clock. It is the raw input for the end-to-end job latency
// histogram: CreatedAt → StartedAt is the queue wait, StartedAt →
// CompletedAt the execution, CreatedAt → CompletedAt the full
// submit→terminal span. Zero timestamps mean the column was never
// written; consumers must treat those stages as unknown rather than
// fabricate a duration.
type CompletionRecord struct {
	ID          string
	Status      JobStatus
	CreatedAt   time.Time
	StartedAt   time.Time
	CompletedAt time.Time
}
