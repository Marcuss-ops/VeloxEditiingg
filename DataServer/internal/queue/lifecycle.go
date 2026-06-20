// Package queue provides job queue management with SQLite persistence
package queue

import (
	"velox-server/internal/jobs"
	"velox-server/internal/store"
)

// LifecycleService validates and executes job status transitions.
//
// All mutation methods live in lifecycle_pr3.go (transactional PR3 path).
// The legacy non-transactional methods (FailJob, SubmitJob, TransitionToRunning,
// LeaseJob, ReleaseClaim, RenewLease, RequeueZombieJobs, RecordRenderFinished,
// Validate) have been removed — zero external callers remained.
//
// Dual-stack design (Ondata 3 PR3):
//   - repo  (store.JobRepository): legacy PR3 surface (ClaimNext, StartJob,
//     PR3RecordRenderFinished, PR3RenewLease, ReleaseClaim, etc.) — still
//     needed for complex workflow operations not yet migrated to the domain
//     interface.
//   - jobsRepo (jobs.Repository): canonical domain surface (Get, List, Counts,
//     Create, SetStatus, Lease, Fail). New code and simple read/write paths
//     use this; complex PR3 operations continue to use repo until migration
//     is complete (future PR). Both are satisfied by the same concrete
//     *store.SQLiteJobRepository.
type LifecycleService struct {
	repo     store.JobRepository // legacy PR3 surface
	jobsRepo jobs.Repository     // canonical domain surface (Ondata 3 PR3)
	clock    Clock
}
