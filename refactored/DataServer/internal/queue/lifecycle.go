// Package queue provides job queue management with SQLite persistence
package queue

import "velox-server/internal/store"

// LifecycleService validates and executes job status transitions.
//
// All mutation methods live in lifecycle_pr3.go (transactional PR3 path).
// The legacy non-transactional methods (FailJob, SubmitJob, TransitionToRunning,
// LeaseJob, ReleaseClaim, RenewLease, RequeueZombieJobs, RecordRenderFinished,
// Validate) have been removed — zero external callers remained.
type LifecycleService struct {
	repo  store.JobRepository
	clock Clock
}
