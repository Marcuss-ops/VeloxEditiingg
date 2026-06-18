// Package joblifecycle provides targeted job lifecycle operations.
// Each method maps to a single business operation and writes only the
// columns it owns via Compare-And-Swap (CAS).
package joblifecycle

import (
	"context"
	"time"

	"velox-server/internal/queue"
	"velox-server/internal/store"
)

// Service provides job lifecycle operations.
// All methods use Compare-And-Swap (CAS) for status transitions and targeted
// column updates for non-status fields.
type Service struct {
	lc         *queue.LifecycleService
	dbStore    *store.SQLiteStore
	maxRetries int
}

// NewService creates a new JobLifecycleService.
func NewService(lc *queue.LifecycleService, dbStore *store.SQLiteStore, maxRetries int) *Service {
	return &Service{
		lc:         lc,
		dbStore:    dbStore,
		maxRetries: maxRetries,
	}
}

// RenewLease extends the lease for an active job.
func (s *Service) RenewLease(ctx context.Context, jobID, workerID, leaseID string, leaseExpiry time.Time) error {
	return s.lc.Repo().PR3RenewLease(ctx, store.RenewLeaseCommand{
		JobID:       jobID,
		WorkerID:    workerID,
		LeaseID:     leaseID,
		LeaseExpiry: leaseExpiry,
		Now:         time.Now().UTC(),
		EmitEvent:   true,
	})
}

// FailJob marks a job as FAILED with optional requeue.
func (s *Service) FailJob(ctx context.Context, jobID, errMsg, workerID string, requeue bool) error {
	cmd := store.FailCommand{
		JobID:        jobID,
		WorkerID:     workerID,
		ErrorMessage: errMsg,
		Retryable:    requeue,
		Now:          time.Now().UTC(),
	}
	return s.lc.Fail(ctx, cmd)
}

// CancelJob marks a job as CANCELLED.
func (s *Service) CancelJob(ctx context.Context, jobID string) error {
	cmd := store.CancelCommand{
		JobID:  jobID,
		Reason: "cancelled by user",
		Now:    time.Now().UTC(),
	}
	return s.lc.Cancel(ctx, cmd)
}

// UpdateCompletedAt writes only the completed_at timestamp without a status transition.
// Used by the calendar reconciler to mark scheduling state.
func (s *Service) UpdateCompletedAt(ctx context.Context, jobID, completedAt string) error {
	return s.dbStore.UpdateJobSupplementary(jobID, map[string]interface{}{
		"completed_at": completedAt,
	})
}
