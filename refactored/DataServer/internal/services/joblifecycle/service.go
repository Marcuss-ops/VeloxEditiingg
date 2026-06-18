// Package joblifecycle provides targeted job lifecycle operations that replace
// the legacy UpdateJobFields catch-all. Each method maps to a single business
// operation and writes only the columns it owns.
package joblifecycle

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"velox-server/internal/queue"
	"velox-server/internal/store"
)

// Service provides job lifecycle operations.
// All methods use Compare-And-Swap (CAS) for status transitions and targeted
// column updates for non-status fields — never the legacy UpdateJobFields.
type Service struct {
	ts         *queue.TransitionService
	dbStore    *store.SQLiteStore
	maxRetries int
}

// NewService creates a new JobLifecycleService.
func NewService(ts *queue.TransitionService, dbStore *store.SQLiteStore, maxRetries int) *Service {
	return &Service{
		ts:         ts,
		dbStore:    dbStore,
		maxRetries: maxRetries,
	}
}

// CompleteJobResult carries the non-status fields to write after job completion.
type CompleteJobResult struct {
	CompletedBy    string
	ArtifactID     string
	OutputSHA256   string
	IdempotencyKey string
	EndTime        string
}

// SubmitResult completes a job and writes result metadata using targeted
// store methods — never UpdateJobFields.
func (s *Service) SubmitResult(ctx context.Context, jobID string, result CompleteJobResult) error {
	// 1. CAS transition to SUCCEEDED via TransitionService
	if err := s.ts.CompleteJob(ctx, jobID); err != nil {
		return fmt.Errorf("complete job: %w", err)
	}

	// 2. Write supplementary fields directly
	nowISO := time.Now().UTC().Format(time.RFC3339)
	supplementary := map[string]interface{}{
		"completed_at": nowISO,
	}
	if result.EndTime != "" {
		supplementary["completed_at"] = result.EndTime
	}
	if result.CompletedBy != "" {
		supplementary["completed_by"] = result.CompletedBy
	}
	_ = s.dbStore.UpdateJobSupplementary(jobID, supplementary)

	// 3. Write clean result_json (no operational fields)
	cleanResult := map[string]interface{}{
		"job_id": jobID,
	}
	if result.ArtifactID != "" {
		cleanResult["primary_artifact_id"] = result.ArtifactID
	}
	if result.OutputSHA256 != "" {
		cleanResult["output_sha256"] = result.OutputSHA256
	}
	if result.IdempotencyKey != "" {
		cleanResult["upload_idempotency_key"] = result.IdempotencyKey
	}

	if len(cleanResult) > 1 { // more than just job_id
		resultJSON, err := json.Marshal(cleanResult)
		if err == nil {
			_ = s.dbStore.UpsertJobResult(jobID, resultJSON)
		}
	}

	return nil
}

// RenewLease extends the lease for an active job.
func (s *Service) RenewLease(ctx context.Context, jobID, workerID, leaseID string, leaseExpiry time.Time) error {
	return s.ts.RenewLease(ctx, jobID, workerID, leaseID, leaseExpiry)
}

// FailJob marks a job as FAILED with optional requeue.
func (s *Service) FailJob(ctx context.Context, jobID, errMsg, workerID string, requeue bool) error {
	return s.ts.FailJob(ctx, jobID, errMsg, workerID, requeue, s.maxRetries)
}

// CancelJob marks a job as CANCELLED.
func (s *Service) CancelJob(ctx context.Context, jobID string) error {
	job, err := s.ts.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("job not found: %s", jobID)
	}
	if err := s.ts.Validate(job.Status, queue.StatusCancelled); err != nil {
		return fmt.Errorf("cannot cancel job %s: %w", jobID, err)
	}

	nowISO := queue.NowISO()
	now := queue.NowUnix()

	job.Status = queue.StatusCancelled
	job.UpdatedAt = now
	job.History = append(job.History, queue.JobHistoryEntry{
		Status:    string(queue.StatusCancelled),
		Timestamp: nowISO,
		Message:   "Job cancelled",
	})

	return queue.PersistJob(job, s.dbStore)
}

// UpdateCompletedAt writes only the completed_at timestamp without a status transition.
// Used by the calendar reconciler to mark scheduling state.
func (s *Service) UpdateCompletedAt(ctx context.Context, jobID, completedAt string) error {
	return s.dbStore.UpdateJobSupplementary(jobID, map[string]interface{}{
		"completed_at": completedAt,
	})
}
