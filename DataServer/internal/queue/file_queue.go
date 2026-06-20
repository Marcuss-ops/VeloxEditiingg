// Package queue provides job queue management with SQLite persistence
package queue

import (
	"context"
	"fmt"

	"velox-server/internal/jobs"
	"velox-server/internal/store"
)

// JobStatus is a type alias for the canonical jobs.Status. It exists so
// existing callers importing queue.JobStatus continue to compile without
// changes while the type itself is unified at compile time with jobs.Status.
// All status constants are re-exported aliases from the jobs package.
// New code should import and use jobs.Status / jobs.StatusPending directly.
type JobStatus = jobs.Status

const (
	StatusPending   = jobs.StatusPending
	StatusLeased    = jobs.StatusLeased
	StatusRunning   = jobs.StatusRunning
	StatusRetryWait = jobs.StatusRetryWait
	StatusSucceeded = jobs.StatusSucceeded
	StatusFailed    = jobs.StatusFailed
	StatusCancelled = jobs.StatusCancelled
)

// QueueItem is a backward-compatible alias to the canonical transport type
// defined in the jobs package (Phase 1 of Ondata 4 Strategy B). The full
// struct (with all 30 JSON-tagged fields + history/logs/slot_data) lives at
// jobs.QueueItem — this file keeps only the alias for backward-compat with
// every existing caller that imports queue.QueueItem / queue.Job.
//
// Job is also a backward-compatible alias for QueueItem (== jobs.QueueItem).
// New code should reference jobs.QueueItem / jobs.Job directly.
type QueueItem = jobs.QueueItem

// Job is a backward-compatible alias for QueueItem (== jobs.QueueItem).
// All existing callers that reference queue.Job continue to compile.
// New code should use jobs.Job or jobs.QueueItem directly.
type Job = jobs.QueueItem

// JobHistoryEntry is a backward-compatible alias to jobs.JobHistoryEntry
// (the canonical history-event type).
type JobHistoryEntry = jobs.JobHistoryEntry

// JobLogEntry is a backward-compatible alias to jobs.JobLogEntry
// (the canonical log-event type).
type JobLogEntry = jobs.JobLogEntry

// FileQueue implements a SQLite-backed job queue with separated lifecycle and query services.
//
// Only methods with production callers are exposed. All pass-through query
// wrappers and dead mutation methods have been removed — callers should use
// LifecycleService() or QueryService() directly.
type FileQueue struct {
	maxRetries int
	lifecycle  *LifecycleService
	query      *QueryService
}

// FileQueueConfig holds configuration for the file queue
type FileQueueConfig struct {
	MaxRetries int
}

// NewFileQueue creates a new SQLite-backed queue.
// LifecycleService and QueryService are mandatory — injected via dependency injection.
func NewFileQueue(cfg *FileQueueConfig, lifecycle *LifecycleService, query *QueryService) (*FileQueue, error) {
	if lifecycle == nil {
		return nil, fmt.Errorf("LifecycleService is required")
	}
	if query == nil {
		return nil, fmt.Errorf("QueryService is required")
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}

	q := &FileQueue{
		maxRetries: cfg.MaxRetries,
		lifecycle:  lifecycle,
		query:      query,
	}

	return q, nil
}

// ── Mutation methods (production callers) ──

func (q *FileQueue) SubmitJob(ctx context.Context, jobID string, payload map[string]interface{}) error {
	var videoName, projectID, runID string
	if s, ok := payload["video_name"].(string); ok {
		videoName = s
	}
	if s, ok := payload["project_id"].(string); ok {
		projectID = s
	}
	if s, ok := payload["run_id"].(string); ok && s != "" {
		runID = s
	}

	params := store.CreateJobParams{
		JobID:      jobID,
		Payload:    payload,
		VideoName:  videoName,
		ProjectID:  projectID,
		RunID:      runID,
		MaxRetries: q.maxRetries,
	}
	if err := q.lifecycle.Repo().CreateJob(ctx, params); err != nil {
		return fmt.Errorf("submit job: %w", err)
	}
	return nil
}

// ── Query methods (production callers) ──

func (q *FileQueue) GetJob(ctx context.Context, jobID string) (*Job, error) {
	return q.query.GetJob(ctx, jobID)
}

func (q *FileQueue) GetAllJobs(ctx context.Context) ([]*Job, error) {
	return q.query.GetAllJobs(ctx)
}

func (q *FileQueue) Stats(ctx context.Context) (map[string]int64, error) {
	return q.query.Stats(ctx)
}

// DeleteJob hard-deletes a job via the canonical jobs.Writer surface
// (LifecycleService.Jobs()). The legacy eventStore.DeleteJob path has
// been dropped (Batch 3).
func (q *FileQueue) DeleteJob(ctx context.Context, jobID string) error {
	return q.lifecycle.Jobs().Delete(ctx, jobID)
}

// ── Accessors ──

// LifecycleService returns the lifecycle service for direct use.
func (q *FileQueue) LifecycleService() *LifecycleService {
	return q.lifecycle
}

// QueryService returns the query service for direct use.
func (q *FileQueue) QueryService() *QueryService {
	return q.query
}
