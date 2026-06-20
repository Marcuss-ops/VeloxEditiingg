// Package queue provides job queue management with SQLite persistence
package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"velox-server/internal/jobs"
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

// QueueItem is a backward-compatible alias to the canonical transport
// projection type in the jobs package. The full struct (with all 30
// JSON-tagged fields + history/logs/slot_data) lives at jobs.QueueItem.
//
// Synonym chain (all three refer to the SAME type at compile time):
//
//	queue.QueueItem  ==  queue.Job  ==  jobs.QueueItem
//
// This file keeps only the aliases for backward compatibility with every
// existing caller that imports queue.QueueItem / queue.Job. New code
// should use jobs.QueueItem directly. Phase 2 of Ondata 4 Strategy B
// will sweep remaining queue.QueueItem / queue.Job references; once
// zero remain, both aliases will be dropped.
type QueueItem = jobs.QueueItem

// Job is a backward-compatible alias for QueueItem — see the QueueItem
// alias above for the synonym chain and migration guidance.
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

	// PR15.5: canonical Create via jobs.Writer (replaces Repo().CreateJob).
	raw, _ := json.Marshal(payload)
	job := &jobs.Job{
		ID:         jobID,
		Status:     jobs.StatusPending,
		VideoName:  videoName,
		ProjectID:  projectID,
		RunID:      runID,
		MaxRetries: q.maxRetries,
		Payload:    string(raw),
	}
	if err := q.lifecycle.Jobs().Create(ctx, job); err != nil {
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
