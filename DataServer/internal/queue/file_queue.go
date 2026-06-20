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

// QueueItem is the scheduling representation of a job in the queue.
// It carries the full operational state needed by HTTP handlers and
// legacy consumers. New code should use jobs.Job for domain logic
// and queue.QueueItem for scheduling/transport concerns.
//
// Job is retained as a type alias for backward compatibility with all
// existing callers that reference queue.Job. New code should use
// QueueItem directly.
type QueueItem struct {
	JobID        string      `json:"job_id"`
	Status       JobStatus   `json:"status"`
	VideoName    string      `json:"video_name,omitempty"`
	ProjectID    string      `json:"project_id,omitempty"`
	CreatedAt    interface{} `json:"created_at,omitempty"`
	UpdatedAt    interface{} `json:"updated_at,omitempty"`
	StartedAt    interface{} `json:"started_at,omitempty"`
	CompletedAt  interface{} `json:"completed_at,omitempty"`
	AssignedAt   interface{} `json:"assigned_at,omitempty"`
	LeaseExpiry  interface{} `json:"lease_expiry,omitempty"`
	ProcessingAt interface{} `json:"processing_at,omitempty"`

	AssignedTo       string `json:"assigned_to,omitempty"`
	AssignedWorkerIP string `json:"assigned_worker_ip,omitempty"`
	WorkerName       string `json:"worker_name,omitempty"`
	ClaimedBy        string `json:"claimed_by,omitempty"`
	ClaimedAt        string `json:"claimed_at,omitempty"`
	LeaseID          string `json:"lease_id,omitempty"`

	RetryCount int `json:"retry_count,omitempty"`
	Attempt    int `json:"attempt,omitempty"`
	MaxRetries int `json:"max_retries,omitempty"`

	LastError    string      `json:"last_error,omitempty"`
	LastErrorAt  interface{} `json:"last_error_at,omitempty"`
	ErrorMessage string      `json:"error_message,omitempty"`
	FailedAt     interface{} `json:"failed_at,omitempty"`
	FailedBy     string      `json:"failed_by,omitempty"`

	History []JobHistoryEntry `json:"history,omitempty"`

	Logs          []JobLogEntry `json:"logs,omitempty"`
	LogsUpdatedAt string        `json:"logs_updated_at,omitempty"`

	SlotData map[string]interface{} `json:"slot_data,omitempty"`

	JobFingerprint string `json:"job_fingerprint,omitempty"`

	SubmittedVia string `json:"submitted_via,omitempty"`
	LastActivity string `json:"last_activity,omitempty"`
	RunID        string `json:"run_id,omitempty"`

	Payload map[string]interface{} `json:"-"`
}

// Job is a backward-compatible type alias for QueueItem.
// All existing callers that reference queue.Job continue to compile.
// New code should use QueueItem directly or the canonical jobs.Job domain model.
type Job = QueueItem

// JobHistoryEntry represents a status change in job history
type JobHistoryEntry struct {
	Status    string      `json:"status"`
	Timestamp interface{} `json:"timestamp"`
	WorkerID  string      `json:"worker_id,omitempty"`
	Message   string      `json:"message,omitempty"`
}

// JobLogEntry represents a log entry from the worker
type JobLogEntry struct {
	Timestamp string `json:"timestamp,omitempty"`
	Time      string `json:"time,omitempty"`
	Message   string `json:"message,omitempty"`
	Level     string `json:"level,omitempty"`
	IsError   bool   `json:"is_error,omitempty"`
	WorkerID  string `json:"worker_id,omitempty"`
}

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
