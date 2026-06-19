// Package queue provides job queue management with SQLite persistence
package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"velox-server/internal/store"
)

// JobStatus represents the current state of a job.
// Canonical statuses (7 total):
//
//	PENDING → LEASED → RUNNING → SUCCEEDED
//	                   ↓
//	              RETRY_WAIT → PENDING (retry)
//	                   ↓
//	              FAILED
//	PENDING → CANCELLED
type JobStatus string

const (
	StatusPending   JobStatus = "PENDING"
	StatusLeased    JobStatus = "LEASED"
	StatusRunning   JobStatus = "RUNNING"
	StatusRetryWait JobStatus = "RETRY_WAIT"
	StatusSucceeded JobStatus = "SUCCEEDED"
	StatusFailed    JobStatus = "FAILED"
	StatusCancelled JobStatus = "CANCELLED"
)

// Job represents a job in the queue (compatible with Python schema)
type Job struct {
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
type FileQueue struct {
	maxRetries int
	dbStore    *store.SQLiteStore
	lifecycle  *LifecycleService
	query      *QueryService

	eventLogger func(jobID, eventType string, extra map[string]interface{})
}

// FileQueueConfig holds configuration for the file queue
type FileQueueConfig struct {
	MaxRetries int
	DBStore    *store.SQLiteStore
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
	if cfg.DBStore == nil {
		return nil, fmt.Errorf("SQLiteStore is required for FileQueue")
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}

	q := &FileQueue{
		maxRetries: cfg.MaxRetries,
		dbStore:    cfg.DBStore,
		lifecycle:  lifecycle,
		query:      query,
	}

	return q, nil
}

// SetEventLogger sets a callback for job events
func (q *FileQueue) SetEventLogger(logger func(jobID, eventType string, extra map[string]interface{})) {
	q.eventLogger = logger
}

func (q *FileQueue) logEvent(jobID, eventType string, extra map[string]interface{}) {
	if q.eventLogger != nil {
		q.eventLogger(jobID, eventType, extra)
	}
}

// ── Mutation methods ──
//
// Each method delegates directly to the JobRepository or LifecycleService PR3
// methods. No eventStore side effects — the transactional PR3 methods on the
// repository handle history + event + outbox atomically.

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
	q.logEvent(jobID, "created", map[string]interface{}{
		"project_id": projectID,
		"video_name": videoName,
	})
	return nil
}

func (q *FileQueue) ClaimNextJob(ctx context.Context, workerID string, allowedJobTypes []string) (*Job, error) {
	result, err := q.lifecycle.Repo().ClaimNext(ctx, store.ClaimParams{
		WorkerID:        workerID,
		AllowedJobTypes: allowedJobTypes,
		Now:             time.Now().UTC(),
	})
	if err != nil {
		return nil, err
	}
	if result == nil || result.JobID == "" {
		return nil, nil
	}
	sj, err := q.lifecycle.Repo().GetJob(ctx, result.JobID)
	if err != nil {
		// Release the claim so the job is not left orphaned.
		if releaseErr := q.lifecycle.Repo().ReleaseClaim(ctx, result.JobID); releaseErr != nil {
			q.logEvent(result.JobID, "release_claim_failed", map[string]interface{}{
				"error": releaseErr.Error(),
			})
		}
		return nil, fmt.Errorf("post-claim job fetch: %w", err)
	}
	if sj == nil {
		// Release the claim — job no longer exists.
		if releaseErr := q.lifecycle.Repo().ReleaseClaim(ctx, result.JobID); releaseErr != nil {
			q.logEvent(result.JobID, "release_claim_failed", map[string]interface{}{
				"error": releaseErr.Error(),
			})
		}
		return nil, nil
	}

	// Parse payload from the request_json column.
	var payload map[string]interface{}
	if sj.PayloadJSON != "" && sj.PayloadJSON != "{}" {
		_ = json.Unmarshal([]byte(sj.PayloadJSON), &payload)
	}

	job := &Job{
		JobID:       sj.JobID,
		Status:      JobStatus(sj.Status),
		VideoName:   sj.VideoName,
		ProjectID:   sj.ProjectID,
		AssignedTo:  sj.AssignedTo,
		LeaseID:     sj.LeaseID,
		RetryCount:  sj.RetryCount,
		MaxRetries:  sj.MaxRetries,
		CreatedAt:   sj.CreatedAt,
		UpdatedAt:   sj.UpdatedAt,
		StartedAt:   sj.StartedAt,
		CompletedAt: sj.CompletedAt,
		Attempt:     result.Attempt,
		RunID:       sj.RunID,
		Payload:     payload,
		LeaseExpiry: result.LeaseExpires,
	}
	q.logEvent(job.JobID, "claimed", map[string]interface{}{
		"worker_id": workerID,
	})
	return job, nil
}

func (q *FileQueue) FailJob(ctx context.Context, jobID, errMsg, workerID string, requeue bool) error {
	// Use the PR3 transactional Fail method
	cmd := store.FailCommand{
		JobID:        jobID,
		WorkerID:     workerID,
		ErrorMessage: errMsg,
		Retryable:    requeue,
		Now:          time.Now().UTC(),
	}
	if err := q.lifecycle.Fail(ctx, cmd); err != nil {
		return err
	}
	q.logEvent(jobID, "failed", map[string]interface{}{
		"error":    errMsg,
		"requeued": requeue,
	})
	return nil
}

func (q *FileQueue) LeaseJob(ctx context.Context, jobID, workerID string) error {
	if err := q.lifecycle.Repo().LeaseJob(ctx, jobID, workerID); err != nil {
		return err
	}
	q.logEvent(jobID, "claimed", map[string]interface{}{
		"worker_id": workerID,
	})
	return nil
}

func (q *FileQueue) RenewJobLease(ctx context.Context, jobID, workerID, leaseID string, leaseExpiry time.Time) error {
	return q.lifecycle.Repo().PR3RenewLease(ctx, store.RenewLeaseCommand{
		JobID:       jobID,
		WorkerID:    workerID,
		LeaseID:     leaseID,
		LeaseExpiry: leaseExpiry.UTC(),
		EmitEvent:   true,
	})
}

func (q *FileQueue) RequeueZombieJobs(ctx context.Context, timeout time.Duration) (int, error) {
	results, err := q.lifecycle.RequeueExpiredLeases(ctx, 100)
	if err != nil {
		return 0, err
	}
	return len(results), nil
}

// ── Query methods (QueryService) ──

func (q *FileQueue) GetJob(ctx context.Context, jobID string) (*Job, error) {
	return q.query.GetJob(ctx, jobID)
}

func (q *FileQueue) GetJobPayload(ctx context.Context, jobID string) (map[string]interface{}, error) {
	return q.query.GetJobPayload(ctx, jobID)
}

func (q *FileQueue) GetJobAttempt(ctx context.Context, jobID string) (int, error) {
	return q.query.GetJobAttempt(ctx, jobID)
}

func (q *FileQueue) GetJobsByStatus(ctx context.Context, status JobStatus) ([]*Job, error) {
	return q.query.GetJobsByStatus(ctx, status)
}

func (q *FileQueue) GetPendingJobs(ctx context.Context) ([]*Job, error) {
	return q.query.GetPendingJobs(ctx)
}

func (q *FileQueue) GetRunningJobs(ctx context.Context) ([]*Job, error) {
	return q.query.GetRunningJobs(ctx)
}

func (q *FileQueue) GetAllJobs(ctx context.Context) (map[string]*Job, error) {
	return q.query.GetAllJobs(ctx)
}

func (q *FileQueue) Stats(ctx context.Context) (map[string]int64, error) {
	return q.query.Stats(ctx)
}

func (q *FileQueue) GetJobAsMap(ctx context.Context, jobID string) (map[string]interface{}, error) {
	return q.query.GetJobAsMap(ctx, jobID)
}

func (q *FileQueue) GetNextJobID(ctx context.Context) (string, error) {
	return q.query.GetNextJobID(ctx)
}

func (q *FileQueue) DeleteJob(ctx context.Context, jobID string) error {
	if err := q.query.DeleteJob(ctx, jobID); err != nil {
		return err
	}
	q.logEvent(jobID, "deleted", nil)
	return nil
}

func (q *FileQueue) UpdateJobLogs(ctx context.Context, jobID string, logs []JobLogEntry) error {
	return q.query.UpdateJobLogs(ctx, jobID, logs)
}

// ── Maintenance ──

func (q *FileQueue) CleanupOldJobs(ctx context.Context, age time.Duration) (int, error) {
	return q.query.CleanupOldJobs(ctx, age)
}

// GetDBStore returns the underlying SQLite store.
func (q *FileQueue) GetDBStore() *store.SQLiteStore {
	return q.dbStore
}

// LifecycleService returns the lifecycle service for direct use.
func (q *FileQueue) LifecycleService() *LifecycleService {
	return q.lifecycle
}

// QueryService returns the query service for direct use.
func (q *FileQueue) QueryService() *QueryService {
	return q.query
}
