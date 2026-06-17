// Package queue provides job queue management with SQLite persistence
package queue

import (
	"context"
	"fmt"
	"log"
	"time"

	"velox-server/internal/store"
)

// JobStatus represents the current state of a job
type JobStatus string

const (
	StatusPending    JobStatus = "PENDING"
	StatusProcessing JobStatus = "PROCESSING"
	StatusCompleted  JobStatus = "COMPLETED"
	StatusError      JobStatus = "ERROR"
	StatusFailed     JobStatus = "FAILED"
	StatusQueued     JobStatus = "QUEUED"
	StatusAssigned   JobStatus = "ASSIGNED"
	StatusLeased     JobStatus = "LEASED"
	StatusCancelling JobStatus = "CANCELLING"
	StatusCancelled  JobStatus = "CANCELLED"
	StatusLost       JobStatus = "LOST"
	StatusRetrying   JobStatus = "RETRYING"
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

	SlotData      map[string]interface{} `json:"slot_data,omitempty"`
	OutputVideoID string                 `json:"output_video_id,omitempty"`
	DriveURL      string                 `json:"drive_url,omitempty"`

	VideoUploaded         bool   `json:"video_uploaded,omitempty"`
	MasterVideoPath       string `json:"master_video_path,omitempty"`
	LastUploadResult      string `json:"last_upload_result,omitempty"`
	LastUploadAttemptAt   string `json:"last_upload_attempt_at,omitempty"`
	LastDriveUploadResult string `json:"last_drive_upload_result,omitempty"`
	RemoteStatus          string `json:"remote_status,omitempty"`
	ArtifactID            string `json:"artifact_id,omitempty"`
	OutputSHA256          string `json:"output_sha256,omitempty"`
	IdempotencyKey        string `json:"upload_idempotency_key,omitempty"`

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

// FileQueue implements a SQLite-backed job queue with a centralized transition service.
// All reads and writes go directly through SQLite — no in-memory cache.
type FileQueue struct {
	maxRetries int
	dbStore    *store.SQLiteStore
	ts         *TransitionService

	eventLogger func(jobID, eventType string, extra map[string]interface{})
}

// FileQueueConfig holds configuration for the file queue
type FileQueueConfig struct {
	MaxRetries int
	DBStore    *store.SQLiteStore
}

// NewFileQueue creates a new SQLite-backed queue
func NewFileQueue(cfg *FileQueueConfig) (*FileQueue, error) {
	if cfg.DBStore == nil {
		return nil, fmt.Errorf("SQLiteStore is required for FileQueue")
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}

	q := &FileQueue{
		maxRetries: cfg.MaxRetries,
		dbStore:    cfg.DBStore,
		ts:         NewTransitionService(cfg.DBStore),
	}

	return q, nil
}

// SetEventLogger sets a callback for job events
func (q *FileQueue) SetEventLogger(logger func(jobID, eventType string, extra map[string]interface{})) {
	q.eventLogger = logger
}

// logEvent logs a job event
func (q *FileQueue) logEvent(jobID, eventType string, extra map[string]interface{}) {
	if q.eventLogger != nil {
		q.eventLogger(jobID, eventType, extra)
	}
}

// GetNextJobID returns the next pending job ID directly from SQLite.
func (q *FileQueue) GetNextJobID(ctx context.Context) (string, error) {
	return q.ts.GetNextJobID(ctx)
}

// DeleteJob removes a job from the queue.
func (q *FileQueue) DeleteJob(ctx context.Context, jobID string) error {
	if err := q.ts.DeleteJob(ctx, jobID); err != nil {
		return err
	}
	q.logEvent(jobID, "deleted", nil)
	return nil
}

// SubmitJob adds a new job to the queue.
func (q *FileQueue) SubmitJob(ctx context.Context, jobID string, payload map[string]interface{}) error {
	job, err := q.ts.SubmitJob(ctx, jobID, payload, q.maxRetries)
	if err != nil {
		return err
	}
	q.logEvent(jobID, "created", map[string]interface{}{
		"project_id": job.ProjectID,
		"video_name": job.VideoName,
	})
	return nil
}

// ClaimNextJob atomically claims the next pending job from SQLite.
func (q *FileQueue) ClaimNextJob(ctx context.Context, workerID string, allowedJobTypes []string) (*Job, error) {
	job, err := q.ts.ClaimNextJob(ctx, workerID, allowedJobTypes)
	if err != nil {
		return nil, err
	}
	if job != nil {
		q.logEvent(job.JobID, "claimed", map[string]interface{}{
			"worker_id": workerID,
		})
	}
	return job, nil
}

// CompleteJob marks a job as completed (idempotent).
func (q *FileQueue) CompleteJob(ctx context.Context, jobID string) error {
	if err := q.ts.CompleteJob(ctx, jobID); err != nil {
		return err
	}
	q.logEvent(jobID, "completed", nil)
	return nil
}

// FailJob marks a job as failed, optionally requeueing for retry.
func (q *FileQueue) FailJob(ctx context.Context, jobID, errMsg, workerID string, requeue bool) error {
	if err := q.ts.FailJob(ctx, jobID, errMsg, workerID, requeue, q.maxRetries); err != nil {
		return err
	}
	q.logEvent(jobID, "failed", map[string]interface{}{
		"error":    errMsg,
		"requeued": requeue,
	})
	return nil
}

// LeaseJob claims a job for a worker from SQLite.
func (q *FileQueue) LeaseJob(ctx context.Context, jobID, workerID string) error {
	if err := q.ts.LeaseJob(ctx, jobID, workerID); err != nil {
		return err
	}
	q.logEvent(jobID, "claimed", map[string]interface{}{
		"worker_id": workerID,
	})
	return nil
}

// RenewJobLease extends the current lease for an active job.
func (q *FileQueue) RenewJobLease(ctx context.Context, jobID, workerID, leaseID string, leaseExpiry time.Time) error {
	return q.ts.RenewLease(ctx, jobID, workerID, leaseID, leaseExpiry)
}

// RequeueZombieJobs finds jobs with expired leases and requeues them.
func (q *FileQueue) RequeueZombieJobs(ctx context.Context, timeout time.Duration) (int, error) {
	return q.ts.RequeueZombieJobs(ctx, timeout)
}

// Query methods — all go directly to SQLite.

func (q *FileQueue) GetJob(ctx context.Context, jobID string) (*Job, error) {
	return q.ts.GetJob(ctx, jobID)
}

func (q *FileQueue) GetJobPayload(ctx context.Context, jobID string) (map[string]interface{}, error) {
	return q.ts.GetJobPayload(ctx, jobID)
}

func (q *FileQueue) GetJobAttempt(ctx context.Context, jobID string) (int, error) {
	return q.ts.GetJobAttempt(ctx, jobID)
}

func (q *FileQueue) GetJobsByStatus(ctx context.Context, status JobStatus) ([]*Job, error) {
	return q.ts.GetJobsByStatus(ctx, status)
}

func (q *FileQueue) GetPendingJobs(ctx context.Context) ([]*Job, error) {
	return q.ts.GetJobsByStatus(ctx, StatusPending)
}

func (q *FileQueue) GetProcessingJobs(ctx context.Context) ([]*Job, error) {
	return q.ts.GetJobsByStatus(ctx, StatusProcessing)
}

func (q *FileQueue) GetAllJobs(ctx context.Context) (map[string]*Job, error) {
	return q.ts.GetAllJobs(ctx)
}

func (q *FileQueue) Stats(ctx context.Context) (map[string]int64, error) {
	return q.ts.Stats(ctx)
}

func (q *FileQueue) GetJobAsMap(ctx context.Context, jobID string) (map[string]interface{}, error) {
	return q.ts.GetJobAsMap(ctx, jobID)
}

// UpdateJobFields updates specific fields of a job.
func (q *FileQueue) UpdateJobFields(ctx context.Context, jobID string, fields map[string]interface{}) error {
	return q.ts.UpdateJobFields(ctx, jobID, fields)
}

// UpdateJobLogs appends logs to a job.
func (q *FileQueue) UpdateJobLogs(ctx context.Context, jobID string, logs []JobLogEntry) error {
	return q.ts.UpdateJobLogs(ctx, jobID, logs)
}

// CleanupOldJobs removes completed/error jobs older than specified age from SQLite.
func (q *FileQueue) CleanupOldJobs(ctx context.Context, age time.Duration) (int, error) {
	cutoff := time.Now().Add(-age)
	count, err := q.dbStore.ArchiveOldJobs(cutoff)
	if err != nil {
		return 0, err
	}
	log.Printf("[CLEANUP] Cleaned up %d old jobs from SQLite", count)
	return int(count), nil
}

// GetDBStore returns the underlying SQLite store.
func (q *FileQueue) GetDBStore() *store.SQLiteStore {
	return q.dbStore
}

// TransitionService returns the transition service for direct use.
func (q *FileQueue) TransitionService() *TransitionService {
	return q.ts
}
