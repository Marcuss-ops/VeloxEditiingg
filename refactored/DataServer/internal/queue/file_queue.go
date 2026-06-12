// Package queue provides job queue management with SQLite persistence
package queue

import (
	"context"
	"fmt"
	"log"
	"sync"
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

	RetryCount int `json:"retry_count,omitempty"`
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

// FileQueue implements a SQLite-backed job queue
type FileQueue struct {
	mu         sync.RWMutex
	maxRetries int

	activeJobs map[string]*Job

	eventLogger func(jobID, eventType string, extra map[string]interface{})

	dbStore *store.SQLiteStore
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
		activeJobs: make(map[string]*Job),
	}

	activeJobs, err := LoadActiveJobs(q.dbStore)
	if err != nil {
		return nil, fmt.Errorf("failed to load active jobs from SQLite: %w", err)
	}
	q.activeJobs = activeJobs

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

// GetNextJobID returns the next pending job ID
func (q *FileQueue) GetNextJobID(ctx context.Context) (string, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	for id, job := range q.activeJobs {
		if job.Status == StatusPending && job.ClaimedBy == "" {
			return id, nil
		}
	}

	return "", nil
}

// DeleteJob removes a job from the queue
func (q *FileQueue) DeleteJob(ctx context.Context, jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if err := q.dbStore.DeleteJob(jobID); err != nil {
		return err
	}

	delete(q.activeJobs, jobID)

	q.logEvent(jobID, "deleted", nil)

	return nil
}

// ============== Projection Methods (delegated to job_projection.go) ==============

func (q *FileQueue) GetJob(ctx context.Context, jobID string) (*Job, error) {
	return GetJob(ctx, jobID, q.dbStore, q.activeJobs)
}

func (q *FileQueue) GetJobPayload(ctx context.Context, jobID string) (map[string]interface{}, error) {
	return GetJobPayload(ctx, jobID, q.dbStore, q.activeJobs)
}

func (q *FileQueue) GetJobAttempt(ctx context.Context, jobID string) (int, error) {
	return GetJobAttempt(ctx, jobID, q.dbStore, q.activeJobs)
}

func (q *FileQueue) GetJobsByStatus(ctx context.Context, status JobStatus) ([]*Job, error) {
	return GetJobsByStatus(ctx, status, q.dbStore, q.activeJobs)
}

func (q *FileQueue) GetPendingJobs(ctx context.Context) ([]*Job, error) {
	return GetPendingJobs(ctx, q.dbStore, q.activeJobs)
}

func (q *FileQueue) GetProcessingJobs(ctx context.Context) ([]*Job, error) {
	return GetProcessingJobs(ctx, q.dbStore, q.activeJobs)
}

func (q *FileQueue) GetAllJobs(ctx context.Context) (map[string]*Job, error) {
	return GetAllJobs(ctx, q.activeJobs)
}

func (q *FileQueue) Stats(ctx context.Context) (map[string]int64, error) {
	return Stats(ctx, q.dbStore)
}

func (q *FileQueue) GetJobAsMap(ctx context.Context, jobID string) (map[string]interface{}, error) {
	return GetJobAsMap(ctx, jobID, q.dbStore, q.activeJobs)
}

// ============== Merge/Patch Methods (delegated to job_merge.go) ==============

func (q *FileQueue) UpdateJobFields(ctx context.Context, jobID string, fields map[string]interface{}) error {
	return UpdateJobFields(ctx, jobID, fields, q.dbStore, q.activeJobs)
}

func (q *FileQueue) UpdateJobLogs(ctx context.Context, jobID string, logs []JobLogEntry) error {
	return UpdateJobLogs(ctx, jobID, logs, q.dbStore, q.activeJobs)
}

// CleanupOldJobs removes completed/error jobs older than specified age from SQLite
func (q *FileQueue) CleanupOldJobs(ctx context.Context, age time.Duration) (int, error) {
	cutoff := time.Now().Add(-age)
	count, err := q.dbStore.ArchiveOldJobs(cutoff)
	if err != nil {
		return 0, err
	}

	log.Printf("[CLEANUP] Cleaned up %d old jobs from SQLite", count)
	return int(count), nil
}

// GetDBStore returns the underlying SQLite store
func (q *FileQueue) GetDBStore() *store.SQLiteStore {
	return q.dbStore
}
