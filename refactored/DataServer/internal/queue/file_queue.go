// Package queue provides job queue management with SQLite persistence
package queue

import (
	"context"
	"encoding/json"
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
	CreatedAt    interface{} `json:"created_at,omitempty"` // Can be float64 (unix) or string (ISO)
	UpdatedAt    interface{} `json:"updated_at,omitempty"`
	StartedAt    interface{} `json:"started_at,omitempty"`
	CompletedAt  interface{} `json:"completed_at,omitempty"`
	AssignedAt   interface{} `json:"assigned_at,omitempty"`
	ProcessingAt interface{} `json:"processing_at,omitempty"`

	// Worker assignment
	AssignedTo       string `json:"assigned_to,omitempty"`
	AssignedWorkerIP string `json:"assigned_worker_ip,omitempty"`
	WorkerName       string `json:"worker_name,omitempty"`
	ClaimedBy        string `json:"claimed_by,omitempty"`
	ClaimedAt        string `json:"claimed_at,omitempty"`

	// Retry tracking
	RetryCount int `json:"retry_count,omitempty"`
	MaxRetries int `json:"max_retries,omitempty"`

	// Error tracking
	LastError    string      `json:"last_error,omitempty"`
	LastErrorAt  interface{} `json:"last_error_at,omitempty"`
	ErrorMessage string      `json:"error_message,omitempty"`
	FailedAt     interface{} `json:"failed_at,omitempty"`
	FailedBy     string      `json:"failed_by,omitempty"`

	// History of status changes
	History []JobHistoryEntry `json:"history,omitempty"`

	// Logs from worker
	Logs          []JobLogEntry `json:"logs,omitempty"`
	LogsUpdatedAt string        `json:"logs_updated_at,omitempty"`

	// Video configuration
	SlotData      map[string]interface{} `json:"slot_data,omitempty"`
	OutputVideoID string                 `json:"output_video_id,omitempty"`
	DriveURL      string                 `json:"drive_url,omitempty"`

	// Upload tracking
	VideoUploaded         bool   `json:"video_uploaded,omitempty"`
	MasterVideoPath       string `json:"master_video_path,omitempty"`
	LastUploadResult      string `json:"last_upload_result,omitempty"`
	LastUploadAttemptAt   string `json:"last_upload_attempt_at,omitempty"`
	LastDriveUploadResult string `json:"last_drive_upload_result,omitempty"`
	RemoteStatus          string `json:"remote_status,omitempty"`

	// Deduplication
	JobFingerprint string `json:"job_fingerprint,omitempty"`

	// Source tracking
	SubmittedVia string `json:"submitted_via,omitempty"`
	LastActivity string `json:"last_activity,omitempty"`
	RunID        string `json:"run_id,omitempty"`

	// Raw payload for flexibility
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

// FileQueue implements a SQLite-backed job queue (JSON file legacy removed)
type FileQueue struct {
	mu         sync.RWMutex
	maxRetries int

	// In-memory cache of ACTIVE jobs only (PENDING/PROCESSING)
	// Completed/Error jobs are NOT cached - query SQLite directly
	activeJobs map[string]*Job

	// Event logger
	eventLogger func(jobID, eventType string, extra map[string]interface{})

	// SQLite is the PRIMARY source of truth
	dbStore *store.SQLiteStore
}

// FileQueueConfig holds configuration for the file queue
type FileQueueConfig struct {
	// FilePath is DEPRECATED - kept for config compatibility
	FilePath   string
	MaxRetries int
	CacheTTL   time.Duration // DEPRECATED - no longer needed
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

	// Load ONLY active jobs into memory (PENDING, PROCESSING, QUEUED, ASSIGNED, LEASED)
	// Using exported LoadActiveJobs from store_io module
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

// SubmitJob adds a new job to the queue
func (q *FileQueue) SubmitJob(ctx context.Context, jobID string, payload map[string]interface{}) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := NowUnix()
	nowISOVal := NowISO()

	job := &Job{
		JobID:      jobID,
		Status:     StatusPending,
		CreatedAt:  now,
		UpdatedAt:  now,
		RetryCount: 0,
		MaxRetries: q.maxRetries,
		History: []JobHistoryEntry{{
			Status:    "PENDING",
			Timestamp: nowISOVal,
			Message:   "Job created",
		}},
		Payload: payload,
	}

	// Copy known fields from payload
	if s, ok := payload["video_name"].(string); ok {
		job.VideoName = s
	}
	if s, ok := payload["project_id"].(string); ok {
		job.ProjectID = s
	}
	if s, ok := payload["project_name"].(string); ok && job.ProjectID == "" {
		job.ProjectID = s
	}
	if s, ok := payload["youtube_group"].(string); ok && job.ProjectID == "" {
		job.ProjectID = s
	}
	if s, ok := payload["output_video_id"].(string); ok {
		job.OutputVideoID = s
	}
	if s, ok := payload["job_fingerprint"].(string); ok {
		job.JobFingerprint = s
	}
	if s, ok := payload["job_run_id"].(string); ok && s != "" {
		job.RunID = s
	} else if s, ok := payload["run_id"].(string); ok && s != "" {
		job.RunID = s
	}
	if m, ok := payload["slot_data"].(map[string]interface{}); ok {
		job.SlotData = m
	}

	// Persist to SQLite (primary source) - using store_io module
	if err := PersistJob(job, q.dbStore); err != nil {
		return err
	}

	// Add to active cache
	q.activeJobs[jobID] = job

	q.logEvent(jobID, "created", map[string]interface{}{
		"project_id": job.ProjectID,
		"video_name": job.VideoName,
	})

	return nil
}

// GetNextJobID returns the next pending job ID
func (q *FileQueue) GetNextJobID(ctx context.Context) (string, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	// Find first pending job without a claim
	for id, job := range q.activeJobs {
		if job.Status == StatusPending && job.ClaimedBy == "" {
			return id, nil
		}
	}

	return "", nil
}

// ClaimNextJob atomically claims the next pending job for a worker.
func (q *FileQueue) ClaimNextJob(ctx context.Context, workerID string) (*Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	rawJSON, ok, err := q.dbStore.ClaimNextPendingJob(workerID, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	var payload map[string]any
	if err := json.Unmarshal(rawJSON, &payload); err != nil {
		return nil, fmt.Errorf("failed to decode claimed job: %w", err)
	}

	job := MapToJob(payload)
	q.activeJobs[job.JobID] = job
	q.logEvent(job.JobID, "claimed", map[string]interface{}{
		"worker_id": workerID,
	})
	return job, nil
}

// LeaseJob claims a job for a worker with atomic check-and-set.
// The in-memory lock (q.mu) protects the entire check-update-persist sequence,
// preventing double-claim races even when the job is not yet in the active cache.
func (q *FileQueue) LeaseJob(ctx context.Context, jobID, workerID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, ok := q.activeJobs[jobID]
	if !ok {
		// Check if job exists in SQLite
		m, err := q.dbStore.GetJob(ctx, jobID)
		if err != nil {
			return fmt.Errorf("job not found: %s", jobID)
		}
		job = MapToJob(m)
		// Add to active cache if it's an active status
		if job.Status == StatusPending || job.Status == StatusProcessing {
			q.activeJobs[jobID] = job
		}
	}
	if job.Status != StatusPending {
		return fmt.Errorf("job %s is not pending", jobID)
	}
	if job.ClaimedBy != "" || job.AssignedTo != "" {
		return fmt.Errorf("job %s already claimed by %s", jobID, job.ClaimedBy)
	}

	now := NowUnix()
	nowISOVal := NowISO()

	job.Status = StatusProcessing
	job.AssignedTo = workerID
	job.AssignedAt = nowISOVal
	job.ClaimedBy = workerID
	job.ClaimedAt = nowISOVal
	job.UpdatedAt = now
	job.RetryCount++

	job.History = append(job.History, JobHistoryEntry{
		Status:    "PROCESSING",
		Timestamp: nowISOVal,
		WorkerID:  workerID,
		Message:   fmt.Sprintf("Job assigned to worker %s", workerID),
	})

	// Persist to SQLite BEFORE releasing the lock - atomic check-and-set
	if err := PersistJob(job, q.dbStore); err != nil {
		return err
	}

	q.logEvent(jobID, "claimed", map[string]interface{}{
		"worker_id": workerID,
	})

	return nil
}

// CompleteJob marks a job as completed
func (q *FileQueue) CompleteJob(ctx context.Context, jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, ok := q.activeJobs[jobID]
	if !ok {
		return fmt.Errorf("job not found: %s", jobID)
	}

	now := NowUnix()
	nowISOVal := NowISO()

	job.Status = StatusCompleted
	job.CompletedAt = nowISOVal
	job.UpdatedAt = now

	job.History = append(job.History, JobHistoryEntry{
		Status:    "COMPLETED",
		Timestamp: nowISOVal,
		WorkerID:  job.AssignedTo,
		Message:   "Job completed successfully",
	})

	// Persist to SQLite - using store_io module
	if err := PersistJob(job, q.dbStore); err != nil {
		return err
	}

	// Remove from active cache (completed jobs don't need to be in memory)
	delete(q.activeJobs, jobID)

	q.logEvent(jobID, "completed", map[string]interface{}{
		"worker_id": job.AssignedTo,
	})

	return nil
}

// FailJob marks a job as failed, optionally requeueing for retry
func (q *FileQueue) FailJob(ctx context.Context, jobID, errMsg string, requeue bool) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, ok := q.activeJobs[jobID]
	if !ok {
		return fmt.Errorf("job not found: %s", jobID)
	}

	now := NowUnix()
	nowISOVal := NowISO()
	workerID := job.AssignedTo

	if requeue && job.RetryCount < job.MaxRetries {
		// Requeue for retry
		job.Status = StatusPending
		job.LastError = errMsg
		job.LastErrorAt = now
		job.AssignedTo = ""
		job.AssignedAt = nil
		job.ClaimedBy = ""
		job.ClaimedAt = ""
		job.ProcessingAt = nil

		job.History = append(job.History, JobHistoryEntry{
			Status:    "PENDING",
			Timestamp: nowISOVal,
			WorkerID:  workerID,
			Message:   fmt.Sprintf("Job requeued after failure: %s", errMsg),
		})

		// Persist and keep in active cache
		if err := PersistJob(job, q.dbStore); err != nil {
			return err
		}
	} else {
		// Mark as error (dead)
		job.Status = StatusError
		job.ErrorMessage = errMsg
		job.LastError = errMsg
		job.LastErrorAt = now
		job.FailedAt = nowISOVal
		job.FailedBy = workerID

		job.History = append(job.History, JobHistoryEntry{
			Status:    "ERROR",
			Timestamp: nowISOVal,
			WorkerID:  workerID,
			Message:   fmt.Sprintf("Job failed: %s", errMsg),
		})

		// Persist to SQLite
		if err := PersistJob(job, q.dbStore); err != nil {
			return err
		}

		// Remove from active cache
		delete(q.activeJobs, jobID)
	}

	job.UpdatedAt = now

	q.logEvent(jobID, "failed", map[string]interface{}{
		"worker_id": workerID,
		"error":     errMsg,
		"requeued":  requeue && job.RetryCount < job.MaxRetries,
	})

	return nil
}

// DeleteJob removes a job from the queue
func (q *FileQueue) DeleteJob(ctx context.Context, jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Remove from SQLite
	if err := q.dbStore.DeleteJob(jobID); err != nil {
		return err
	}

	// Remove from active cache
	delete(q.activeJobs, jobID)

	q.logEvent(jobID, "deleted", nil)

	return nil
}

// ============== Projection Methods (delegated to job_projection.go) ==============

// GetJob retrieves a job by ID
func (q *FileQueue) GetJob(ctx context.Context, jobID string) (*Job, error) {
	return GetJob(ctx, jobID, q.dbStore, q.activeJobs)
}

// GetJobPayload returns the job payload
func (q *FileQueue) GetJobPayload(ctx context.Context, jobID string) (map[string]interface{}, error) {
	return GetJobPayload(ctx, jobID, q.dbStore, q.activeJobs)
}

// GetJobAttempt returns the current retry count
func (q *FileQueue) GetJobAttempt(ctx context.Context, jobID string) (int, error) {
	return GetJobAttempt(ctx, jobID, q.dbStore, q.activeJobs)
}

// GetJobsByStatus returns all jobs with a given status
func (q *FileQueue) GetJobsByStatus(ctx context.Context, status JobStatus) ([]*Job, error) {
	return GetJobsByStatus(ctx, status, q.dbStore, q.activeJobs)
}

// GetPendingJobs returns all pending jobs
func (q *FileQueue) GetPendingJobs(ctx context.Context) ([]*Job, error) {
	return GetPendingJobs(ctx, q.dbStore, q.activeJobs)
}

// GetProcessingJobs returns all processing jobs
func (q *FileQueue) GetProcessingJobs(ctx context.Context) ([]*Job, error) {
	return GetProcessingJobs(ctx, q.dbStore, q.activeJobs)
}

// GetAllJobs returns all jobs (limited to recent active + query for historical)
func (q *FileQueue) GetAllJobs(ctx context.Context) (map[string]*Job, error) {
	return GetAllJobs(ctx, q.activeJobs)
}

// Stats returns queue statistics (uses SQLite for accurate counts)
func (q *FileQueue) Stats(ctx context.Context) (map[string]int64, error) {
	return Stats(ctx, q.dbStore)
}

// GetJobAsMap returns a job as a map for flexible field access
func (q *FileQueue) GetJobAsMap(ctx context.Context, jobID string) (map[string]interface{}, error) {
	return GetJobAsMap(ctx, jobID, q.dbStore, q.activeJobs)
}

// ============== Merge/Patch Methods (delegated to job_merge.go) ==============

// UpdateJobFields updates specific fields of a job
func (q *FileQueue) UpdateJobFields(ctx context.Context, jobID string, fields map[string]interface{}) error {
	return UpdateJobFields(ctx, jobID, fields, q.dbStore, q.activeJobs)
}

// UpdateJobLogs appends logs to a job
func (q *FileQueue) UpdateJobLogs(ctx context.Context, jobID string, logs []JobLogEntry) error {
	return UpdateJobLogs(ctx, jobID, logs, q.dbStore, q.activeJobs)
}

// ============== Zombie Detection Types ==============

// ZombieState represents the current state of a potential zombie job
type ZombieState struct {
	JobID             string        `json:"job_id"`
	DetectedAt        time.Time     `json:"detected_at"`
	LastActivity      time.Time     `json:"last_activity"`
	WorkerID          string        `json:"worker_id"`
	WorkerLastSeen    time.Time     `json:"worker_last_seen,omitempty"`
	Age               time.Duration `json:"age"`
	HasRecentLogs     bool          `json:"has_recent_logs"`
	LogsLastUpdated   time.Time     `json:"logs_last_updated,omitempty"`
	WarningLevel      int           `json:"warning_level"` // 0=none, 1=warning, 2=critical, 3=zombie
	ConsecutiveChecks int           `json:"consecutive_checks"`
	Resolution        string        `json:"resolution,omitempty"` // "requeued", "forced_complete", "dlq"
}

// ZombieHandlerConfig holds configuration for zombie detection
type ZombieHandlerConfig struct {
	WarnThreshold      time.Duration `json:"warn_threshold"`       // Time before warning
	CriticalThreshold  time.Duration `json:"critical_threshold"`   // Time before critical
	ZombieThreshold    time.Duration `json:"zombie_threshold"`     // Time before declaring zombie
	MinConsecutiveHits int           `json:"min_consecutive_hits"` // Must fail N checks before action
	CheckInterval      time.Duration `json:"check_interval"`
	WorkerOfflineGrace time.Duration `json:"worker_offline_grace"` // Extra time if worker is offline
	MaxZombieAge       time.Duration `json:"max_zombie_age"`       // Max age before DLQ
}

// DefaultZombieHandlerConfig returns sensible defaults
func DefaultZombieHandlerConfig() *ZombieHandlerConfig {
	return &ZombieHandlerConfig{
		WarnThreshold:      10 * time.Minute,
		CriticalThreshold:  20 * time.Minute,
		ZombieThreshold:    30 * time.Minute,
		MinConsecutiveHits: 2,
		CheckInterval:      1 * time.Minute,
		WorkerOfflineGrace: 10 * time.Minute,
		MaxZombieAge:       2 * time.Hour,
	}
}

// ZombieTracker tracks zombie detection state
type ZombieTracker struct {
	mu          sync.RWMutex
	states      map[string]*ZombieState
	config      *ZombieHandlerConfig
	workerState func(workerID string) (lastSeen time.Time, isOnline bool)
	onZombie    func(state *ZombieState, job *Job)
}

// NewZombieTracker creates a new zombie tracker
func NewZombieTracker(cfg *ZombieHandlerConfig) *ZombieTracker {
	if cfg == nil {
		cfg = DefaultZombieHandlerConfig()
	}
	return &ZombieTracker{
		states: make(map[string]*ZombieState),
		config: cfg,
	}
}

// SetWorkerStateProvider sets the callback for checking worker state
func (zt *ZombieTracker) SetWorkerStateProvider(provider func(workerID string) (lastSeen time.Time, isOnline bool)) {
	zt.workerState = provider
}

// SetOnZombieCallback sets the callback when a zombie is confirmed
func (zt *ZombieTracker) SetOnZombieCallback(cb func(state *ZombieState, job *Job)) {
	zt.onZombie = cb
}

// CheckJob evaluates a job for zombie state
func (zt *ZombieTracker) CheckJob(job *Job) *ZombieState {
	zt.mu.Lock()
	defer zt.mu.Unlock()

	if job.Status != StatusProcessing {
		// Clear any existing state
		delete(zt.states, job.JobID)
		return nil
	}

	now := time.Now()

	// Get or create state
	state, exists := zt.states[job.JobID]
	if !exists {
		state = &ZombieState{
			JobID:        job.JobID,
			DetectedAt:   now,
			WorkerID:     job.AssignedTo,
			WarningLevel: 0,
		}
		zt.states[job.JobID] = state
	}

	// Parse activity time
	var lastActivity time.Time
	switch v := job.AssignedAt.(type) {
	case string:
		lastActivity, _ = time.Parse(time.RFC3339, v)
	case float64:
		lastActivity = time.Unix(int64(v), 0)
	}

	// Check logs for recent activity
	var logsUpdated time.Time
	if job.LogsUpdatedAt != "" {
		logsUpdated, _ = time.Parse(time.RFC3339, job.LogsUpdatedAt)
		state.HasRecentLogs = now.Sub(logsUpdated) < zt.config.WarnThreshold
		state.LogsLastUpdated = logsUpdated
		if logsUpdated.After(lastActivity) {
			lastActivity = logsUpdated
		}
	}

	state.LastActivity = lastActivity
	state.Age = now.Sub(lastActivity)

	// Check worker status if available.
	// Important policy: do not auto-mark zombie while the assigned worker is online.
	// Long-running jobs can legitimately exceed fixed thresholds.
	var workerGrace time.Duration
	if zt.workerState != nil && job.AssignedTo != "" {
		workerLastSeen, isOnline := zt.workerState(job.AssignedTo)
		state.WorkerLastSeen = workerLastSeen
		if isOnline {
			state.WarningLevel = 0
			state.ConsecutiveChecks = 0
			return nil
		}
		if !isOnline {
			workerGrace = zt.config.WorkerOfflineGrace
		}
	}

	// Determine warning level with grace period
	effectiveAge := state.Age - workerGrace

	prevLevel := state.WarningLevel
	switch {
	case effectiveAge >= zt.config.ZombieThreshold:
		state.WarningLevel = 3
	case effectiveAge >= zt.config.CriticalThreshold:
		state.WarningLevel = 2
	case effectiveAge >= zt.config.WarnThreshold:
		state.WarningLevel = 1
	default:
		state.WarningLevel = 0
		state.ConsecutiveChecks = 0
	}

	// Increment consecutive checks for non-normal states
	if state.WarningLevel > 0 {
		state.ConsecutiveChecks++
	}

	// Check for resolution conditions
	if state.WarningLevel == 3 && state.ConsecutiveChecks >= zt.config.MinConsecutiveHits {
		// Job is a confirmed zombie
		return state
	}

	// Log state changes
	if prevLevel != state.WarningLevel && state.WarningLevel > 0 {
		levelNames := []string{"normal", "warning", "critical", "zombie"}
		log.Printf("🧟 Job %s zombie state: %s (age: %v, consecutive: %d)",
			job.JobID[:8], levelNames[state.WarningLevel], state.Age.Round(time.Second), state.ConsecutiveChecks)
	}

	return nil // Not yet a confirmed zombie
}

// ClearState clears zombie tracking state for a job
func (zt *ZombieTracker) ClearState(jobID string) {
	zt.mu.Lock()
	defer zt.mu.Unlock()
	delete(zt.states, jobID)
}

// GetState returns the current zombie state for a job
func (zt *ZombieTracker) GetState(jobID string) *ZombieState {
	zt.mu.RLock()
	defer zt.mu.RUnlock()
	if state, ok := zt.states[jobID]; ok {
		return state
	}
	return nil
}

// GetAllZombieStates returns all current zombie states
func (zt *ZombieTracker) GetAllZombieStates() []*ZombieState {
	zt.mu.RLock()
	defer zt.mu.RUnlock()

	states := make([]*ZombieState, 0, len(zt.states))
	for _, state := range zt.states {
		states = append(states, state)
	}
	return states
}

// Stats returns zombie tracking statistics
func (zt *ZombieTracker) Stats() map[string]interface{} {
	zt.mu.RLock()
	defer zt.mu.RUnlock()

	stats := map[string]interface{}{
		"tracked_jobs": len(zt.states),
		"by_level":     map[string]int{"warning": 0, "critical": 0, "zombie": 0},
	}

	for _, state := range zt.states {
		switch state.WarningLevel {
		case 1:
			stats["by_level"].(map[string]int)["warning"]++
		case 2:
			stats["by_level"].(map[string]int)["critical"]++
		case 3:
			stats["by_level"].(map[string]int)["zombie"]++
		}
	}

	return stats
}

// ============== ZombieAwareFileQueue ==============

// ZombieAwareFileQueue extends FileQueue with advanced zombie handling
type ZombieAwareFileQueue struct {
	*FileQueue
	zombieTracker *ZombieTracker
	dlq           *DeadLetterQueue
	notifyChan    chan ZombieState
}

// NewZombieAwareFileQueue creates a queue with zombie handling
func NewZombieAwareFileQueue(cfg *FileQueueConfig, zombieCfg *ZombieHandlerConfig, dlq *DeadLetterQueue) (*ZombieAwareFileQueue, error) {
	fq, err := NewFileQueue(cfg)
	if err != nil {
		return nil, err
	}

	if zombieCfg == nil {
		zombieCfg = DefaultZombieHandlerConfig()
	}

	return &ZombieAwareFileQueue{
		FileQueue:     fq,
		zombieTracker: NewZombieTracker(zombieCfg),
		dlq:           dlq,
		notifyChan:    make(chan ZombieState, 50),
	}, nil
}

// SetWorkerStateProvider forwards to zombie tracker
func (zaq *ZombieAwareFileQueue) SetWorkerStateProvider(provider func(workerID string) (lastSeen time.Time, isOnline bool)) {
	zaq.zombieTracker.SetWorkerStateProvider(provider)
}

// ZombieCheckLoop runs periodic zombie detection
func (zaq *ZombieAwareFileQueue) ZombieCheckLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			zaq.checkForZombies(ctx)
		case state := <-zaq.notifyChan:
			zaq.handleZombieNotification(ctx, state)
		}
	}
}

// checkForZombies scans all processing jobs for zombie state
func (zaq *ZombieAwareFileQueue) checkForZombies(ctx context.Context) {
	jobs, err := zaq.GetProcessingJobs(ctx)
	if err != nil {
		log.Printf("⚠️ Zombie check failed to load jobs: %v", err)
		return
	}

	for _, job := range jobs {
		if zombieState := zaq.zombieTracker.CheckJob(job); zombieState != nil {
			// Confirmed zombie - queue for handling
			select {
			case zaq.notifyChan <- *zombieState:
			default:
				// Channel full, handle synchronously
				zaq.handleZombieNotification(ctx, *zombieState)
			}
		}
	}
}

// handleZombieNotification handles a confirmed zombie job
func (zaq *ZombieAwareFileQueue) handleZombieNotification(ctx context.Context, state ZombieState) {
	job, err := zaq.GetJob(ctx, state.JobID)
	if err != nil {
		zaq.zombieTracker.ClearState(state.JobID)
		return
	}

	// Check if job was already resolved
	if job.Status != StatusProcessing {
		zaq.zombieTracker.ClearState(state.JobID)
		return
	}

	// Determine resolution based on age and retry count
	var resolution string
	maxAge := zaq.zombieTracker.config.MaxZombieAge

	if state.Age > maxAge || job.RetryCount >= job.MaxRetries {
		// Move to DLQ
		resolution = "dlq"
		if zaq.dlq != nil {
			zaq.dlq.AddJob(ctx, job, "zombie_timeout", fmt.Sprintf("No activity for %v", state.Age))
		}
		zaq.FileQueue.DeleteJob(ctx, state.JobID)
	} else {
		// Requeue for retry
		resolution = "requeued"
		zaq.requeueZombieJob(ctx, job, state)
	}

	state.Resolution = resolution
	zaq.zombieTracker.ClearState(state.JobID)

	log.Printf("🧟 Job %s resolved as zombie: %s (age: %v)", state.JobID[:8], resolution, state.Age.Round(time.Second))
}

// requeueZombieJob requeues a zombie job with proper tracking
func (zaq *ZombieAwareFileQueue) requeueZombieJob(ctx context.Context, job *Job, state ZombieState) error {
	zaq.mu.Lock()
	defer zaq.mu.Unlock()

	now := NowUnix()
	nowISOVal := NowISO()

	job.Status = StatusPending
	job.LastError = fmt.Sprintf("Zombie requeue: no activity for %v (worker: %s)", state.Age, state.WorkerID)
	job.LastErrorAt = now
	job.AssignedTo = ""
	job.AssignedAt = nil
	job.ClaimedBy = ""
	job.ClaimedAt = ""
	job.ProcessingAt = nil
	job.RetryCount++
	job.UpdatedAt = now

	job.History = append(job.History, JobHistoryEntry{
		Status:    "PENDING",
		Timestamp: nowISOVal,
		Message:   fmt.Sprintf("Requeued after zombie detection (attempt %d)", job.RetryCount),
	})

	if err := PersistJob(job, zaq.dbStore); err != nil {
		return err
	}

	zaq.logEvent(job.JobID, "zombie_requeued", map[string]interface{}{
		"worker_id":   state.WorkerID,
		"age":         state.Age.String(),
		"retry_count": job.RetryCount,
	})

	return nil
}

// GetZombieStats returns zombie tracking statistics
func (zaq *ZombieAwareFileQueue) GetZombieStats() map[string]interface{} {
	return zaq.zombieTracker.Stats()
}

// GetZombieStates returns all current zombie states
func (zaq *ZombieAwareFileQueue) GetZombieStates() []*ZombieState {
	return zaq.zombieTracker.GetAllZombieStates()
}

// RequeueZombieJobs finds jobs with expired leases and requeues them
func (q *FileQueue) RequeueZombieJobs(ctx context.Context, timeout time.Duration) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now()
	requeued := 0

	for id, job := range q.activeJobs {
		if job.Status != StatusProcessing {
			continue
		}

		// Check if lease has expired
		var assignedTime time.Time
		switch v := job.AssignedAt.(type) {
		case string:
			assignedTime, _ = time.Parse(time.RFC3339, v)
		case float64:
			assignedTime = time.Unix(int64(v), 0)
		}

		if assignedTime.IsZero() {
			continue
		}

		if now.Sub(assignedTime) > timeout {
			// Zombie job found
			nowISOVal := NowISO()
			job.Status = StatusPending
			job.LastError = fmt.Sprintf("Zombie: no heartbeat for %v", now.Sub(assignedTime))
			job.LastErrorAt = now.Unix()
			job.AssignedTo = ""
			job.AssignedAt = nil
			job.ClaimedBy = ""
			job.ClaimedAt = ""
			job.RetryCount++

			job.History = append(job.History, JobHistoryEntry{
				Status:    "PENDING",
				Timestamp: nowISOVal,
				Message:   "Requeued after zombie timeout",
			})

			if err := PersistJob(job, q.dbStore); err != nil {
				log.Printf("⚠️ Failed to persist zombie requeue for %s: %v", id, err)
				continue
			}

			requeued++
		}
	}

	return requeued, nil
}

// CleanupOldJobs removes completed/error jobs older than specified age from SQLite
func (q *FileQueue) CleanupOldJobs(ctx context.Context, age time.Duration) (int, error) {
	cutoff := time.Now().Add(-age)
	count, err := q.dbStore.ArchiveOldJobs(cutoff)
	if err != nil {
		return 0, err
	}

	log.Printf("🧹 Cleaned up %d old jobs from SQLite", count)
	return int(count), nil
}

// GetDBStore returns the underlying SQLite store (for direct queries if needed)
func (q *FileQueue) GetDBStore() *store.SQLiteStore {
	return q.dbStore
}
