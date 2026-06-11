package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// DeadLetterJob represents a job that has failed and been moved to the DLQ
type DeadLetterJob struct {
	JobID        string                 `json:"job_id"`
	OriginalJob  *Job                   `json:"original_job"`
	FailedAt     time.Time              `json:"failed_at"`
	FailReason   string                 `json:"fail_reason"`
	FailCount    int                    `json:"fail_count"`
	LastWorkerID string                 `json:"last_worker_id"`
	History      []FailHistoryEntry     `json:"history,omitempty"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
	DeadAt       time.Time              `json:"dead_at"`     // When moved to DLQ
	DeadReason   string                 `json:"dead_reason"` // Why moved to DLQ (max_retries, manual, etc.)
	Replayable   bool                   `json:"replayable"`  // Can be replayed
}

// FailHistoryEntry tracks each failure attempt
type FailHistoryEntry struct {
	Timestamp time.Time `json:"timestamp"`
	WorkerID  string    `json:"worker_id"`
	Error     string    `json:"error"`
	Attempt   int       `json:"attempt"`
}

// DLQConfig holds configuration for the Dead Letter Queue
type DLQConfig struct {
	FilePath       string        `json:"file_path"`
	MaxAge         time.Duration `json:"max_age"`         // How long to keep dead jobs
	MaxItems       int           `json:"max_items"`       // Maximum items in DLQ
	AutoReplay     bool          `json:"auto_replay"`     // Auto-replay replayable jobs
	ReplayDelay    time.Duration `json:"replay_delay"`    // Delay before auto-replay
	AlertThreshold int           `json:"alert_threshold"` // Alert when DLQ size exceeds this
}

// DefaultDLQConfig returns sensible defaults
func DefaultDLQConfig(dataDir string) *DLQConfig {
	return &DLQConfig{
		FilePath:       filepath.Join(dataDir, "jobs", "dead_letter_queue.json"),
		MaxAge:         7 * 24 * time.Hour, // 7 days
		MaxItems:       10000,
		AutoReplay:     false,
		ReplayDelay:    5 * time.Minute,
		AlertThreshold: 100,
	}
}

// DeadLetterQueue manages failed jobs
type DeadLetterQueue struct {
	mu       sync.RWMutex
	config   *DLQConfig
	jobs     map[string]*DeadLetterJob
	filePath string
	onAlert  func(alertType string, data map[string]interface{})
}

// NewDeadLetterQueue creates a new DLQ
func NewDeadLetterQueue(cfg *DLQConfig) (*DeadLetterQueue, error) {
	if cfg == nil {
		return nil, fmt.Errorf("DLQ config is required")
	}

	dlq := &DeadLetterQueue{
		config:   cfg,
		jobs:     make(map[string]*DeadLetterJob),
		filePath: cfg.FilePath,
	}

	// Ensure directory exists
	dir := filepath.Dir(cfg.FilePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create DLQ directory: %w", err)
	}

	// Load existing jobs
	if err := dlq.load(); err != nil {
		log.Printf("[WARN] DLQ load error (starting fresh): %v", err)
	}

	return dlq, nil
}

// load reads DLQ from file
func (dlq *DeadLetterQueue) load() error {
	data, err := os.ReadFile(dlq.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	if len(data) == 0 {
		return nil
	}

	return json.Unmarshal(data, &dlq.jobs)
}

// save writes DLQ to file atomically
func (dlq *DeadLetterQueue) save() error {
	data, err := json.MarshalIndent(dlq.jobs, "", "  ")
	if err != nil {
		return err
	}

	tmpPath := dlq.filePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}

	return os.Rename(tmpPath, dlq.filePath)
}

// AddJob adds a failed job to the DLQ
func (dlq *DeadLetterQueue) AddJob(ctx context.Context, job *Job, reason, error string) error {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	now := time.Now().UTC()

	// Check if already exists (update)
	existing, exists := dlq.jobs[job.JobID]

	dl := &DeadLetterJob{
		JobID:        job.JobID,
		OriginalJob:  job,
		FailedAt:     now,
		FailReason:   error,
		FailCount:    job.RetryCount,
		LastWorkerID: job.AssignedTo,
		DeadAt:       now,
		DeadReason:   reason,
		Replayable:   dlq.isReplayable(job, reason),
	}

	if exists {
		// Preserve history
		dl.History = existing.History
	}

	// Add current failure to history
	dl.History = append(dl.History, FailHistoryEntry{
		Timestamp: now,
		WorkerID:  job.AssignedTo,
		Error:     error,
		Attempt:   job.RetryCount,
	})

	// Limit history size
	if len(dl.History) > 100 {
		dl.History = dl.History[len(dl.History)-100:]
	}

	dlq.jobs[job.JobID] = dl

	if err := dlq.save(); err != nil {
		return err
	}

	// Check alert threshold
	if dlq.config.AlertThreshold > 0 && len(dlq.jobs) >= dlq.config.AlertThreshold {
		dlq.sendAlert("dlq_threshold_exceeded", map[string]interface{}{
			"count":     len(dlq.jobs),
			"threshold": dlq.config.AlertThreshold,
		})
	}

	log.Printf("[DLQ] Job %s moved to DLQ: %s", job.JobID[:8], reason)
	return nil
}

// isReplayable determines if a job can be replayed
func (dlq *DeadLetterQueue) isReplayable(job *Job, reason string) bool {
	// Non-replayable reasons
	nonReplayable := map[string]bool{
		"cancelled":       true,
		"invalid_payload": true,
		"deleted":         true,
	}

	if nonReplayable[reason] {
		return false
	}

	// Check if job has required fields
	if job.VideoName == "" && job.ProjectID == "" {
		return false
	}

	return true
}

// GetJob retrieves a job from DLQ
func (dlq *DeadLetterQueue) GetJob(jobID string) (*DeadLetterJob, error) {
	dlq.mu.RLock()
	defer dlq.mu.RUnlock()

	job, ok := dlq.jobs[jobID]
	if !ok {
		return nil, fmt.Errorf("job not found in DLQ: %s", jobID)
	}
	return job, nil
}

// ListJobs returns all jobs in DLQ
func (dlq *DeadLetterQueue) ListJobs() []*DeadLetterJob {
	dlq.mu.RLock()
	defer dlq.mu.RUnlock()

	jobs := make([]*DeadLetterJob, 0, len(dlq.jobs))
	for _, job := range dlq.jobs {
		jobs = append(jobs, job)
	}
	return jobs
}

// ReplayJob moves a job back to the main queue for retry
func (dlq *DeadLetterQueue) ReplayJob(ctx context.Context, jobID string, fq *FileQueue) error {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	dl, ok := dlq.jobs[jobID]
	if !ok {
		return fmt.Errorf("job not found in DLQ: %s", jobID)
	}

	if !dl.Replayable {
		return fmt.Errorf("job is not replayable: %s", jobID)
	}

	// Build payload for job submission
	payload := dl.OriginalJob.Payload
	if payload == nil {
		payload = make(map[string]interface{})
	}

	// Add relevant fields to payload
	payload["video_name"] = dl.OriginalJob.VideoName
	payload["project_id"] = dl.OriginalJob.ProjectID
	if dl.OriginalJob.SlotData != nil {
		payload["slot_data"] = dl.OriginalJob.SlotData
	}
	payload["replayed_from_dlq"] = true
	payload["original_dead_reason"] = dl.DeadReason

	// Submit job to queue (SQLite-backed)
	if err := fq.SubmitJob(ctx, jobID, payload); err != nil {
		return fmt.Errorf("failed to submit replayed job: %w", err)
	}

	// Remove from DLQ
	delete(dlq.jobs, jobID)
	if err := dlq.save(); err != nil {
		log.Printf("[WARN] DLQ save error after replay: %v", err)
	}

	log.Printf("[INFO] Job %s replayed from DLQ", jobID[:8])
	return nil
}

// RemoveJob permanently removes a job from DLQ
func (dlq *DeadLetterQueue) RemoveJob(jobID string) error {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	if _, ok := dlq.jobs[jobID]; !ok {
		return fmt.Errorf("job not found in DLQ: %s", jobID)
	}

	delete(dlq.jobs, jobID)
	return dlq.save()
}

// PurgeOldJobs removes jobs older than maxAge
func (dlq *DeadLetterQueue) PurgeOldJobs(ctx context.Context) (int, error) {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	if dlq.config.MaxAge <= 0 {
		return 0, nil
	}

	cutoff := time.Now().UTC().Add(-dlq.config.MaxAge)
	removed := 0

	for id, job := range dlq.jobs {
		if job.DeadAt.Before(cutoff) {
			delete(dlq.jobs, id)
			removed++
		}
	}

	if removed > 0 {
		if err := dlq.save(); err != nil {
			return 0, err
		}
		log.Printf("[DLQ] DLQ purged %d old jobs", removed)
	}

	return removed, nil
}

// PurgeToLimit removes oldest jobs to stay under MaxItems limit
func (dlq *DeadLetterQueue) PurgeToLimit() (int, error) {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	if dlq.config.MaxItems <= 0 || len(dlq.jobs) <= dlq.config.MaxItems {
		return 0, nil
	}

	// Sort by DeadAt and remove oldest
	type jobWithTime struct {
		id     string
		deadAt time.Time
	}

	var sorted []jobWithTime
	for id, job := range dlq.jobs {
		sorted = append(sorted, jobWithTime{id, job.DeadAt})
	}

	// Simple bubble sort for small datasets (could use sort.Slice)
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[j].deadAt.Before(sorted[i].deadAt) {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	toRemove := len(dlq.jobs) - dlq.config.MaxItems
	removed := 0

	for i := 0; i < toRemove && i < len(sorted); i++ {
		delete(dlq.jobs, sorted[i].id)
		removed++
	}

	if removed > 0 {
		if err := dlq.save(); err != nil {
			return 0, err
		}
		log.Printf("[DLQ] DLQ purged %d jobs to meet limit", removed)
	}

	return removed, nil
}

// Stats returns DLQ statistics
func (dlq *DeadLetterQueue) Stats() map[string]interface{} {
	dlq.mu.RLock()
	defer dlq.mu.RUnlock()

	stats := map[string]interface{}{
		"total":      len(dlq.jobs),
		"replayable": 0,
		"by_reason":  make(map[string]int),
		"oldest_job": nil,
		"newest_job": nil,
	}

	byReason := stats["by_reason"].(map[string]int)
	var oldest, newest time.Time

	for _, job := range dlq.jobs {
		if job.Replayable {
			stats["replayable"] = stats["replayable"].(int) + 1
		}
		byReason[job.DeadReason]++

		if oldest.IsZero() || job.DeadAt.Before(oldest) {
			oldest = job.DeadAt
			stats["oldest_job"] = job.JobID
		}
		if newest.IsZero() || job.DeadAt.After(newest) {
			newest = job.DeadAt
			stats["newest_job"] = job.JobID
		}
	}

	return stats
}

// SetAlertHandler sets a callback for DLQ alerts
func (dlq *DeadLetterQueue) SetAlertHandler(handler func(alertType string, data map[string]interface{})) {
	dlq.onAlert = handler
}

// sendAlert triggers an alert
func (dlq *DeadLetterQueue) sendAlert(alertType string, data map[string]interface{}) {
	if dlq.onAlert != nil {
		go dlq.onAlert(alertType, data)
	}
}

// ProcessLoop runs periodic DLQ maintenance
func (dlq *DeadLetterQueue) ProcessLoop(ctx context.Context, interval time.Duration, fq *FileQueue) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Purge old jobs
			if _, err := dlq.PurgeOldJobs(ctx); err != nil {
				log.Printf("[WARN] DLQ purge error: %v", err)
			}

			// Enforce limit
			if _, err := dlq.PurgeToLimit(); err != nil {
				log.Printf("[WARN] DLQ limit enforcement error: %v", err)
			}

			// Auto-replay if enabled
			if dlq.config.AutoReplay && fq != nil {
				dlq.autoReplayJobs(ctx, fq)
			}
		}
	}
}

// autoReplayJobs attempts to auto-replay eligible jobs
func (dlq *DeadLetterQueue) autoReplayJobs(ctx context.Context, fq *FileQueue) {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	now := time.Now().UTC()
	replayAfter := dlq.config.ReplayDelay

	for id, job := range dlq.jobs {
		if !job.Replayable {
			continue
		}

		// Check if enough time has passed
		if now.Sub(job.DeadAt) < replayAfter {
			continue
		}

		// Attempt replay
		log.Printf("[INFO] Auto-replaying job %s from DLQ", id[:8])

		// This will be handled by the main loop calling ReplayJob
		// We just mark it for replay here
		if job.Metadata == nil {
			job.Metadata = make(map[string]interface{})
		}
		job.Metadata["auto_replay_pending"] = true
	}
}

// GetReplayableJobs returns jobs that can be replayed
func (dlq *DeadLetterQueue) GetReplayableJobs() []*DeadLetterJob {
	dlq.mu.RLock()
	defer dlq.mu.RUnlock()

	var replayable []*DeadLetterJob
	for _, job := range dlq.jobs {
		if job.Replayable {
			replayable = append(replayable, job)
		}
	}
	return replayable
}

// Count returns the number of jobs in DLQ
func (dlq *DeadLetterQueue) Count() int {
	dlq.mu.RLock()
	defer dlq.mu.RUnlock()
	return len(dlq.jobs)
}
