// Package queue provides job queue management with SQLite persistence
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"velox-server/internal/store"
)

// TransitionService validates and executes job status transitions.
// All status changes flow through this service to ensure consistency.
type TransitionService struct {
	dbStore *store.SQLiteStore

	// jobRepo is an optional narrow JobRepository (spec §5). When set,
	// ClaimNextJob delegates to it for the atomic lease acquisition step.
	// Nil falls back to the older dbStore path. PR-2 wires it; the rollout
	// to other methods (Complete/Fail/UpdateFields) is planned for PR-2b.
	jobRepo store.JobRepository
}

// NewTransitionService creates a new transition service.
func NewTransitionService(dbStore *store.SQLiteStore) *TransitionService {
	return &TransitionService{dbStore: dbStore}
}

// SetJobRepository wires a narrow JobRepository for spec §5 callers.
// Passing nil disables the fast-path (dbStore is used). The setter is
// non-blocking and idempotent; multiple calls in startup are fine.
func (ts *TransitionService) SetJobRepository(repo store.JobRepository) {
	ts.jobRepo = repo
}

// JobRepository returns the wired narrow repository (or nil if unconfigured).
// Tests use this to swap implementations; production code should not need it.
func (ts *TransitionService) JobRepository() store.JobRepository {
	return ts.jobRepo
}

// Validate checks whether a transition from one status to another is allowed.
func (ts *TransitionService) Validate(from, to JobStatus) error {
	if !isValidJobStatusTransition(from, to) {
		return fmt.Errorf("invalid transition: %s → %s", from, to)
	}
	return nil
}

// ClaimNextJob atomically claims the next pending job for a worker directly from SQLite.
// Returns the claimed job (with updated status), or nil if no pending jobs.
//
// When a JobRepository is wired (SetJobRepository) the atomic lease step
// delegates to its narrow ClaimNext method, satisfying spec §5 single-method
// atomicity end-to-end. Otherwise it falls back to the legacy dbStore path.
//
// NOTE: After the atomic claim, the path calls dbStore.GetJob + MapToJob to
// load the rich payload. On SQLite single-writer this is safe (the lease is
// committed before the read sees the row). On Postgres or any MVCC backend
// the read could observe a stale snapshot; the map's result_json blob is the
// authoritative post-claim state at that moment, not an eventually-consistent view.
func (ts *TransitionService) ClaimNextJob(ctx context.Context, workerID string, allowedJobTypes []string) (*Job, error) {
	if ts.jobRepo != nil {
		result, err := ts.jobRepo.ClaimNext(ctx, store.ClaimParams{
			WorkerID:        workerID,
			AllowedJobTypes: allowedJobTypes,
			Now:             time.Now().UTC(),
		})
		if err != nil {
			if errors.Is(err, store.ErrNoClaimableJob) {
				return nil, nil
			}
			return nil, fmt.Errorf("job repository claim: %w", err)
		}
		if result == nil || result.JobID == "" {
			return nil, nil
		}
		// Spec §5: ClaimResult is opaque; the rich payload still needs the legacy
		// MapToJob pipeline so callers see the full request/result/history blobs.
		// Pull the canonical row via dbStore.GetJob and project through MapToJob.
		m, err := ts.dbStore.GetJob(ctx, result.JobID)
		if err != nil {
			return nil, fmt.Errorf("post-claim job fetch: %w", err)
		}
		return MapToJob(m), nil
	}
	// Legacy path (dbStore-direct).
	claimedJSON, ok, err := ts.dbStore.ClaimNextPendingJob(workerID, allowedJobTypes, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	// Parse claimed result to get the job_id, then fetch full job from SQLite
	claimed, _ := parseRawJSON(claimedJSON)
	jobID := ""
	if id, ok := claimed["job_id"].(string); ok {
		jobID = id
	} else if id, ok := claimed["job_id"]; ok {
		jobID = fmt.Sprintf("%v", id)
	}
	if jobID == "" {
		return nil, fmt.Errorf("claimed job missing job_id")
	}
	m, err := ts.dbStore.GetJob(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to get claimed job: %w", err)
	}
	return MapToJob(m), nil
}

// CompleteJob marks a job as SUCCEEDED using CAS (compare-and-swap on revision).
// Idempotent: returns nil if already succeeded.
func (ts *TransitionService) CompleteJob(ctx context.Context, jobID string) error {
	m, err := ts.dbStore.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("job not found: %s", jobID)
	}
	job := MapToJob(m)

	normalized := normalizeJobStatus(string(job.Status))
	if normalized == StatusSucceeded {
		return nil // idempotent
	}

	revision := getIntField(m, "revision")
	if err := ts.Validate(job.Status, StatusSucceeded); err != nil {
		return err
	}

	nowISO := NowISO()
	newRevision, err := ts.dbStore.TransitionJobStatus(ctx, jobID, string(normalized), string(StatusSucceeded), revision)
	if err != nil {
		return fmt.Errorf("CAS transition failed: %w", err)
	}

	// Update supplementary fields (non-CAS, but after successful transition)
	ts.dbStore.UpdateJobSupplementary(jobID, map[string]interface{}{
		"completed_at":  nowISO,
		"last_error":    "",
		"error_message": "",
		"failed_at":     nil,
		"failed_by":     nil,
		"lease_id":      "",
		"lease_expiry":  nil,
		"assigned_to":   job.AssignedTo,
	})

	ts.dbStore.LogJobEvent(jobID, "job_succeeded", map[string]interface{}{
		"worker_id": job.AssignedTo,
		"revision":  newRevision,
	})

	return nil
}

// FailJob marks a job as FAILED or RETRY_WAIT using CAS.
// If requeue is true and retries remain, transitions to RETRY_WAIT → PENDING.
func (ts *TransitionService) FailJob(ctx context.Context, jobID, errMsg, workerID string, requeue bool, maxRetries int) error {
	m, err := ts.dbStore.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("job not found: %s", jobID)
	}
	job := MapToJob(m)
	normalized := normalizeJobStatus(string(job.Status))

	revision := getIntField(m, "revision")
	nowISO := NowISO()

	if requeue && job.RetryCount < maxRetries {
		if err := ts.Validate(job.Status, StatusRetryWait); err != nil {
			return err
		}
		newRevision, err := ts.dbStore.TransitionJobStatus(ctx, jobID, string(normalized), string(StatusRetryWait), revision)
		if err != nil {
			return fmt.Errorf("CAS transition to RETRY_WAIT failed: %w", err)
		}

		ts.dbStore.UpdateJobSupplementary(jobID, map[string]interface{}{
			"last_error":   errMsg,
			"assigned_to":  "",
			"claimed_by":   "",
			"lease_id":     "",
			"lease_expiry": nil,
		})

		ts.dbStore.LogJobEvent(jobID, "job_retry_wait", map[string]interface{}{
			"worker_id": workerID,
			"error":     errMsg,
			"revision":  newRevision,
		})
	} else {
		if err := ts.Validate(job.Status, StatusFailed); err != nil {
			return err
		}
		newRevision, err := ts.dbStore.TransitionJobStatus(ctx, jobID, string(normalized), string(StatusFailed), revision)
		if err != nil {
			return fmt.Errorf("CAS transition to FAILED failed: %w", err)
		}

		ts.dbStore.UpdateJobSupplementary(jobID, map[string]interface{}{
			"error_message": errMsg,
			"last_error":    errMsg,
			"failed_at":     nowISO,
			"failed_by":     workerID,
			"lease_id":      "",
			"lease_expiry":  nil,
		})

		ts.dbStore.LogJobEvent(jobID, "job_failed", map[string]interface{}{
			"worker_id": workerID,
			"error":     errMsg,
			"revision":  newRevision,
		})
	}

	return nil
}

// RequeueZombieJobs finds processing jobs with expired leases and requeues them.
// Returns the count of requeued jobs.
func (ts *TransitionService) RequeueZombieJobs(ctx context.Context, timeout time.Duration) (int, error) {
	jobs, err := ts.dbStore.GetActiveJobs()
	if err != nil {
		return 0, err
	}

	now := time.Now()
	requeued := 0

	for _, m := range jobs {
		job := MapToJob(m)
		if job.Status != StatusProcessing {
			continue
		}

		leaseExpired := false
		if job.LeaseExpiry != nil {
			if leaseStr, ok := job.LeaseExpiry.(string); ok && leaseStr != "" {
				if leaseTime, err := time.Parse(time.RFC3339, leaseStr); err == nil && now.After(leaseTime) {
					leaseExpired = true
				}
			}
		}

		var assignedTime time.Time
		switch v := job.AssignedAt.(type) {
		case string:
			assignedTime, _ = time.Parse(time.RFC3339, v)
		case float64:
			assignedTime = time.Unix(int64(v), 0)
		}

		if now.Sub(assignedTime) > timeout || leaseExpired {
			nowISO := NowISO()
			reason := fmt.Sprintf("Zombie: no heartbeat for %v", now.Sub(assignedTime))
			if leaseExpired {
				reason = "Lease expired"
			}
			job.Status = StatusPending
			job.LastError = reason
			job.LastErrorAt = now.Unix()
			job.AssignedTo = ""
			job.AssignedAt = nil
			job.ClaimedBy = ""
			job.ClaimedAt = ""
			job.LeaseExpiry = nil
			job.RetryCount++

			job.History = append(job.History, JobHistoryEntry{
				Status:    "PENDING",
				Timestamp: nowISO,
				Message:   "Requeued after zombie timeout",
			})

			if err := PersistJob(job, ts.dbStore); err != nil {
				continue
			}
			requeued++
		}
	}
	return requeued, nil
}

// RenewLease extends the lease for an active job.
func (ts *TransitionService) RenewLease(ctx context.Context, jobID, workerID, leaseID string, leaseExpiry time.Time) error {
	m, err := ts.dbStore.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("job not found: %s", jobID)
	}
	job := MapToJob(m)

	if err := ts.Validate(job.Status, StatusProcessing); err != nil {
		return fmt.Errorf("job %s is not renewable in state %s", jobID, job.Status)
	}

	nowISO := NowISO()
	job.Status = StatusProcessing
	job.LeaseID = leaseID
	job.LeaseExpiry = leaseExpiry.UTC().Format(time.RFC3339)
	job.UpdatedAt = NowUnix()
	if job.Attempt == 0 {
		job.Attempt = job.RetryCount
	}
	job.History = append(job.History, JobHistoryEntry{
		Status:    "PROCESSING",
		Timestamp: nowISO,
		WorkerID:  workerID,
		Message:   "Lease renewed",
	})

	return PersistJob(job, ts.dbStore)
}

// SubmitJob creates a new job in SQLite.
func (ts *TransitionService) SubmitJob(ctx context.Context, jobID string, payload map[string]interface{}, maxRetries int) (*Job, error) {
	now := NowUnix()
	nowISO := NowISO()

	job := &Job{
		JobID:      jobID,
		Status:     StatusPending,
		CreatedAt:  now,
		UpdatedAt:  now,
		RetryCount: 0,
		MaxRetries: maxRetries,
		History: []JobHistoryEntry{{
			Status:    "PENDING",
			Timestamp: nowISO,
			Message:   "Job created",
		}},
		Payload: payload,
	}

	// Extract known fields from payload
	if s, ok := payload["video_name"].(string); ok {
		job.VideoName = s
	}
	if s, ok := payload["project_id"].(string); ok {
		job.ProjectID = s
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

	if err := PersistJob(job, ts.dbStore); err != nil {
		return nil, err
	}
	// Store the immutable request payload separately
	if err := PersistJobRequest(jobID, payload, ts.dbStore); err != nil {
		return nil, fmt.Errorf("failed to persist request_json: %w", err)
	}
	return job, nil
}

// UpdateJobFields reads the job from SQLite, applies field updates via merge logic,
// validates any status transition, and persists back.
func (ts *TransitionService) UpdateJobFields(ctx context.Context, jobID string, fields map[string]interface{}) error {
	m, err := ts.dbStore.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("job not found: %s", jobID)
	}
	job := MapToJob(m)

	now := NowUnix()
	nowISO := NowISO()
	job.UpdatedAt = now

	// Apply fields dynamically (same merge logic as before)
	for key, value := range fields {
		switch key {
		case "status":
			if s, ok := value.(string); ok {
				next := normalizeJobStatus(s)
				if err := ts.Validate(job.Status, next); err != nil {
					continue
				}
				job.Status = next
			}
		case "completed_at":
			job.CompletedAt = value
		case "completed_by":
			if s, ok := value.(string); ok {
				job.AssignedTo = s
			}
		case "video_uploaded":
			if b, ok := value.(bool); ok {
				job.VideoUploaded = b
			}
		case "master_video_path":
			if s, ok := value.(string); ok {
				job.MasterVideoPath = s
			}
		case "drive_url":
			if s, ok := value.(string); ok {
				job.DriveURL = s
			}
		case "result_path_worker":
			if s, ok := value.(string); ok {
				if job.Payload == nil {
					job.Payload = make(map[string]interface{})
				}
				job.Payload["result_path_worker"] = s
			}
		case "job_run_id":
			if s, ok := value.(string); ok {
				job.RunID = s
			}
		case "assigned_to":
			if s, ok := value.(string); ok {
				job.AssignedTo = s
			}
		case "attempt":
			switch v := value.(type) {
			case int:
				job.Attempt = v
			case int64:
				job.Attempt = int(v)
			case float64:
				job.Attempt = int(v)
			}
		case "lease_id":
			if s, ok := value.(string); ok {
				job.LeaseID = s
			}
		case "lease_expiry":
			job.LeaseExpiry = value
		case "artifact_id":
			if s, ok := value.(string); ok {
				job.ArtifactID = s
			}
		case "output_sha256":
			if s, ok := value.(string); ok {
				job.OutputSHA256 = s
			}
		case "upload_idempotency_key":
			if s, ok := value.(string); ok {
				job.IdempotencyKey = s
			}
		default:
			if job.Payload == nil {
				job.Payload = make(map[string]interface{})
			}
			job.Payload[key] = value
		}
	}

	// Ensure history entry for status change
	if newStatus, ok := fields["status"].(string); ok && newStatus == "COMPLETED" {
		job.LastError = ""
		job.LastErrorAt = nil
		job.ErrorMessage = ""
		job.FailedAt = nil
		job.FailedBy = ""
		job.History = append(job.History, JobHistoryEntry{
			Status:    "COMPLETED",
			Timestamp: nowISO,
			WorkerID:  job.AssignedTo,
			Message:   "Job completed",
		})
	}

	return PersistJob(job, ts.dbStore)
}

// UpdateJobLogs persists worker log entries directly to the job_logs table.
func (ts *TransitionService) UpdateJobLogs(ctx context.Context, jobID string, logs []JobLogEntry) error {
	for _, entry := range logs {
		if err := ts.dbStore.AddJobLog(jobID, entry.Message, entry.WorkerID, entry.IsError); err != nil {
			return fmt.Errorf("failed to add job log: %w", err)
		}
	}
	return nil
}

// GetJob retrieves a job by ID directly from SQLite.
func (ts *TransitionService) GetJob(ctx context.Context, jobID string) (*Job, error) {
	m, err := ts.dbStore.GetJob(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}
	return MapToJob(m), nil
}

// GetJobPayload returns the job payload.
func (ts *TransitionService) GetJobPayload(ctx context.Context, jobID string) (map[string]interface{}, error) {
	job, err := ts.GetJob(ctx, jobID)
	if err != nil {
		return nil, err
	}
	payload := make(map[string]interface{})
	if job.Payload != nil {
		for k, v := range job.Payload {
			payload[k] = v
		}
	}
	payload["job_id"] = job.JobID
	payload["job_run_id"] = job.RunID
	payload["run_id"] = job.RunID
	payload["status"] = string(job.Status)
	payload["video_name"] = job.VideoName
	payload["project_id"] = job.ProjectID
	if job.LeaseID != "" {
		payload["lease_id"] = job.LeaseID
	}
	if job.LeaseExpiry != nil {
		payload["lease_expiry"] = job.LeaseExpiry
	}
	return payload, nil
}

// GetJobAttempt returns the current retry count.
func (ts *TransitionService) GetJobAttempt(ctx context.Context, jobID string) (int, error) {
	job, err := ts.GetJob(ctx, jobID)
	if err != nil {
		return 0, err
	}
	return job.RetryCount, nil
}

// GetJobsByStatus returns all jobs with a given status directly from SQLite.
func (ts *TransitionService) GetJobsByStatus(ctx context.Context, status JobStatus) ([]*Job, error) {
	jobs, err := ts.dbStore.ListJobsByStatus([]string{string(status)}, 1000)
	if err != nil {
		return nil, err
	}
	result := make([]*Job, 0, len(jobs))
	for _, m := range jobs {
		result = append(result, MapToJob(m))
	}
	return result, nil
}

// GetAllJobs returns all active jobs from SQLite.
func (ts *TransitionService) GetAllJobs(ctx context.Context) (map[string]*Job, error) {
	activeJobs, err := ts.dbStore.GetActiveJobs()
	if err != nil {
		return nil, err
	}
	result := make(map[string]*Job)
	for id, m := range activeJobs {
		result[id] = MapToJob(m)
	}
	return result, nil
}

// Stats returns queue statistics from SQLite.
func (ts *TransitionService) Stats(ctx context.Context) (map[string]int64, error) {
	return ts.dbStore.JobCounts(ctx)
}

// DeleteJob removes a job from SQLite.
func (ts *TransitionService) DeleteJob(ctx context.Context, jobID string) error {
	return ts.dbStore.DeleteJob(jobID)
}

// GetNextJobID returns the next pending job ID directly from SQLite.
func (ts *TransitionService) GetNextJobID(ctx context.Context) (string, error) {
	jobs, err := ts.dbStore.ListJobsByStatus([]string{"PENDING", "QUEUED"}, 1)
	if err != nil {
		return "", err
	}
	if len(jobs) == 0 {
		return "", nil
	}
	if id, ok := jobs[0]["job_id"].(string); ok {
		return id, nil
	}
	return "", nil
}

// GetJobAsMap returns a job as a map for flexible field access.
func (ts *TransitionService) GetJobAsMap(ctx context.Context, jobID string) (map[string]interface{}, error) {
	job, err := ts.GetJob(ctx, jobID)
	if err != nil {
		return nil, err
	}
	result := make(map[string]interface{})
	result["job_id"] = job.JobID
	result["status"] = string(job.Status)
	result["video_name"] = job.VideoName
	result["project_id"] = job.ProjectID
	result["created_at"] = job.CreatedAt
	result["updated_at"] = job.UpdatedAt
	result["started_at"] = job.StartedAt
	result["completed_at"] = job.CompletedAt
	result["assigned_to"] = job.AssignedTo
	result["claimed_by"] = job.ClaimedBy
	result["claimed_at"] = job.ClaimedAt
	result["lease_id"] = job.LeaseID
	result["lease_expiry"] = job.LeaseExpiry
	result["worker_name"] = job.WorkerName
	result["retry_count"] = job.RetryCount
	result["attempt"] = job.Attempt
	result["max_retries"] = job.MaxRetries
	result["last_error"] = job.LastError
	result["error_message"] = job.ErrorMessage
	result["video_uploaded"] = job.VideoUploaded
	result["master_video_path"] = job.MasterVideoPath
	result["artifact_id"] = job.ArtifactID
	result["output_sha256"] = job.OutputSHA256
	result["upload_idempotency_key"] = job.IdempotencyKey
	result["output_video_id"] = job.OutputVideoID
	result["run_id"] = job.RunID
	result["job_run_id"] = job.RunID
	if len(job.Logs) > 0 {
		result["logs"] = job.Logs
	}
	if len(job.History) > 0 {
		result["history"] = job.History
	}
	if job.Payload != nil {
		for k, v := range job.Payload {
			if _, exists := result[k]; !exists {
				result[k] = v
			}
		}
	}
	return result, nil
}

// parseRawJSON unmarshals raw job JSON bytes.
func parseRawJSON(raw []byte) (map[string]interface{}, error) {
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// LeaseJob claims a job for a worker atomically from SQLite.
func (ts *TransitionService) LeaseJob(ctx context.Context, jobID, workerID string) error {
	// This is handled by ClaimNextJob — for explicit leases, re-validate from SQLite
	m, err := ts.dbStore.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("job not found: %s", jobID)
	}
	job := MapToJob(m)

	if err := ts.Validate(job.Status, StatusProcessing); err != nil {
		return fmt.Errorf("job %s cannot be leased: %w", jobID, err)
	}

	now := NowUnix()
	nowISO := NowISO()
	job.Status = StatusProcessing
	job.AssignedTo = workerID
	job.AssignedAt = nowISO
	job.ClaimedBy = workerID
	job.ClaimedAt = nowISO
	job.LeaseID = uuid.NewString()
	job.LeaseExpiry = time.Now().UTC().Add(30 * time.Minute).Format(time.RFC3339)
	job.UpdatedAt = now
	job.RetryCount++

	job.History = append(job.History, JobHistoryEntry{
		Status:    "PROCESSING",
		Timestamp: nowISO,
		WorkerID:  workerID,
		Message:   fmt.Sprintf("Job assigned to worker %s", workerID),
	})

	return PersistJob(job, ts.dbStore)
}

// getIntField extracts an integer field from a job map, returning 0 if not found.
func getIntField(m map[string]interface{}, key string) int {
	if m == nil {
		return 0
	}
	v, ok := m[key]
	if !ok || v == nil {
		return 0
	}
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	default:
		return 0
	}
}
