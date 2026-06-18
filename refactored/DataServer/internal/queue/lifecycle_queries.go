package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"

	"velox-server/internal/store"
)

// legacyKeyWarnLog is a sync.Map keyed by jobID + key to dedupe the WARN
// output. Upload handlers retry in tight loops so without this dedupe a
// single job's legacy writes could log dozens of lines. Operators see at
// most one WARN per (jobID, key) for the life of the process.
var legacyKeyWarnLog sync.Map

func logLegacyKeyOnce(jobID, key string) {
	ck := jobID + "|" + key
	if _, seen := legacyKeyWarnLog.LoadOrStore(ck, struct{}{}); !seen {
		log.Printf("[QUEUE] WARN: UpdateJobFields legacy key=%q job=%s — migrate to artifacts/job_deliveries", key, jobID)
	}
}

// UpdateJobFieldsStrictWhitelistKey is the set of canonical keys that may be
// merged into the Job via UpdateJobFields. Anything outside this set is a
// programmer error and is rejected with ErrJobFieldNotWhitelisted.
//
// Deprecated: Use targeted lifecycle methods (RecordRenderFinished, CompleteJob,
// FailJob) instead of the generic field merger. This whitelist exists only for
// backward compatibility with tests and will be removed when all callers migrate.
var UpdateJobFieldsStrictWhitelistKey = map[string]struct{}{
	// CURRENT canonical fields
	"status":                {},
	"completed_at":          {},
	"completed_by":          {},
	"assigned_to":           {},
	"attempt":               {},
	"lease_id":              {},
	"lease_expiry":          {},
	"job_run_id":            {},
	"result_path_worker":    {},
	"error_message":         {},
	"last_error":            {},
	"failed_at":             {},
	"failed_by":             {},
	"started_at":            {},
	"claimed_by":            {},
	"claimed_at":            {},
	"worker_name":           {},
	"max_retries":           {},
	"youtube_upload_status": {},
	"drive_upload_status":   {},
	"worker_id":             {},
	"worker_output":         {},
	"result_path":           {},
	"video_sha256":          {},
	"upload_info":           {},

	// LEGACY keys — logged at runtime
	"master_video_path":      {},
	"drive_url":              {},
	"drive_folder_id":        {},
	"youtube_url":            {},
	"video_uploaded":         {},
	"artifact_id":            {},
	"output_sha256":          {},
	"upload_idempotency_key": {},
}

// UpdateJobFieldsLegacyKeys are accepted but logged at runtime.
var UpdateJobFieldsLegacyKeys = map[string]struct{}{
	"master_video_path":      {},
	"drive_url":              {},
	"drive_folder_id":        {},
	"youtube_url":            {},
	"video_uploaded":         {},
	"artifact_id":            {},
	"output_sha256":          {},
	"upload_idempotency_key": {},
}

// ErrJobFieldNotWhitelisted is returned by UpdateJobFields for keys outside
// the canonical whitelist.
var ErrJobFieldNotWhitelisted = errors.New("queue: job field not in UpdateJobFields whitelist")

// UpdateJobFields reads the job, applies field updates via a STRICT WHITELIST,
// validates any status transition, and persists back via the repository.
//
// Deprecated: Use targeted lifecycle methods instead. This generic field merger
// is retained only for backward compatibility with tests.
func (l *LifecycleService) UpdateJobFields(ctx context.Context, jobID string, fields map[string]interface{}) error {
	if len(fields) == 0 {
		return nil
	}
	for key := range fields {
		if _, allowed := UpdateJobFieldsStrictWhitelistKey[key]; !allowed {
			return fmt.Errorf("%w: %q (canonical: %v; legacy allowed: %v)",
				ErrJobFieldNotWhitelisted, key, whitelistKeysSorted(), whitelistLegacyKeysSorted())
		}
		if _, isLegacy := UpdateJobFieldsLegacyKeys[key]; isLegacy {
			logLegacyKeyOnce(jobID, key)
		}
	}

	// Read rich job via eventStore (it holds dbStore under the hood)
	m, err := l.eventStore.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("job not found: %s", jobID)
	}
	job := MapToJob(m)

	now := NowUnix()
	nowISO := NowISO()
	job.UpdatedAt = now

	for key, value := range fields {
		switch key {
		case "status":
			if s, ok := value.(string); ok {
				next := JobStatus(s)
				if err := l.Validate(job.Status, next); err != nil {
					return fmt.Errorf("transition rejected: %w", err)
				}
				job.Status = next
			}
		case "completed_at":
			job.CompletedAt = value
		case "completed_by", "assigned_to":
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
		case "job_run_id":
			if s, ok := value.(string); ok {
				job.RunID = s
			}
		case "result_path_worker":
			if s, ok := value.(string); ok {
				if job.Payload == nil {
					job.Payload = make(map[string]interface{})
				}
				job.Payload["result_path_worker"] = s
			}
		case "error_message":
			if s, ok := value.(string); ok {
				job.ErrorMessage = s
			}
		case "last_error":
			if s, ok := value.(string); ok {
				job.LastError = s
			}
		case "failed_at":
			job.FailedAt = value
		case "failed_by":
			if s, ok := value.(string); ok {
				job.FailedBy = s
			}
		case "started_at":
			job.StartedAt = value
		case "claimed_by":
			if s, ok := value.(string); ok {
				job.ClaimedBy = s
			}
		case "claimed_at":
			if s, ok := value.(string); ok {
				job.ClaimedAt = s
			}
		case "worker_name":
			if s, ok := value.(string); ok {
				job.WorkerName = s
			}
		case "max_retries":
			switch v := value.(type) {
			case int:
				job.MaxRetries = v
			case int64:
				job.MaxRetries = int(v)
			case float64:
				job.MaxRetries = int(v)
			}
		case "worker_id":
			if s, ok := value.(string); ok {
				if job.Payload == nil {
					job.Payload = make(map[string]interface{})
				}
				job.Payload["worker_id"] = s
			}
		case "worker_output":
			if job.Payload == nil {
				job.Payload = make(map[string]interface{})
			}
			job.Payload["worker_output"] = value
		case "result_path":
			if s, ok := value.(string); ok {
				if job.Payload == nil {
					job.Payload = make(map[string]interface{})
				}
				job.Payload["result_path"] = s
			}
		case "video_sha256":
			if s, ok := value.(string); ok {
				if job.Payload == nil {
					job.Payload = make(map[string]interface{})
				}
				job.Payload["video_sha256"] = s
			}
		case "upload_info":
			if job.Payload == nil {
				job.Payload = make(map[string]interface{})
			}
			job.Payload["upload_info"] = value
		case "youtube_upload_status":
			if s, ok := value.(string); ok {
				if job.Payload == nil {
					job.Payload = make(map[string]interface{})
				}
				job.Payload["youtube_upload_status"] = s
			}
		case "drive_upload_status":
			if s, ok := value.(string); ok {
				if job.Payload == nil {
					job.Payload = make(map[string]interface{})
				}
				job.Payload["drive_upload_status"] = s
			}
		case "youtube_url":
			if s, ok := value.(string); ok {
				if job.Payload == nil {
					job.Payload = make(map[string]interface{})
				}
				job.Payload["youtube_url"] = s
			}
		case "drive_folder_id":
			if s, ok := value.(string); ok {
				if job.Payload == nil {
					job.Payload = make(map[string]interface{})
				}
				job.Payload["drive_folder_id"] = s
			}
		case "master_video_path":
			if s, ok := value.(string); ok {
				job.MasterVideoPath = s
			}
		case "drive_url":
			if s, ok := value.(string); ok {
				job.DriveURL = s
			}
		case "video_uploaded":
			if b, ok := value.(bool); ok {
				job.VideoUploaded = b
			}
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
		}
	}

	// Ensure history entry for status change
	if newStatusRaw, ok := fields["status"].(string); ok {
		if JobStatus(newStatusRaw) == StatusSucceeded {
			job.LastError = ""
			job.LastErrorAt = nil
			job.ErrorMessage = ""
			job.FailedAt = nil
			job.FailedBy = ""
			job.History = append(job.History, JobHistoryEntry{
				Status:    string(job.Status),
				Timestamp: nowISO,
				WorkerID:  job.AssignedTo,
				Message:   "Job completed",
			})
		}
	}

	// Serialize and persist via repository
	rawJSON, err := json.Marshal(buildResultJSON(job))
	if err != nil {
		return fmt.Errorf("failed to marshal result_json: %w", err)
	}
	if err := l.jobRepo.UpdateJobResult(ctx, jobID, rawJSON); err != nil {
		return err
	}
	// Sync field updates to DB columns (not just the result_json blob).
	if err := l.eventStore.UpdateJobSupplementary(jobID, fields); err != nil {
		log.Printf("UpdateJobFields: supplementary column sync for %s: %v", jobID, err)
	}
	return nil
}

// buildResultJSON builds the result_json blob for persistence.
func buildResultJSON(job *Job) map[string]interface{} {
	m := make(map[string]interface{})
	m["job_id"] = job.JobID
	m["status"] = string(job.Status)
	m["video_name"] = job.VideoName
	m["project_id"] = job.ProjectID
	m["created_at"] = job.CreatedAt
	m["updated_at"] = job.UpdatedAt
	m["started_at"] = job.StartedAt
	m["completed_at"] = job.CompletedAt
	m["assigned_at"] = job.AssignedAt
	m["processing_at"] = job.ProcessingAt
	m["assigned_to"] = job.AssignedTo
	m["worker_name"] = job.WorkerName
	m["claimed_by"] = job.ClaimedBy
	m["claimed_at"] = job.ClaimedAt
	m["lease_id"] = job.LeaseID
	m["lease_expiry"] = job.LeaseExpiry
	m["retry_count"] = job.RetryCount
	m["attempt"] = job.Attempt
	m["max_retries"] = job.MaxRetries
	m["last_error"] = job.LastError
	m["error_message"] = job.ErrorMessage
	m["failed_at"] = job.FailedAt
	m["failed_by"] = job.FailedBy
	m["video_uploaded"] = job.VideoUploaded
	m["master_video_path"] = job.MasterVideoPath
	m["artifact_id"] = job.ArtifactID
	m["output_sha256"] = job.OutputSHA256
	m["upload_idempotency_key"] = job.IdempotencyKey
	m["output_video_id"] = job.OutputVideoID
	m["drive_url"] = job.DriveURL
	m["run_id"] = job.RunID
	m["job_run_id"] = job.RunID
	m["logs_updated_at"] = job.LogsUpdatedAt
	m["job_fingerprint"] = job.JobFingerprint
	m["last_upload_result"] = job.LastUploadResult
	m["last_upload_attempt_at"] = job.LastUploadAttemptAt
	m["last_drive_upload_result"] = job.LastDriveUploadResult
	m["remote_status"] = job.RemoteStatus
	m["submitted_via"] = job.SubmittedVia
	m["last_activity"] = job.LastActivity
	m["slot_data"] = job.SlotData
	m["last_error_at"] = job.LastErrorAt
	if job.Payload != nil {
		for k, v := range job.Payload {
			if _, exists := m[k]; !exists {
				m[k] = v
			}
		}
	}
	return m
}

// whitelistKeysSorted returns the whitelist keys in canonical order.
func whitelistKeysSorted() []string {
	keys := make([]string, 0, len(UpdateJobFieldsStrictWhitelistKey))
	for k := range UpdateJobFieldsStrictWhitelistKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// whitelistLegacyKeysSorted returns the legacy-flagged keys sorted.
func whitelistLegacyKeysSorted() []string {
	keys := make([]string, 0, len(UpdateJobFieldsLegacyKeys))
	for k := range UpdateJobFieldsLegacyKeys {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// GetJobsByStatus returns all jobs with a given status via JobRepository.
func (l *LifecycleService) GetJobsByStatus(ctx context.Context, status JobStatus) ([]*Job, error) {
	storeJobs, err := l.jobRepo.ListByStatus(ctx, []store.JobStatus{toStoreJobStatus(status)}, 1000)
	if err != nil {
		return nil, fmt.Errorf("job repo list by status: %w", err)
	}
	result := make([]*Job, 0, len(storeJobs))
	for _, sj := range storeJobs {
		m, err := l.eventStore.GetJob(ctx, sj.JobID)
		if err != nil {
			log.Printf("GetJobsByStatus: GetJob(%s) failed after ListByStatus returned it: %v", sj.JobID, err)
			continue
		}
		result = append(result, MapToJob(m))
	}
	return result, nil
}

// GetNextJobID returns the next pending job ID.
func (l *LifecycleService) GetNextJobID(ctx context.Context) (string, error) {
	jobs, err := l.eventStore.ListJobsByStatus([]string{"PENDING"}, 1)
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

// toStoreJobStatus maps a queue.JobStatus to the equivalent store.JobStatus.
func toStoreJobStatus(s JobStatus) store.JobStatus {
	return store.JobStatus(s)
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
