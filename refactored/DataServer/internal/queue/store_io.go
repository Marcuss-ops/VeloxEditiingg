// Package queue provides job queue management with SQLite persistence
package queue

import (
	"encoding/json"
	"fmt"

	"velox-server/internal/store"
)

// MapToJob converts a map[string]any (from SQLite query) to a Job struct.
// Merge priority (lowest to highest): raw_json → request_json → result_json → DB columns.
// History and logs are NOT read from blob — they are stored in separate tables.
func MapToJob(m map[string]any) *Job {
	job := &Job{
		Payload: make(map[string]interface{}),
	}

	// ── Step 1: raw_json fallback (backward compat during migration) ──
	if raw, ok := m["raw_json"].(map[string]any); ok && len(raw) > 0 {
		mergeMap(job.Payload, raw)
	} else if raw, ok := m["raw_json"].(string); ok && raw != "" {
		var parsed map[string]any
		if err := json.Unmarshal([]byte(raw), &parsed); err == nil {
			mergeMap(job.Payload, parsed)
		}
	}

	// ── Step 2: request_json (immutable request payload) ──
	if req, ok := m["request_json"].(map[string]any); ok && len(req) > 0 {
		mergeMap(job.Payload, req)
	} else if req, ok := m["request_json"].(string); ok && req != "" {
		var parsed map[string]any
		if err := json.Unmarshal([]byte(req), &parsed); err == nil {
			mergeMap(job.Payload, parsed)
		}
	}

	// ── Step 3: result_json (mutable operational state) ──
	if res, ok := m["result_json"].(map[string]any); ok && len(res) > 0 {
		mergeMap(job.Payload, res)
	} else if res, ok := m["result_json"].(string); ok && res != "" {
		var parsed map[string]any
		if err := json.Unmarshal([]byte(res), &parsed); err == nil {
			mergeMap(job.Payload, parsed)
		}
	}

	// ── Step 4: Populate struct fields from Payload/raw_json (will be overridden by columns below) ──
	job.JobID = asString(m["job_id"])
	job.Status = JobStatus(asString(m["status"]))
	job.VideoName = asString(m["video_name"])
	job.ProjectID = asString(m["project_id"])
	job.AssignedTo = asString(m["assigned_to"])
	job.WorkerName = asString(m["worker_name"])
	job.ClaimedBy = asString(m["claimed_by"])
	job.ClaimedAt = asString(m["claimed_at"])
	job.LeaseID = asString(m["lease_id"])
	job.LeaseExpiry = m["lease_expiry"]
	job.OutputVideoID = asString(m["output_video_id"])
	job.DriveURL = asString(m["drive_url"])
	job.JobFingerprint = asString(m["job_fingerprint"])
	job.LastError = asString(m["last_error"])
	job.ErrorMessage = asString(m["error_message"])
	job.MasterVideoPath = asString(m["master_video_path"])
	job.ArtifactID = asString(m["artifact_id"])
	job.OutputSHA256 = asString(m["output_sha256"])
	job.IdempotencyKey = asString(m["upload_idempotency_key"])
	job.LogsUpdatedAt = asString(m["logs_updated_at"])
	job.LastUploadResult = asString(m["last_upload_result"])
	job.LastUploadAttemptAt = asString(m["last_upload_attempt_at"])
	job.LastDriveUploadResult = asString(m["last_drive_upload_result"])
	job.RemoteStatus = asString(m["remote_status"])
	job.SubmittedVia = asString(m["submitted_via"])
	job.LastActivity = asString(m["last_activity"])
	job.FailedBy = asString(m["failed_by"])

	if s, ok := m["run_id"].(string); ok {
		job.RunID = s
	}
	if s, ok := m["job_run_id"].(string); ok && s != "" {
		job.RunID = s
	}

	// Integer fields
	job.RetryCount = asIntFromMap(m, "retry_count")
	job.Attempt = asIntFromMap(m, "attempt")
	job.MaxRetries = asIntFromMap(m, "max_retries")

	// Boolean fields
	if b, ok := m["video_uploaded"].(bool); ok {
		job.VideoUploaded = b
	} else if s, ok := m["video_uploaded"].(string); ok && s == "1" {
		job.VideoUploaded = true
	}

	// Timestamp fields (leave as interface{} from m)
	job.CreatedAt = m["created_at"]
	job.UpdatedAt = m["updated_at"]
	job.StartedAt = m["started_at"]
	job.CompletedAt = m["completed_at"]
	job.AssignedAt = m["assigned_at"]
	job.ProcessingAt = m["processing_at"]
	job.LastErrorAt = m["last_error_at"]
	job.FailedAt = m["failed_at"]

	// Slot data
	if slot, ok := m["slot_data"].(map[string]any); ok {
		job.SlotData = slot
	}

	// History and logs are NOT read from the blob — they live in separate tables.
	// If present in the map (from raw_json fallback), populate for read-only access.
	if hist, ok := m["history"].([]any); ok {
		job.History = make([]JobHistoryEntry, 0, len(hist))
		for _, h := range hist {
			if hm, ok := h.(map[string]any); ok {
				entry := JobHistoryEntry{}
				if s, ok := hm["status"].(string); ok {
					entry.Status = s
				}
				entry.Timestamp = hm["timestamp"]
				if s, ok := hm["worker_id"].(string); ok {
					entry.WorkerID = s
				}
				if s, ok := hm["message"].(string); ok {
					entry.Message = s
				}
				job.History = append(job.History, entry)
			}
		}
	}

	if logs, ok := m["logs"].([]any); ok {
		job.Logs = make([]JobLogEntry, 0, len(logs))
		for _, l := range logs {
			if lm, ok := l.(map[string]any); ok {
				entry := JobLogEntry{}
				if s, ok := lm["timestamp"].(string); ok {
					entry.Timestamp = s
				}
				if s, ok := lm["time"].(string); ok {
					entry.Time = s
				}
				if s, ok := lm["message"].(string); ok {
					entry.Message = s
				}
				if s, ok := lm["level"].(string); ok {
					entry.Level = s
				}
				if b, ok := lm["is_error"].(bool); ok {
					entry.IsError = b
				}
				if s, ok := lm["worker_id"].(string); ok {
					entry.WorkerID = s
				}
				job.Logs = append(job.Logs, entry)
			}
		}
	}

	return job
}

// mergeMap copies keys from src to dst (shallow).
func mergeMap(dst, src map[string]any) {
	for k, v := range src {
		dst[k] = v
	}
}

// PersistJob saves a job to SQLite using the new result_json path.
// Delegates to PersistJobResult — no longer serializes history/logs into the blob.
func PersistJob(job *Job, dbStore *store.SQLiteStore) error {
	return PersistJobResult(job, dbStore)
}

// PersistJobResult stores the mutable operational state of a job in result_json.
// History and logs are NOT included (they live in separate tables).
// Operational columns are set from struct fields (authoritative).
func PersistJobResult(job *Job, dbStore *store.SQLiteStore) error {
	// Build result_json blob with mutable fields only (no history, no logs, no request payload)
	m := make(map[string]any)
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

	// Also include any extra payload fields that don't have dedicated struct fields
	if job.Payload != nil {
		for k, v := range job.Payload {
			if _, exists := m[k]; !exists {
				m[k] = v
			}
		}
	}

	// NOTE: history and logs are intentionally NOT included here.
	// They are stored in job_history and job_logs tables respectively.

	rawJSON, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("failed to marshal result_json: %w", err)
	}

	return dbStore.UpsertJobResult(job.JobID, rawJSON)
}

// PersistJobRequest stores the immutable request payload in request_json.
// Only called once at job creation.
func PersistJobRequest(jobID string, payload map[string]interface{}, eventStore store.EventStore) error {
	m := make(map[string]any)
	for k, v := range payload {
		m[k] = v
	}
	// Remove mutable fields that don't belong in request
	delete(m, "status")
	delete(m, "assigned_to")
	delete(m, "claimed_by")
	delete(m, "claimed_at")
	delete(m, "lease_id")
	delete(m, "lease_expiry")
	delete(m, "retry_count")
	delete(m, "attempt")
	delete(m, "last_error")
	delete(m, "error_message")
	delete(m, "history")
	delete(m, "logs")

	rawJSON, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("failed to marshal request_json: %w", err)
	}

	return eventStore.SetJobRequest(jobID, rawJSON)
}

// ── Helpers ──

func asString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func asIntFromMap(m map[string]any, key string) int {
	switch v := m[key].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	case string:
		var n int
		_ = json.Unmarshal([]byte(v), &n)
		return n
	}
	return 0
}
