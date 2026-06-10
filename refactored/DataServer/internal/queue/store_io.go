// Package queue provides job queue management with SQLite persistence
package queue

import (
	"encoding/json"
	"fmt"
	"log"

	"velox-server/internal/store"
)

// MapToJob converts a map[string]any to a Job struct
func MapToJob(m map[string]any) *Job {
	m = renderPlanVersion(m)
	job := &Job{
		Payload: m,
	}

	if s, ok := m["job_id"].(string); ok {
		job.JobID = s
	}
	if s, ok := m["status"].(string); ok {
		job.Status = JobStatus(s)
	}
	if s, ok := m["video_name"].(string); ok {
		job.VideoName = s
	}
	if s, ok := m["project_id"].(string); ok {
		job.ProjectID = s
	}
	if s, ok := m["assigned_to"].(string); ok {
		job.AssignedTo = s
	}
	if s, ok := m["worker_name"].(string); ok {
		job.WorkerName = s
	}
	if s, ok := m["claimed_by"].(string); ok {
		job.ClaimedBy = s
	}
	if s, ok := m["claimed_at"].(string); ok {
		job.ClaimedAt = s
	}
	if s, ok := m["output_video_id"].(string); ok {
		job.OutputVideoID = s
	}
	if s, ok := m["drive_url"].(string); ok {
		job.DriveURL = s
	}
	if s, ok := m["job_fingerprint"].(string); ok {
		job.JobFingerprint = s
	}
	if s, ok := m["last_error"].(string); ok {
		job.LastError = s
	}
	if s, ok := m["error_message"].(string); ok {
		job.ErrorMessage = s
	}
	if s, ok := m["master_video_path"].(string); ok {
		job.MasterVideoPath = s
	}
	if s, ok := m["run_id"].(string); ok {
		job.RunID = s
	}
	if s, ok := m["job_run_id"].(string); ok && s != "" {
		job.RunID = s
	}
	if s, ok := m["logs_updated_at"].(string); ok {
		job.LogsUpdatedAt = s
	}
	if i, ok := m["retry_count"].(int); ok {
		job.RetryCount = i
	} else if f, ok := m["retry_count"].(float64); ok {
		job.RetryCount = int(f)
	}
	if i, ok := m["max_retries"].(int); ok {
		job.MaxRetries = i
	} else if f, ok := m["max_retries"].(float64); ok {
		job.MaxRetries = int(f)
	}
	if b, ok := m["video_uploaded"].(bool); ok {
		job.VideoUploaded = b
	}
	job.CreatedAt = m["created_at"]
	job.UpdatedAt = m["updated_at"]
	job.StartedAt = m["started_at"]
	job.CompletedAt = m["completed_at"]
	job.AssignedAt = m["assigned_at"]
	job.ProcessingAt = m["processing_at"]
	job.LastErrorAt = m["last_error_at"]
	job.FailedAt = m["failed_at"]

	if m, ok := m["slot_data"].(map[string]any); ok {
		job.SlotData = m
	}

	// Parse history
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

	// Parse logs
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

// PersistJob saves a job to SQLite (primary source of truth)
// Exported for use by other queue modules
func PersistJob(job *Job, dbStore *store.SQLiteStore) error {
	// Build full job map for storage
	m := make(map[string]any)
	if job.Payload != nil {
		for k, v := range job.Payload {
			m[k] = v
		}
	}
	m = renderPlanVersion(m)

	// Overwrite with struct fields
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
	m["retry_count"] = job.RetryCount
	m["max_retries"] = job.MaxRetries
	m["last_error"] = job.LastError
	m["error_message"] = job.ErrorMessage
	m["failed_at"] = job.FailedAt
	m["failed_by"] = job.FailedBy
	m["video_uploaded"] = job.VideoUploaded
	m["master_video_path"] = job.MasterVideoPath
	m["output_video_id"] = job.OutputVideoID
	m["drive_url"] = job.DriveURL
	m["run_id"] = job.RunID
	m["job_run_id"] = job.RunID
	m["logs_updated_at"] = job.LogsUpdatedAt
	m["job_fingerprint"] = job.JobFingerprint
	m["slot_data"] = job.SlotData

	// Marshal history
	if len(job.History) > 0 {
		m["history"] = job.History
	}

	// Marshal logs
	if len(job.Logs) > 0 {
		m["logs"] = job.Logs
	}

	rawJSON, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	return dbStore.UpsertJob(job.JobID, rawJSON)
}

// LoadActiveJobs loads only PENDING and PROCESSING jobs from SQLite into memory
// This minimizes memory usage by not caching completed/historical jobs
func LoadActiveJobs(dbStore *store.SQLiteStore) (map[string]*Job, error) {
	activeJobs, err := dbStore.GetActiveJobs()
	if err != nil {
		return nil, err
	}

	result := make(map[string]*Job)
	for id, m := range activeJobs {
		job := MapToJob(m)
		result[id] = job
	}

	log.Printf("✅ Loaded %d active jobs from SQLite (PENDING/PROCESSING only)", len(result))
	return result, nil
}
