// Package queue provides job queue management with SQLite persistence
package queue

import (
	"context"
	"fmt"
	"time"

	"velox-server/internal/store"
)

// NowISO returns current time in ISO format (exported for use by other modules)
func NowISO() string {
	return time.Now().UTC().Format(time.RFC3339)
}

// NowUnix returns current time as Unix timestamp (exported for use by other modules)
func NowUnix() int64 {
	return time.Now().Unix()
}

// UpdateJobFields updates specific fields of a job
func UpdateJobFields(ctx context.Context, jobID string, fields map[string]interface{}, dbStore *store.SQLiteStore, activeJobs map[string]*Job) error {
	job, ok := activeJobs[jobID]
	if !ok {
		// Check SQLite
		m, err := dbStore.GetJob(ctx, jobID)
		if err != nil {
			return fmt.Errorf("job not found: %s", jobID)
		}
		job = MapToJob(m)
		// Add to active cache if it's now active
		if job.Status == StatusPending || job.Status == StatusProcessing {
			activeJobs[jobID] = job
		}
	}

	now := NowUnix()
	nowISOVal := NowISO()
	job.UpdatedAt = now

	// Update fields dynamically
	for key, value := range fields {
		switch key {
		case "status":
			if s, ok := value.(string); ok {
				job.Status = JobStatus(s)
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
		case "result_path":
			if s, ok := value.(string); ok {
				if job.Payload == nil {
					job.Payload = make(map[string]interface{})
				}
				job.Payload["result_path"] = s
			}
		case "upload_info":
			if m, ok := value.(map[string]interface{}); ok {
				if job.Payload == nil {
					job.Payload = make(map[string]interface{})
				}
				job.Payload["upload_info"] = m
			}
		case "history":
			if h, ok := value.([]JobHistoryEntry); ok {
				job.History = h
			}
		case "completed_job_run_ids":
			if job.Payload == nil {
				job.Payload = make(map[string]interface{})
			}
			job.Payload["completed_job_run_ids"] = value
		default:
			if job.Payload == nil {
				job.Payload = make(map[string]interface{})
			}
			job.Payload[key] = value
		}
	}

	// Ensure history entry for status change
	if newStatus, ok := fields["status"].(string); ok && newStatus == "COMPLETED" {
		job.History = append(job.History, JobHistoryEntry{
			Status:    "COMPLETED",
			Timestamp: nowISOVal,
			WorkerID:  job.AssignedTo,
			Message:   "Job completed",
		})
	}

	// Persist to SQLite
	if err := PersistJob(job, dbStore); err != nil {
		return err
	}

	// If job is now completed/error, remove from active cache
	if job.Status == StatusCompleted || job.Status == StatusError {
		delete(activeJobs, jobID)
	}

	return nil
}

// UpdateJobLogs appends logs to a job
func UpdateJobLogs(ctx context.Context, jobID string, logs []JobLogEntry, dbStore *store.SQLiteStore, activeJobs map[string]*Job) error {
	job, ok := activeJobs[jobID]
	if !ok {
		// Check SQLite for the job
		m, err := dbStore.GetJob(ctx, jobID)
		if err != nil {
			return fmt.Errorf("job not found: %s", jobID)
		}
		job = MapToJob(m)
		// Don't add to active cache if it's not active
		if job.Status != StatusPending && job.Status != StatusProcessing {
			// Just update logs in SQLite without caching
			// Add logs to existing job logs in SQLite
			if err := dbStore.AddJobLog(jobID, "Worker log update", "", false); err != nil {
				return err
			}
			return nil
		}
		activeJobs[jobID] = job
	}

	job.Logs = append(job.Logs, logs...)
	job.LogsUpdatedAt = NowISO()

	// Limit logs
	maxLogs := 20000
	if len(job.Logs) > maxLogs {
		job.Logs = job.Logs[len(job.Logs)-maxLogs:]
	}

	return PersistJob(job, dbStore)
}
