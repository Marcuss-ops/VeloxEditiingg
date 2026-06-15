package jobs

import (
	"context"
	"strings"
	"time"
)

func (s *Service) ListJobs(ctx context.Context, limit int) ([]map[string]interface{}, error) {
	if s.jobsRepo != nil {
		dbJobs, err := s.jobsRepo.ListJobs(ctx, limit)
		if err == nil && len(dbJobs) > 0 {
			jobsList := make([]map[string]interface{}, 0, len(dbJobs))
			for _, job := range dbJobs {
				id := asJobString(job["job_id"])
				jobsList = append(jobsList, map[string]interface{}{
					"job_id":                   id,
					"video_name":               job["video_name"],
					"status":                   job["status"],
					"created_at":               job["created_at"],
					"updated_at":               job["updated_at"],
					"completed_at":             job["completed_at"],
					"started_at":               job["started_at"],
					"processing_at":            job["processing_at"],
					"assigned_at":              job["assigned_at"],
					"assigned_to":              job["assigned_to"],
					"assigned_worker_ip":       job["assigned_worker_ip"],
					"error":                    job["error"],
					"error_message":            job["error_message"],
					"last_error":               job["last_error"],
					"drive_url":                job["drive_url"],
					"remote_status":            job["remote_status"],
					"last_drive_upload_result": job["last_drive_upload_result"],
					"last_upload_result":       job["last_upload_result"],
					"last_upload_attempt_at":   job["last_upload_attempt_at"],
				})
			}
			return jobsList, nil
		}
	}

	if s.fileQ == nil {
		return []map[string]interface{}{}, nil
	}

	jobs, err := s.fileQ.GetAllJobs(ctx)
	if err != nil {
		return nil, err
	}

	jobsList := make([]map[string]interface{}, 0, min(limit, len(jobs)))
	keepFields := []string{
		"job_id", "video_name", "status", "created_at", "updated_at",
		"completed_at", "started_at", "processing_at", "assigned_at",
		"assigned_to", "assigned_worker_ip", "error", "error_message",
		"last_error", "drive_url", "remote_status", "last_drive_upload_result",
		"last_upload_result", "last_upload_attempt_at",
	}

	for id, job := range jobs {
		trimmed := map[string]interface{}{"job_id": id}
		for _, field := range keepFields {
			switch field {
			case "job_id":
				trimmed[field] = job.JobID
			case "video_name":
				trimmed[field] = job.VideoName
			case "status":
				trimmed[field] = string(job.Status)
			case "created_at":
				trimmed[field] = job.CreatedAt
			case "updated_at":
				trimmed[field] = job.UpdatedAt
			case "assigned_to":
				trimmed[field] = job.AssignedTo
			case "last_error":
				trimmed[field] = job.LastError
			case "drive_url":
				trimmed[field] = job.DriveURL
			}
		}
		if trimmed["video_name"] == "" && job.SlotData != nil {
			if videoName, ok := job.SlotData["video_name"].(string); ok {
				trimmed["video_name"] = videoName
			}
		}
		jobsList = append(jobsList, trimmed)
		if len(jobsList) >= limit {
			break
		}
	}

	return jobsList, nil
}

func (s *Service) GetJob(ctx context.Context, jobID string) (map[string]interface{}, bool, error) {
	if s.jobsRepo != nil {
		job, err := s.jobsRepo.GetJob(ctx, jobID)
		if err == nil && len(job) > 0 {
			s.enrichJobWithProcessingLogs(ctx, job, jobID)
			return job, true, nil
		}
	}

	if s.fileQ == nil {
		return nil, false, nil
	}

	job, err := s.fileQ.GetJob(ctx, jobID)
	if err != nil {
		return nil, false, err
	}

	result := map[string]interface{}{
		"job_id":     job.JobID,
		"job_run_id": job.RunID,
		"run_id":     job.RunID,
		"status":     string(job.Status),
		"video_name": job.VideoName,
		"project_id": job.ProjectID,
		"created_at": job.CreatedAt,
		"updated_at": job.UpdatedAt,
	}

	if job.Payload != nil {
		for key, value := range job.Payload {
			if _, exists := result[key]; !exists {
				result[key] = value
			}
		}
	}
	s.enrichJobWithProcessingLogs(ctx, result, jobID)

	return result, true, nil
}

func (s *Service) GetSummary(ctx context.Context, limit int, now time.Time) (map[string]interface{}, error) {
	if s.jobsRepo != nil {
		if counts, err := s.jobsRepo.JobCounts(ctx); err == nil {
			recent := map[string][]interface{}{
				"PENDING":    {},
				"PROCESSING": {},
				"COMPLETED":  {},
				"ERROR":      {},
			}
			if jobsList, listErr := s.jobsRepo.ListJobs(ctx, max(limit*4, limit)); listErr == nil {
				for _, job := range jobsList {
					status := strings.ToUpper(strings.TrimSpace(asJobString(job["status"])))
					switch status {
					case "FAILED":
						status = "ERROR"
					case "QUEUED":
						status = "PENDING"
					case "ASSIGNED", "LEASED":
						status = "PROCESSING"
					}
					arr, ok := recent[status]
					if !ok || len(arr) >= limit {
						continue
					}
					recent[status] = append(arr, map[string]interface{}{
						"job_id":     asJobString(job["job_id"]),
						"video_name": job["video_name"],
						"status":     status,
						"created_at": job["created_at"],
						"updated_at": job["updated_at"],
					})
				}
			}
			return map[string]interface{}{
				"counts":    counts,
				"recent":    recent,
				"timestamp": now.Unix(),
			}, nil
		}
	}

	if s.fileQ == nil {
		return map[string]interface{}{
			"counts":    map[string]int64{"pending": 0, "processing": 0, "completed": 0, "error": 0},
			"recent":    map[string][]interface{}{"PENDING": {}, "PROCESSING": {}, "COMPLETED": {}, "ERROR": {}},
			"timestamp": now.Unix(),
		}, nil
	}

	stats, err := s.fileQ.Stats(ctx)
	if err != nil {
		return nil, err
	}

	jobs, _ := s.fileQ.GetAllJobs(ctx)
	recent := map[string][]map[string]interface{}{
		"PENDING":    {},
		"PROCESSING": {},
		"COMPLETED":  {},
		"ERROR":      {},
	}

	for _, job := range jobs {
		status := string(job.Status)
		if status == "FAILED" {
			status = "ERROR"
		}
		if arr, ok := recent[status]; ok && len(arr) < limit {
			recent[status] = append(arr, map[string]interface{}{
				"job_id":     job.JobID,
				"video_name": job.VideoName,
				"status":     status,
				"created_at": job.CreatedAt,
				"updated_at": job.UpdatedAt,
			})
		}
	}

	return map[string]interface{}{
		"counts":    stats,
		"recent":    recent,
		"timestamp": now.Unix(),
	}, nil
}
