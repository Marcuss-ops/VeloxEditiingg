package jobs

import (
	"context"
	"sort"
	"strings"
	"time"

	"velox-server/internal/config"
	"velox-server/internal/queue"
	"velox-server/internal/store"
	"velox-server/internal/workers"
)

// Service holds job read/use-case logic that should stay outside HTTP handlers.
type Service struct {
	cfg      *config.Config
	fileQ    *queue.FileQueue
	redisQ   *queue.Queue
	jobsRepo store.JobsRepository
	logger   *queue.EventLogger
	reg      *workers.Registry
}

type ClaimRequest struct {
	WorkerID    string
	WorkerName  string
	ClientIP    string
	Drain       bool
	Schedulable bool
}

type ClaimResult struct {
	JobID   string
	Payload map[string]interface{}
	Reason  string
}

type SubmitResultRequest struct {
	JobID    string
	WorkerID string
	Status   string
	Error    string
	Output   map[string]interface{}
	EndTime  string
}

func NewService(cfg *config.Config, fileQ *queue.FileQueue, redisQ *queue.Queue, jobsRepo store.JobsRepository, logger *queue.EventLogger, reg *workers.Registry) *Service {
	return &Service{
		cfg:      cfg,
		fileQ:    fileQ,
		redisQ:   redisQ,
		jobsRepo: jobsRepo,
		logger:   logger,
		reg:      reg,
	}
}

func (s *Service) ClaimNextJob(ctx context.Context, req ClaimRequest) (*ClaimResult, error) {
	if strings.TrimSpace(req.WorkerID) == "" {
		return &ClaimResult{Reason: "missing worker_id"}, nil
	}

	if s.cfg != nil && s.cfg.ForceSingleWorker != "" && s.cfg.ForceSingleWorker != "0" && !strings.EqualFold(s.cfg.ForceSingleWorker, "false") {
		allowed := req.WorkerID == s.cfg.ForceSingleWorker || req.ClientIP == s.cfg.ForceSingleWorker
		if !allowed {
			return &ClaimResult{Reason: "Single-worker mode active (" + s.cfg.ForceSingleWorker + ")"}, nil
		}
	}

	if s.cfg != nil {
		allowedWorkers := strings.TrimSpace(s.cfg.AllowedWorkers)
		if allowedWorkers != "" && !strings.EqualFold(allowedWorkers, "*") && !strings.EqualFold(allowedWorkers, "ALL") {
			parts := strings.Split(allowedWorkers, ",")
			allowlistOK := false
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if p == "" {
					continue
				}
				if req.ClientIP == p || req.WorkerID == p || req.WorkerName == p {
					allowlistOK = true
					break
				}
			}
			if !allowlistOK && s.cfg.AllowlistAllowRegistered && s.reg != nil && s.reg.IsRegistered(ctx, req.WorkerID) {
				allowlistOK = true
			}
			if !allowlistOK {
				return &ClaimResult{Reason: "Worker not allowed"}, nil
			}
		}
	}

	if !req.Drain && s.reg != nil {
		workerInfo := s.reg.GetWorker(ctx, req.WorkerID)
		if workerInfo != nil {
			if workerInfo.Drain {
				return &ClaimResult{Reason: "Worker draining"}, nil
			}
			if !req.Schedulable && !workerInfo.Schedulable {
				return &ClaimResult{Reason: "Worker not schedulable"}, nil
			}
		}
	}

	var (
		payload map[string]interface{}
		jobID   string
		err     error
	)
	if s.fileQ != nil {
		job, claimErr := s.fileQ.ClaimNextJob(ctx, req.WorkerID)
		if claimErr != nil {
			return &ClaimResult{Reason: "lease failed"}, nil
		}
		if job == nil {
			return &ClaimResult{}, nil
		}
		jobID = job.JobID
		payload = job.Payload
	} else if s.redisQ != nil {
		jobID, err = s.redisQ.GetNextJobID(ctx)
		if err != nil || jobID == "" {
			return &ClaimResult{}, nil
		}
		payload, _ = s.redisQ.GetJobPayload(ctx, jobID)
		err = s.redisQ.LeaseJob(ctx, jobID, req.WorkerID)
		if err != nil {
			return &ClaimResult{Reason: "lease failed"}, nil
		}
	}

	if payload == nil {
		payload = make(map[string]interface{})
	}
	payload["job_id"] = jobID
	if _, ok := payload["id"]; !ok {
		payload["id"] = jobID
	}
	payload["id"] = jobID

	if s.reg != nil {
		_ = s.reg.Heartbeat(ctx, req.WorkerID, req.WorkerName, "busy", jobID, nil)
	}

	return &ClaimResult{
		JobID:   jobID,
		Payload: payload,
	}, nil
}

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
		return nil, false, nil
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

func (s *Service) SubmitResult(ctx context.Context, req SubmitResultRequest) (bool, error) {
	status := strings.ToLower(strings.TrimSpace(req.Status))
	if status == "" || status == "success" || status == "completed" {
		var err error
		if s.fileQ != nil {
			if len(req.Output) > 0 {
				logEntries := extractWorkerLogEntries(req.Output, req.WorkerID)
				if len(logEntries) > 0 {
					_ = s.fileQ.UpdateJobLogs(ctx, req.JobID, logEntries)
				}
			}

			updates := map[string]interface{}{
				"status": "COMPLETED",
			}
			if strings.TrimSpace(req.EndTime) != "" {
				updates["completed_at"] = strings.TrimSpace(req.EndTime)
			}
			if strings.TrimSpace(req.WorkerID) != "" {
				updates["completed_by"] = strings.TrimSpace(req.WorkerID)
			}
			if len(req.Output) > 0 {
				updates["worker_output"] = req.Output
				if path := extractOutputVideoPath(req.Output); path != "" {
					updates["master_video_path"] = path
				}
			}
			err = s.fileQ.UpdateJobFields(ctx, req.JobID, updates)
		} else if s.redisQ != nil {
			err = s.redisQ.CompleteJob(ctx, req.JobID)
		}
		if err != nil {
			return false, err
		}
		if strings.TrimSpace(req.WorkerID) != "" && s.reg != nil {
			_ = s.reg.Heartbeat(ctx, req.WorkerID, "", "online", "", nil)
		}
		return s.fileQ != nil, nil
	}

	var err error
	if s.fileQ != nil {
		if len(req.Output) > 0 {
			logEntries := extractWorkerLogEntries(req.Output, req.WorkerID)
			if len(logEntries) > 0 {
				_ = s.fileQ.UpdateJobLogs(ctx, req.JobID, logEntries)
			}
		}
		err = s.fileQ.FailJob(ctx, req.JobID, req.Error, true)
	} else if s.redisQ != nil {
		err = s.redisQ.FailJob(ctx, req.JobID, req.Error, true)
	}
	if err != nil {
		return false, err
	}
	if strings.TrimSpace(req.WorkerID) != "" && s.reg != nil {
		_ = s.reg.Heartbeat(ctx, req.WorkerID, "", "online", "", nil)
	}
	return false, nil
}

func (s *Service) CompleteJob(ctx context.Context, jobID, workerID string) error {
	var err error
	if s.fileQ != nil {
		err = s.fileQ.CompleteJob(ctx, jobID)
	} else if s.redisQ != nil {
		err = s.redisQ.CompleteJob(ctx, jobID)
	}
	if err != nil {
		return err
	}
	if strings.TrimSpace(workerID) != "" && s.reg != nil {
		_ = s.reg.Heartbeat(ctx, workerID, "", "online", "", nil)
	}
	return nil
}

func (s *Service) FailJob(ctx context.Context, jobID, workerID, errMsg string) error {
	var attempt int
	if s.fileQ != nil {
		attempt, _ = s.fileQ.GetJobAttempt(ctx, jobID)
	} else if s.redisQ != nil {
		attempt, _ = s.redisQ.GetJobAttempt(ctx, jobID)
	}

	maxAttempts := 3
	if s.cfg != nil && s.cfg.MaxJobAttempts > 0 {
		maxAttempts = s.cfg.MaxJobAttempts
	}
	requeue := attempt < maxAttempts

	var err error
	if s.fileQ != nil {
		err = s.fileQ.FailJob(ctx, jobID, errMsg, requeue)
	} else if s.redisQ != nil {
		err = s.redisQ.FailJob(ctx, jobID, errMsg, requeue)
	}
	if err != nil {
		return err
	}
	if strings.TrimSpace(workerID) != "" && s.reg != nil {
		_ = s.reg.Heartbeat(ctx, workerID, "", "online", "", nil)
	}
	return nil
}

func (s *Service) GetJobEvents(ctx context.Context, jobID string, limit int) ([]map[string]interface{}, error) {
	events := make([]map[string]interface{}, 0, limit)
	if s.logger != nil {
		loggedEvents, err := s.logger.GetRecentEvents(jobID, limit)
		if err != nil {
			return nil, err
		}
		events = append(events, loggedEvents...)
	}

	if jobID != "" && s.fileQ != nil {
		jobMap, err := s.fileQ.GetJobAsMap(ctx, jobID)
		if err == nil && jobMap != nil {
			events = append(events, buildJobLogEvents(jobID, jobMap["logs"])...)
			events = append(events, BuildWorkerRecentLogEvents(ctx, s.reg, jobMap, jobID)...)
		}
	}

	if len(events) > 1 {
		events = DedupeAndSortEvents(events)
	}
	if len(events) > limit {
		events = events[:limit]
	}
	return events, nil
}

func (s *Service) enrichJobWithProcessingLogs(ctx context.Context, job map[string]interface{}, jobID string) {
	if s == nil || s.reg == nil || len(job) == 0 || strings.TrimSpace(jobID) == "" {
		return
	}
	recent := BuildWorkerRecentLogEvents(ctx, s.reg, job, jobID)
	if len(recent) == 0 {
		return
	}

	existingAny, _ := job["logs"].([]interface{})
	seen := make(map[string]struct{}, len(existingAny)+len(recent))
	merged := make([]interface{}, 0, len(existingAny)+len(recent))

	for _, row := range existingAny {
		if m, ok := row.(map[string]interface{}); ok {
			msg := strings.TrimSpace(asJobString(m["message"]))
			ts := strings.TrimSpace(asJobString(m["timestamp"]))
			if msg != "" {
				seen[ts+"|"+msg] = struct{}{}
			}
		}
		merged = append(merged, row)
	}

	for _, ev := range recent {
		msg := strings.TrimSpace(asJobString(ev["message"]))
		ts := strings.TrimSpace(asJobString(ev["timestamp"]))
		if msg == "" {
			continue
		}
		key := ts + "|" + msg
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		merged = append(merged, map[string]interface{}{
			"timestamp": ts,
			"message":   msg,
			"worker_id": asJobString(ev["worker_id"]),
			"level":     "info",
		})
	}

	if len(merged) > 0 {
		job["logs"] = merged
	}
}

func BuildWorkerRecentLogEvents(ctx context.Context, reg *workers.Registry, job map[string]interface{}, jobID string) []map[string]interface{} {
	if reg == nil || job == nil {
		return nil
	}
	workerID := strings.TrimSpace(asJobString(job["assigned_to"]))
	if workerID == "" {
		workerID = strings.TrimSpace(asJobString(job["claimed_by"]))
	}
	if workerID == "" {
		return nil
	}
	info := reg.GetWorker(ctx, workerID)
	if info == nil || len(info.RecentLogs) == 0 {
		return nil
	}
	status := strings.ToUpper(strings.TrimSpace(asJobString(job["status"])))
	includeAll := status == "PROCESSING"

	out := make([]map[string]interface{}, 0, len(info.RecentLogs))
	for _, line := range info.RecentLogs {
		raw := strings.TrimSpace(line)
		if raw == "" {
			continue
		}
		if !includeAll && !strings.Contains(raw, jobID) {
			continue
		}
		ts := time.Now().UTC().Format(time.RFC3339)
		msg := raw
		if split := strings.SplitN(raw, " [", 2); len(split) == 2 {
			if parsed, err := time.ParseInLocation("2006/01/02 15:04:05", split[0], time.UTC); err == nil {
				ts = parsed.UTC().Format(time.RFC3339)
				msg = "[" + split[1]
			}
		}
		out = append(out, map[string]interface{}{
			"timestamp":  ts,
			"job_id":     jobID,
			"event":      "worker_log",
			"event_type": "worker_log",
			"message":    msg,
			"worker_id":  workerID,
			"source":     "worker_recent_logs",
		})
	}
	return out
}

func DedupeAndSortEvents(events []map[string]interface{}) []map[string]interface{} {
	seen := make(map[string]struct{}, len(events))
	out := make([]map[string]interface{}, 0, len(events))
	for _, event := range events {
		key := strings.TrimSpace(asJobString(event["timestamp"])) + "|" +
			strings.TrimSpace(asJobString(event["event"])) + "|" +
			strings.TrimSpace(asJobString(event["message"]))
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, event)
	}
	sort.SliceStable(out, func(i, j int) bool {
		ti := strings.TrimSpace(asJobString(out[i]["timestamp"]))
		tj := strings.TrimSpace(asJobString(out[j]["timestamp"]))
		return ti > tj
	})
	return out
}

func buildJobLogEvents(jobID string, rawLogs interface{}) []map[string]interface{} {
	switch logs := rawLogs.(type) {
	case []queue.JobLogEntry:
		out := make([]map[string]interface{}, 0, len(logs))
		for _, row := range logs {
			msg := strings.TrimSpace(row.Message)
			if msg == "" {
				continue
			}
			ts := strings.TrimSpace(row.Timestamp)
			if ts == "" {
				ts = strings.TrimSpace(row.Time)
			}
			if ts == "" {
				ts = time.Now().UTC().Format(time.RFC3339)
			}
			eventType := "worker_log"
			if strings.TrimSpace(row.Level) != "" {
				eventType = strings.ToLower(strings.TrimSpace(row.Level))
			}
			if row.IsError {
				eventType = "error"
			}
			out = append(out, map[string]interface{}{
				"timestamp":  ts,
				"job_id":     jobID,
				"event":      eventType,
				"event_type": eventType,
				"message":    msg,
				"worker_id":  row.WorkerID,
				"source":     "job_logs",
			})
		}
		return out
	case []interface{}:
		out := make([]map[string]interface{}, 0, len(logs))
		for _, row := range logs {
			m, ok := row.(map[string]interface{})
			if !ok {
				continue
			}
			msg := strings.TrimSpace(asJobString(m["message"]))
			if msg == "" {
				continue
			}
			ts := strings.TrimSpace(asJobString(m["timestamp"]))
			if ts == "" {
				ts = strings.TrimSpace(asJobString(m["time"]))
			}
			if ts == "" {
				ts = time.Now().UTC().Format(time.RFC3339)
			}
			workerID := strings.TrimSpace(asJobString(m["worker_id"]))
			eventType := "worker_log"
			if strings.TrimSpace(asJobString(m["level"])) != "" {
				eventType = strings.ToLower(strings.TrimSpace(asJobString(m["level"])))
			}
			if v, ok := m["is_error"].(bool); ok && v {
				eventType = "error"
			}
			out = append(out, map[string]interface{}{
				"timestamp":  ts,
				"job_id":     jobID,
				"event":      eventType,
				"event_type": eventType,
				"message":    msg,
				"worker_id":  workerID,
				"source":     "job_logs",
			})
		}
		return out
	default:
		return nil
	}
}

func extractWorkerLogEntries(output map[string]interface{}, workerID string) []queue.JobLogEntry {
	if len(output) == 0 {
		return nil
	}
	now := time.Now().UTC().Format(time.RFC3339)
	var lines []string

	candidates := []string{"logs", "progress_logs", "processing_logs", "events", "validation_details"}
	for _, key := range candidates {
		v, ok := output[key]
		if !ok || v == nil {
			continue
		}
		switch vv := v.(type) {
		case []interface{}:
			for _, it := range vv {
				if s, ok := it.(string); ok && strings.TrimSpace(s) != "" {
					lines = append(lines, strings.TrimSpace(s))
				}
			}
		case []string:
			for _, s := range vv {
				if strings.TrimSpace(s) != "" {
					lines = append(lines, strings.TrimSpace(s))
				}
			}
		}
	}
	for _, key := range []string{"status_log"} {
		v, ok := output[key]
		if !ok || v == nil {
			continue
		}
		if s, ok := v.(string); ok {
			for _, part := range strings.Split(s, "\n") {
				part = strings.TrimSpace(part)
				if part != "" {
					lines = append(lines, part)
				}
			}
		}
	}

	if len(lines) == 0 {
		return nil
	}

	entries := make([]queue.JobLogEntry, 0, len(lines))
	for _, line := range lines {
		entries = append(entries, queue.JobLogEntry{
			Timestamp: now,
			Time:      now,
			Message:   line,
			WorkerID:  workerID,
		})
	}
	return entries
}

func extractOutputVideoPath(output map[string]interface{}) string {
	if len(output) == 0 {
		return ""
	}
	for _, key := range []string{"master_video_path", "output_path", "result_path", "video_path"} {
		if s, ok := output[key].(string); ok && strings.TrimSpace(s) != "" {
			return strings.TrimSpace(s)
		}
	}
	if nested, ok := output["result"].(map[string]interface{}); ok {
		return extractOutputVideoPath(nested)
	}
	return ""
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func asJobString(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
