package calendar

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"velox-server/internal/jobs"
	"velox-server/internal/store"
	"velox-server/internal/taskgraph"
)

func (api *CalendarAPI) reconcileCalendarEvent(ctx context.Context, event *store.CalendarEvent, force bool) error {
	if api == nil || api.reader == nil || event == nil {
		return nil
	}

	desired := calendarDesiredStatus(event)
	if desired == "cancelled" || desired == "completed" || desired == "published_manual" {
		event.Status = desired
		return nil
	}

	due := force || calendarEventDue(event)
	if desired == "needs_script" || desired == "needs_assets" {
		event.Status = desired
		event.JobStatus = "WAITING_ASSETS"
		event.QueueError = "waiting for clips and voiceover/audio"
		return nil
	}

	if !due {
		event.Status = "scheduled"
		event.JobStatus = strings.TrimSpace(event.JobStatus)
		event.QueueError = ""
		return nil
	}

	if strings.TrimSpace(event.JobID) == "" {
		event.JobID = "cal_" + uuid.NewString()
	}

	jobPayload := buildCalendarJobPayload(event, "")
	j, err := api.reader.Get(ctx, event.JobID)
	existing := jobs.ToQueueItem(j)
	if err == nil && existing != nil && existing.Status == jobs.StatusPending {
		jobPayload = buildCalendarJobPayload(event, existingJobRunID(existing))
		if existing.Payload != nil {
			for k, v := range jobPayload {
				existing.Payload[k] = v
			}
		}
		if err := persistJobResult(existing, api.store); err != nil {
			return err
		}
		applyQueueStateToEvent(ctx, event, existing, api.store)
		return nil
	}
	if existing != nil && (existing.Status == jobs.StatusRunning || existing.Status == jobs.StatusLeased) {
		jobPayload = buildCalendarJobPayload(event, existingJobRunID(existing))
		jobPayload["status"] = string(existing.Status)
		if existing.Payload != nil {
			for k, v := range jobPayload {
				existing.Payload[k] = v
			}
		}
		if err := persistJobResult(existing, api.store); err != nil {
			return err
		}
		applyQueueStateToEvent(ctx, event, existing, api.store)
		return nil
	}
	if existing != nil && existing.Status != jobs.StatusPending && existing.Status != jobs.StatusRunning && existing.Status != jobs.StatusLeased {
		event.JobID = "cal_" + uuid.NewString()
		jobPayload = buildCalendarJobPayload(event, "")
	}
	if err := submitCalendarJob(ctx, api.atomic, event.JobID, jobPayload); err != nil {
		return err
	}
	event.Status = "queued"
	event.JobStatus = string(jobs.StatusPending)
	event.QueueError = ""
	event.PublishStatus = "manual"
	if strings.TrimSpace(event.QueuedAt) == "" {
		event.QueuedAt = time.Now().UTC().Format(time.RFC3339)
	}
	return nil
}

func calendarDesiredStatus(event *store.CalendarEvent) string {
	if event == nil {
		return "draft"
	}
	hasScript := strings.TrimSpace(event.ScriptText) != "" || len(event.Titles) > 0 || len(event.VoiceoverPaths) > 0
	hasClip := len(event.StockFootage) > 0 || len(event.InitialClips) > 0 || len(event.IntermediateClips) > 0 || len(event.FinalClips) > 0
	switch {
	case !hasScript:
		return "needs_script"
	case !hasClip:
		return "needs_assets"
	default:
		return "ready_for_queue"
	}
}

func calendarEventDue(event *store.CalendarEvent) bool {
	if event == nil {
		return false
	}
	if event.Year <= 0 || event.Month <= 0 || event.Date <= 0 {
		return true
	}
	eventTime := time.Date(event.Year, time.Month(event.Month), event.Date, 0, 0, 0, 0, time.UTC)
	return !eventTime.After(time.Now().UTC())
}

func applyQueueStateToEvent(ctx context.Context, event *store.CalendarEvent, job *jobs.QueueItem, dbStore *store.SQLiteStore) {
	if event == nil || job == nil {
		return
	}
	event.JobStatus = string(job.Status)
	event.QueueError = job.LastError
	event.PublishStatus = "manual"
	switch v := job.CreatedAt.(type) {
	case string:
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			event.QueuedAt = t.UTC().Format(time.RFC3339)
		}
	case int64:
		event.QueuedAt = time.Unix(v, 0).UTC().Format(time.RFC3339)
	case float64:
		event.QueuedAt = time.Unix(int64(v), 0).UTC().Format(time.RFC3339)
	}
	switch job.Status {
	case jobs.StatusPending:
		event.Status = "queued"
	case jobs.StatusRunning, jobs.StatusLeased:
		event.Status = "processing"
	case jobs.StatusSucceeded:
		event.Status = "completed"
		artifacts, _ := dbStore.GetArtifactsByJob(job.JobID, 5)
		for _, a := range artifacts {
			if a.Status == "READY" {
				if a.LocalPath != "" {
					event.OutputVideoPath = a.LocalPath
				} else if a.StorageKey != "" {
					event.OutputVideoPath = a.StorageKey
				}
				deliveries, _ := dbStore.ListJobDeliveriesByJob(job.JobID)
				for _, d := range deliveries {
					if d.ArtifactID == a.ID && d.RemoteURL != "" {
						event.OutputVideoURL = d.RemoteURL
						break
					}
				}
				break
			}
		}
	case jobs.StatusFailed:
		event.Status = "failed"
	}
}

// submitCalendarJob creates a new job via AtomicJobTaskCreator (Job+Task atomically).
// PR #3: replaces jobs.Writer.Create (Job-only) with the single atomic creation path.
func submitCalendarJob(ctx context.Context, atomic *store.AtomicJobTaskCreator, jobID string, payload map[string]interface{}) error {
	if atomic == nil {
		return fmt.Errorf("submit calendar job: creator is nil")
	}
	var videoName, projectID, runID string
	if s, ok := payload["video_name"].(string); ok {
		videoName = s
	}
	if s, ok := payload["project_id"].(string); ok {
		projectID = s
	}
	if s, ok := payload["job_run_id"].(string); ok && s != "" {
		runID = s
	} else if s, ok := payload["run_id"].(string); ok && s != "" {
		runID = s
	}
	raw, _ := json.Marshal(payload)
	job := &jobs.Job{
		ID:         jobID,
		Status:     jobs.StatusPending,
		VideoName:  videoName,
		ProjectID:  projectID,
		RunID:      runID,
		MaxRetries: 3,
		Payload:    string(raw),
	}
	spec := &taskgraph.TaskSpec{
		Version:    taskgraph.SpecVersion,
		JobID:      jobID,
		ExecutorID: "scene.composite.v1@1",
		Payload:    payload,
	}
	priority := 5
	return atomic.CreateJobWithTask(ctx, job, spec, priority)
}

// persistJobResult saves mutable job state to result_json via SQLiteStore
// (replaces queue.PersistJob).
func persistJobResult(job *jobs.QueueItem, dbStore *store.SQLiteStore) error {
	if job == nil || dbStore == nil {
		return nil
	}
	m := make(map[string]interface{})
	m["job_id"] = job.JobID
	m["status"] = string(job.Status)
	m["video_name"] = job.VideoName
	m["project_id"] = job.ProjectID
	m["max_retries"] = job.MaxRetries
	m["last_error"] = job.LastError
	m["error_message"] = job.ErrorMessage
	m["run_id"] = job.RunID
	m["created_at"] = job.CreatedAt
	m["updated_at"] = job.UpdatedAt
	m["started_at"] = job.StartedAt
	m["completed_at"] = job.CompletedAt
	if job.Payload != nil {
		for k, v := range job.Payload {
			if _, exists := m[k]; !exists {
				m[k] = v
			}
		}
	}
	rawJSON, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("failed to marshal result_json: %w", err)
	}
	return dbStore.UpsertJobResult(job.JobID, rawJSON)
}
