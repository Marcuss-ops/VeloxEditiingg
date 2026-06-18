package calendar

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"

	"velox-server/internal/compat"
	"velox-server/internal/queue"
	"velox-server/internal/store"
)

func (api *CalendarAPI) reconcileCalendarEvent(ctx context.Context, event *store.CalendarEvent, force bool) error {
	if api == nil || api.queue == nil || event == nil {
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
	existing, err := api.queue.GetJob(ctx, event.JobID)
	if err == nil && existing != nil && existing.Status == queue.StatusPending {
		jobPayload = buildCalendarJobPayload(event, existingJobRunID(existing))
		// Update the job payload for pending jobs via persist
		if existing.Payload != nil {
			for k, v := range jobPayload {
				existing.Payload[k] = v
			}
		}
		if err := queue.PersistJob(existing, api.store); err != nil {
			return err
		}
		applyQueueStateToEvent(ctx, event, existing, api.store)
		return nil
	}
	if existing != nil && (existing.Status == queue.StatusRunning || existing.Status == queue.StatusLeased) {
		jobPayload = buildCalendarJobPayload(event, existingJobRunID(existing))
		jobPayload["status"] = string(existing.Status)
		// Update the job payload for running jobs via persist
		if existing.Payload != nil {
			for k, v := range jobPayload {
				existing.Payload[k] = v
			}
		}
		if err := queue.PersistJob(existing, api.store); err != nil {
			return err
		}
		applyQueueStateToEvent(ctx, event, existing, api.store)
		return nil
	}
	if existing != nil && existing.Status != queue.StatusPending && existing.Status != queue.StatusRunning && existing.Status != queue.StatusLeased {
		event.JobID = "cal_" + uuid.NewString()
		jobPayload = buildCalendarJobPayload(event, "")
	}
	if err := api.queue.SubmitJob(ctx, event.JobID, jobPayload); err != nil {
		return err
	}
	event.Status = "queued"
	event.JobStatus = string(queue.StatusPending)
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

func applyQueueStateToEvent(ctx context.Context, event *store.CalendarEvent, job *queue.Job, dbStore *store.SQLiteStore) {
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
	case queue.StatusPending:
		event.Status = "queued"
	case queue.StatusRunning, queue.StatusLeased:
		event.Status = "processing"
	case queue.StatusSucceeded:
		event.Status = "completed"
		if view, err := compat.AssembleLegacyJobView(ctx, dbStore, job); err == nil && view != nil {
			event.OutputVideoPath = view.MasterVideoPath
			event.OutputVideoURL = view.DriveURL
		}
	case queue.StatusFailed:
		event.Status = "failed"
	}
}
