package calendar

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"velox-server/internal/queue"
	"velox-server/internal/store"
)

// CalendarAPI provides handlers for calendar event operations
type CalendarAPI struct {
	store     *store.SQLiteStore
	queue     *queue.FileQueue
	scheduler *CalendarScheduler
}

// NewCalendarAPI creates a new CalendarAPI instance
func NewCalendarAPI(s *store.SQLiteStore, q *queue.FileQueue, sched *CalendarScheduler) *CalendarAPI {
	return &CalendarAPI{store: s, queue: q, scheduler: sched}
}

// MinimalEvent is a lightweight event representation for fast list queries
type MinimalEvent struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	Date  int    `json:"date"`
	Month int    `json:"month"`
	Year  int    `json:"year"`
}

// ListEvents handles GET /api/v1/calendar/events
func (api *CalendarAPI) ListEvents() gin.HandlerFunc {
	return func(c *gin.Context) {
		filter := store.CalendarEventFilter{}

		// Parse query parameters
		if search := c.Query("search"); search != "" {
			filter.Search = search
		}
		if month := c.Query("month"); month != "" {
			if m, err := strconv.Atoi(month); err == nil {
				filter.Month = &m
			}
		}
		if year := c.Query("year"); year != "" {
			if y, err := strconv.Atoi(year); err == nil {
				filter.Year = &y
			}
		}
		if hasClips := c.Query("has_clips"); hasClips == "true" {
			filter.HasClips = true
		}
		if limit := c.Query("limit"); limit != "" {
			if l, err := strconv.Atoi(limit); err == nil {
				filter.Limit = l
			}
		}
		if offset := c.Query("offset"); offset != "" {
			if o, err := strconv.Atoi(offset); err == nil {
				filter.Offset = o
			}
		}

		events, err := api.store.ListCalendarEvents(c.Request.Context(), filter)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		api.hydrateQueueState(c.Request.Context(), events)

		c.JSON(http.StatusOK, gin.H{
			"events": events,
			"count":  len(events),
		})
	}
}

// GetEvent handles GET /api/v1/calendar/events/:id
func (api *CalendarAPI) GetEvent() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.Param("id")
		if id == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "id required"})
			return
		}

		event, err := api.store.GetCalendarEvent(c.Request.Context(), id)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if event == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "event not found"})
			return
		}

		api.hydrateQueueState(c.Request.Context(), []*store.CalendarEvent{event})
		c.JSON(http.StatusOK, event)
	}
}

// CreateEvent handles POST /api/v1/calendar/events
func (api *CalendarAPI) CreateEvent() gin.HandlerFunc {
	return func(c *gin.Context) {
		var event store.CalendarEvent
		if err := c.ShouldBindJSON(&event); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// Validate required fields
		if event.Title == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "title required"})
			return
		}

		existing, err := api.findExistingEvent(c.Request.Context(), &event)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if existing != nil {
			event = *mergeCalendarEvent(existing, &event)
		}

		created := existing == nil
		if created {
			if err := api.store.CreateCalendarEvent(c.Request.Context(), &event); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
		} else if err := api.store.UpdateCalendarEvent(c.Request.Context(), &event); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		if err := api.reconcileCalendarEvent(c.Request.Context(), &event, false); err != nil {
			event.QueueError = err.Error()
		}
		if err := api.store.UpdateCalendarEvent(c.Request.Context(), &event); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		api.hydrateQueueState(c.Request.Context(), []*store.CalendarEvent{&event})

		if created {
			c.JSON(http.StatusCreated, event)
			return
		}
		c.JSON(http.StatusOK, event)
	}
}

// UpdateEvent handles PUT /api/v1/calendar/events/:id
func (api *CalendarAPI) UpdateEvent() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.Param("id")
		if id == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "id required"})
			return
		}

		var event store.CalendarEvent
		if err := c.ShouldBindJSON(&event); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// Ensure ID matches
		event.ID = id
		existing, err := api.store.GetCalendarEvent(c.Request.Context(), id)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if existing == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "event not found"})
			return
		}
		event = *mergeCalendarEvent(existing, &event)

		if err := api.store.UpdateCalendarEvent(c.Request.Context(), &event); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		if err := api.reconcileCalendarEvent(c.Request.Context(), &event, false); err != nil {
			event.QueueError = err.Error()
		}
		_ = api.store.UpdateCalendarEvent(c.Request.Context(), &event)
		api.hydrateQueueState(c.Request.Context(), []*store.CalendarEvent{&event})

		c.JSON(http.StatusOK, event)
	}
}

// DeleteEvent handles DELETE /api/v1/calendar/events/:id
func (api *CalendarAPI) DeleteEvent() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.Param("id")
		if id == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "id required"})
			return
		}

		if err := api.store.DeleteCalendarEvent(c.Request.Context(), id); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"success": true})
	}
}

// UpsertEvent handles POST /api/v1/calendar/events/upsert
// Creates or updates an event based on date/month/year
func (api *CalendarAPI) UpsertEvent() gin.HandlerFunc {
	return func(c *gin.Context) {
		var event store.CalendarEvent
		if err := c.ShouldBindJSON(&event); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// Validate required fields
		if event.Title == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "title required"})
			return
		}

		existing, err := api.findExistingEvent(c.Request.Context(), &event)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if existing != nil {
			event = *mergeCalendarEvent(existing, &event)
		}
		if event.Status == "" {
			event.Status = "draft"
		}

		created := existing == nil
		if created {
			if err := api.store.CreateCalendarEvent(c.Request.Context(), &event); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
		} else if err := api.store.UpdateCalendarEvent(c.Request.Context(), &event); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		if err := api.reconcileCalendarEvent(c.Request.Context(), &event, false); err != nil {
			event.QueueError = err.Error()
		}
		if err := api.store.UpdateCalendarEvent(c.Request.Context(), &event); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		api.hydrateQueueState(c.Request.Context(), []*store.CalendarEvent{&event})

		if created {
			c.JSON(http.StatusCreated, event)
			return
		}
		c.JSON(http.StatusOK, event)
	}
}

// GetEventsByDateRange handles GET /api/v1/calendar/events/range
// Supports ?fields=minimal to return lightweight events (id, title, date, month, year only)
// Supports If-None-Match for conditional requests (ETag caching)
func (api *CalendarAPI) GetEventsByDateRange() gin.HandlerFunc {
	return func(c *gin.Context) {
		startMonth, _ := strconv.Atoi(c.Query("start_month"))
		startYear, _ := strconv.Atoi(c.Query("start_year"))
		endMonth, _ := strconv.Atoi(c.Query("end_month"))
		endYear, _ := strconv.Atoi(c.Query("end_year"))
		fields := c.Query("fields")
		minimal := strings.ToLower(fields) == "minimal"

		if startYear == 0 || endYear == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "start_year and end_year required"})
			return
		}

		events, err := api.store.GetCalendarEventsByDateRange(
			c.Request.Context(),
			startMonth, startYear, endMonth, endYear,
		)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		api.hydrateQueueState(c.Request.Context(), events)

		// Generate ETag for conditional requests
		etag := generateETag(events, minimal)
		c.Header("ETag", etag)
		c.Header("Cache-Control", "public, max-age=30, stale-while-revalidate=300")

		// Check If-None-Match
		ifNoneMatch := c.GetHeader("If-None-Match")
		if ifNoneMatch == etag {
			c.Status(http.StatusNotModified)
			return
		}

		// Return minimal events to reduce payload size (~40% smaller)
		if minimal {
			minimalEvents := make([]MinimalEvent, 0, len(events))
			for _, e := range events {
				minimalEvents = append(minimalEvents, MinimalEvent{
					ID:    e.ID,
					Title: e.Title,
					Date:  e.Date,
					Month: e.Month,
					Year:  e.Year,
				})
			}
			c.JSON(http.StatusOK, gin.H{
				"events": minimalEvents,
				"count":  len(minimalEvents),
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"events": events,
			"count":  len(events),
		})
	}
}

// generateETag creates a weak ETag from events data for conditional requests
func generateETag(events []*store.CalendarEvent, minimal bool) string {
	h := sha256.New()
	for _, e := range events {
		fmt.Fprintf(h, "%s-%d-%d-%d-%d-%s-%s-%s-%s", e.ID, e.Date, e.Month, e.Year, len(e.Title), e.Status, e.JobID, e.JobStatus, e.UpdatedAt.UTC().Format(time.RFC3339))
	}
	hash := hex.EncodeToString(h.Sum(nil))[:16]
	return fmt.Sprintf("W/\"cal-%s-%d\"", hash, len(events))
}

func (api *CalendarAPI) EnqueueEvent() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.Param("id")
		if id == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "id required"})
			return
		}

		event, err := api.store.GetCalendarEvent(c.Request.Context(), id)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if event == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "event not found"})
			return
		}

		if err := api.reconcileCalendarEvent(c.Request.Context(), event, true); err != nil {
			event.QueueError = err.Error()
			_ = api.store.UpdateCalendarEvent(c.Request.Context(), event)
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error(), "event": event})
			return
		}

		if err := api.store.UpdateCalendarEvent(c.Request.Context(), event); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		api.hydrateQueueState(c.Request.Context(), []*store.CalendarEvent{event})
		c.JSON(http.StatusOK, event)
	}
}

func (api *CalendarAPI) findExistingEvent(ctx context.Context, event *store.CalendarEvent) (*store.CalendarEvent, error) {
	if event == nil {
		return nil, nil
	}
	if strings.TrimSpace(event.ID) != "" {
		existing, err := api.store.GetCalendarEvent(ctx, event.ID)
		if err != nil {
			return nil, err
		}
		if existing != nil {
			return existing, nil
		}
	}
	if strings.TrimSpace(event.ExternalID) != "" {
		existing, err := api.store.GetCalendarEventByExternalID(ctx, event.ExternalID)
		if err != nil {
			return nil, err
		}
		if existing != nil {
			return existing, nil
		}
	}
	return nil, nil
}

// RegisterRoutes registers all calendar routes
func RegisterRoutes(r *gin.RouterGroup, s *store.SQLiteStore, q *queue.FileQueue, sched *CalendarScheduler) {
	api := NewCalendarAPI(s, q, sched)

	r.GET("/calendar/events", api.ListEvents())
	r.GET("/calendar/events/range", api.GetEventsByDateRange())
	r.GET("/calendar/events/:id", api.GetEvent())
	r.POST("/calendar/events", api.CreateEvent())
	r.POST("/calendar/events/upsert", api.UpsertEvent())
	r.POST("/calendar/events/:id/enqueue", api.EnqueueEvent())
	r.PUT("/calendar/events/:id", api.UpdateEvent())
	r.DELETE("/calendar/events/:id", api.DeleteEvent())
}

func (api *CalendarAPI) hydrateQueueState(ctx context.Context, events []*store.CalendarEvent) {
	if api == nil || api.queue == nil {
		return
	}
	for _, event := range events {
		if event == nil || strings.TrimSpace(event.JobID) == "" {
			continue
		}
		job, err := api.queue.GetJob(ctx, event.JobID)
		if err != nil || job == nil {
			continue
		}
		applyQueueStateToEvent(event, job)
	}
}

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
		if err := api.queue.UpdateJobFields(ctx, event.JobID, jobPayload); err != nil {
			return err
		}
		applyQueueStateToEvent(event, existing)
		return nil
	}
	if existing != nil && existing.Status == queue.StatusProcessing {
		jobPayload = buildCalendarJobPayload(event, existingJobRunID(existing))
		jobPayload["status"] = string(existing.Status)
		if err := api.queue.UpdateJobFields(ctx, event.JobID, jobPayload); err != nil {
			return err
		}
		applyQueueStateToEvent(event, existing)
		return nil
	}
	if existing != nil && existing.Status != queue.StatusPending && existing.Status != queue.StatusProcessing {
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

func applyQueueStateToEvent(event *store.CalendarEvent, job *queue.Job) {
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
	case queue.StatusProcessing:
		event.Status = "processing"
	case queue.StatusCompleted:
		event.Status = "completed"
		event.OutputVideoPath = job.MasterVideoPath
		event.OutputVideoURL = job.DriveURL
	case queue.StatusError, queue.StatusFailed:
		event.Status = "failed"
	}
}

func mergeCalendarEvent(existing, incoming *store.CalendarEvent) *store.CalendarEvent {
	if existing == nil {
		if incoming == nil {
			return nil
		}
		clone := *incoming
		return &clone
	}
	merged := *existing
	if incoming == nil {
		return &merged
	}
	if strings.TrimSpace(incoming.ExternalID) != "" {
		merged.ExternalID = incoming.ExternalID
	}
	if strings.TrimSpace(incoming.Source) != "" {
		merged.Source = incoming.Source
	}
	if strings.TrimSpace(incoming.Title) != "" {
		merged.Title = incoming.Title
	}
	if incoming.Date != 0 {
		merged.Date = incoming.Date
	}
	if incoming.Month != 0 {
		merged.Month = incoming.Month
	}
	if incoming.Year != 0 {
		merged.Year = incoming.Year
	}
	if strings.TrimSpace(incoming.Status) != "" {
		merged.Status = incoming.Status
	}
	if strings.TrimSpace(incoming.YouTubeGroup) != "" {
		merged.YouTubeGroup = incoming.YouTubeGroup
	}
	if incoming.StockFootage != nil {
		merged.StockFootage = incoming.StockFootage
	}
	if incoming.InitialClips != nil {
		merged.InitialClips = incoming.InitialClips
	}
	if incoming.IntermediateClips != nil {
		merged.IntermediateClips = incoming.IntermediateClips
	}
	if incoming.FinalClips != nil {
		merged.FinalClips = incoming.FinalClips
	}
	if incoming.VoiceoverPaths != nil {
		merged.VoiceoverPaths = incoming.VoiceoverPaths
	}
	if incoming.Titles != nil {
		merged.Titles = incoming.Titles
	}
	if strings.TrimSpace(incoming.ScriptText) != "" {
		merged.ScriptText = incoming.ScriptText
	}
	if incoming.YouTubeLinks != nil {
		merged.YouTubeLinks = incoming.YouTubeLinks
	}
	if strings.TrimSpace(incoming.Category) != "" {
		merged.Category = incoming.Category
	}
	if strings.TrimSpace(incoming.JobID) != "" {
		merged.JobID = incoming.JobID
	}
	if strings.TrimSpace(incoming.JobStatus) != "" {
		merged.JobStatus = incoming.JobStatus
	}
	if strings.TrimSpace(incoming.QueuedAt) != "" {
		merged.QueuedAt = incoming.QueuedAt
	}
	if strings.TrimSpace(incoming.QueueError) != "" {
		merged.QueueError = incoming.QueueError
	}
	if strings.TrimSpace(incoming.PublishStatus) != "" {
		merged.PublishStatus = incoming.PublishStatus
	}
	if strings.TrimSpace(incoming.OutputVideoPath) != "" {
		merged.OutputVideoPath = incoming.OutputVideoPath
	}
	if strings.TrimSpace(incoming.OutputVideoURL) != "" {
		merged.OutputVideoURL = incoming.OutputVideoURL
	}
	return &merged
}

func buildCalendarJobPayload(event *store.CalendarEvent, jobRunID string) map[string]interface{} {
	clipPaths := func(clips []store.VideoClip) []string {
		out := make([]string, 0, len(clips))
		for _, clip := range clips {
			path := calendarClipPath(clip)
			if path != "" {
				out = append(out, path)
			}
		}
		return out
	}

	voiceovers := make([]string, 0, len(event.VoiceoverPaths))
	for _, s := range event.VoiceoverPaths {
		if trimmed := strings.TrimSpace(s); trimmed != "" {
			voiceovers = append(voiceovers, trimmed)
		}
	}

	parameters := map[string]interface{}{
		"calendar_event_id":    event.ID,
		"external_id":          event.ExternalID,
		"source":               event.Source,
		"calendar_event_date":  event.Date,
		"calendar_event_month": event.Month,
		"calendar_event_year":  event.Year,
		"job_run_id":           jobRunID,
		"script_text":          event.ScriptText,
		"titles":               event.Titles,
		"youtube_links":        event.YouTubeLinks,
		"youtube_group":        event.YouTubeGroup,
		"category":             event.Category,
		"start_clip_paths":     clipPaths(event.InitialClips),
		"middle_clip_paths":    clipPaths(event.IntermediateClips),
		"end_clip_paths":       clipPaths(event.FinalClips),
		"stock_clip_paths":     clipPaths(event.StockFootage),
		"voiceover_paths":      voiceovers,
	}
	if len(voiceovers) > 0 {
		parameters["audio_path"] = voiceovers[0]
	}

	if strings.TrimSpace(jobRunID) == "" {
		jobRunID = "run_" + uuid.NewString()
	}
	parameters["job_run_id"] = jobRunID
	createdAt := time.Now().UTC().Format(time.RFC3339)
	payload := map[string]interface{}{
		"job_id":            event.JobID,
		"job_run_id":        jobRunID,
		"job_type":          "process_video",
		"priority":          1,
		"created_at":        createdAt,
		"timeout_secs":      1800,
		"video_name":        event.Title,
		"project_id":        event.ID,
		"youtube_group":     event.YouTubeGroup,
		"status":            "PENDING",
		"submitted_via":     "calendar",
		"source":            event.Source,
		"external_id":       event.ExternalID,
		"calendar_event_id": event.ID,
		"calendar_date":     event.Date,
		"parameters":        parameters,
	}
	return payload
}

func existingJobRunID(job *queue.Job) string {
	if job == nil || job.Payload == nil {
		return ""
	}
	if v, ok := job.Payload["job_run_id"].(string); ok {
		return strings.TrimSpace(v)
	}
	return ""
}

func calendarClipPath(clip store.VideoClip) string {
	for _, candidate := range []string{clip.Path, clip.URL, clip.WebView} {
		if trimmed := strings.TrimSpace(candidate); trimmed != "" {
			return trimmed
		}
	}
	if strings.TrimSpace(clip.DriveID) != "" {
		return "/api/drive/media/" + strings.TrimSpace(clip.DriveID)
	}
	return strings.TrimSpace(clip.Name)
}
