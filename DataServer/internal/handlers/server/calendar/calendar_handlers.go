package calendar

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/jobs"
	"velox-server/internal/store"
)

// CalendarAPI provides handlers for calendar event operations
type CalendarAPI struct {
	store    *store.SQLiteStore
	reader   jobs.Reader
	writer   jobs.Writer
	scheduler *CalendarScheduler
}

// NewCalendarAPI creates a new CalendarAPI instance
func NewCalendarAPI(s *store.SQLiteStore, reader jobs.Reader, writer jobs.Writer, sched *CalendarScheduler) *CalendarAPI {
	return &CalendarAPI{store: s, reader: reader, writer: writer, scheduler: sched}
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
func (api *CalendarAPI) UpsertEvent() gin.HandlerFunc {
	return func(c *gin.Context) {
		var event store.CalendarEvent
		if err := c.ShouldBindJSON(&event); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

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

		etag := generateETag(events, minimal)
		c.Header("ETag", etag)
		c.Header("Cache-Control", "public, max-age=30, stale-while-revalidate=300")

		ifNoneMatch := c.GetHeader("If-None-Match")
		if ifNoneMatch == etag {
			c.Status(http.StatusNotModified)
			return
		}

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

// EnqueueEvent handles POST /api/v1/calendar/events/:id/enqueue
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

// RegisterRoutes registers all calendar routes
func RegisterRoutes(r *gin.RouterGroup, s *store.SQLiteStore, reader jobs.Reader, writer jobs.Writer, sched *CalendarScheduler) {
	api := NewCalendarAPI(s, reader, writer, sched)

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
	if api == nil || api.reader == nil {
		return
	}
	for _, event := range events {
		if event == nil || strings.TrimSpace(event.JobID) == "" {
			continue
		}
		j, err := api.reader.Get(ctx, event.JobID)
		if err != nil || j == nil {
			continue
		}
		job := jobs.ToQueueItem(j)
		applyQueueStateToEvent(ctx, event, job, api.store)
	}
}
