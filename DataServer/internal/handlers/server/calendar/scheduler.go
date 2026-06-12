package calendar

import (
	"context"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"velox-server/internal/queue"
	"velox-server/internal/store"
)

// CalendarScheduler reconciles calendar events into queue/state transitions.
type CalendarScheduler struct {
	store    *store.SQLiteStore
	queue    *queue.FileQueue
	interval time.Duration
	api      *CalendarAPI
}

// NewCalendarScheduler creates a scheduler with a sane default interval.
func NewCalendarScheduler(s *store.SQLiteStore, q *queue.FileQueue) *CalendarScheduler {
	interval := 30 * time.Second
	if raw := strings.TrimSpace(os.Getenv("VELOX_CALENDAR_SCHEDULER_INTERVAL_SECONDS")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			interval = time.Duration(n) * time.Second
		}
	}
	return &CalendarScheduler{
		store:    s,
		queue:    q,
		interval: interval,
		api:      &CalendarAPI{store: s, queue: q},
	}
}

// Start runs the initial reconciliation and keeps reconciling until ctx is done.
func (s *CalendarScheduler) Start(ctx context.Context) {
	if s == nil || s.store == nil {
		return
	}

	if _, err := s.ReconcileAll(ctx); err != nil {
		log.Printf("calendar scheduler initial reconcile failed: %v", err)
	}

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := s.ReconcileAll(ctx); err != nil {
				log.Printf("calendar scheduler reconcile failed: %v", err)
			}
		}
	}
}

// ReconcileAll applies the scheduler rules to every calendar event.
func (s *CalendarScheduler) ReconcileAll(ctx context.Context) (int, error) {
	if s == nil || s.store == nil {
		return 0, nil
	}

	const pageSize = 200
	changed := 0
	for offset := 0; ; offset += pageSize {
		events, err := s.store.ListCalendarEvents(ctx, store.CalendarEventFilter{
			Limit:  pageSize,
			Offset: offset,
		})
		if err != nil {
			return changed, err
		}
		if len(events) == 0 {
			break
		}
		for _, event := range events {
			didChange, err := s.ReconcileEvent(ctx, event, false)
			if err != nil {
				return changed, err
			}
			if didChange {
				changed++
			}
		}
		if len(events) < pageSize {
			break
		}
	}
	return changed, nil
}

// ReconcileEvent updates one event according to the scheduler rules.
func (s *CalendarScheduler) ReconcileEvent(ctx context.Context, event *store.CalendarEvent, force bool) (bool, error) {
	if s == nil || s.store == nil || event == nil {
		return false, nil
	}

	before := *event

	if s.api != nil {
		s.api.hydrateQueueState(ctx, []*store.CalendarEvent{event})
	}

	if strings.EqualFold(strings.TrimSpace(event.Status), "cancelled") ||
		strings.EqualFold(strings.TrimSpace(event.Status), "published_manual") {
		if err := s.store.UpdateCalendarEvent(ctx, event); err != nil {
			return false, err
		}
		return !calendarEventsEqual(before, *event), nil
	}

	if strings.EqualFold(strings.TrimSpace(event.Status), "completed") && strings.TrimSpace(event.JobID) == "" {
		if err := s.store.UpdateCalendarEvent(ctx, event); err != nil {
			return false, err
		}
		return !calendarEventsEqual(before, *event), nil
	}

	if s.api != nil {
		if err := s.api.reconcileCalendarEvent(ctx, event, force); err != nil {
			return false, err
		}
	}

	if err := s.store.UpdateCalendarEvent(ctx, event); err != nil {
		return false, err
	}

	return !calendarEventsEqual(before, *event), nil
}

// ReconcileDueNow is a convenience helper used by tests and manual triggers.
func (s *CalendarScheduler) ReconcileDueNow(ctx context.Context) (int, error) {
	return s.ReconcileAll(ctx)
}

func calendarEventsEqual(a, b store.CalendarEvent) bool {
	return a.ID == b.ID &&
		a.ExternalID == b.ExternalID &&
		a.Source == b.Source &&
		a.Title == b.Title &&
		a.Date == b.Date &&
		a.Month == b.Month &&
		a.Year == b.Year &&
		a.Status == b.Status &&
		a.YouTubeGroup == b.YouTubeGroup &&
		a.ScriptText == b.ScriptText &&
		a.Category == b.Category &&
		a.JobID == b.JobID &&
		a.JobStatus == b.JobStatus &&
		a.QueuedAt == b.QueuedAt &&
		a.QueueError == b.QueueError &&
		a.OutputVideoPath == b.OutputVideoPath &&
		a.OutputVideoURL == b.OutputVideoURL &&
		a.PublishStatus == b.PublishStatus
}
