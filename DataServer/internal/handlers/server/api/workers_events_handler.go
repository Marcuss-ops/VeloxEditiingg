// Package api — RW-PROD-005 events endpoint.
//
// workers_events_handler.go owns the GET /api/v1/workers/:worker_id/events
// handler. It exposes the per-worker audit ledger for dashboards /
// on-call tooling.
//
// SECURITY posture (canonical, see OWNERSHIP.md §3):
//   - details_json is the raw audit detail blob. The handler parses
//     it into map[string]any and routes every string value through
//     sanitiseHostname() so an embedded IP / path / long-hex cannot
//     leak. Parse failures fall back to the raw string (still
//     sanitised) so the audit ledger is NEVER silently dropped —
//     a malformed detail is the operator's signal that the producer
//     wrote something unexpected.
//   - event_type / severity / reason_code are the canonical
//     operators-controlled taxonomy; surfaced verbatim.
//
// File layout: see workers_metrics_handler.go header for the package
// file-split convention.
package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/store"
)

// EventsReader abstracts the underlying store.
type EventsReader interface {
	ListWorkerEvents(ctx context.Context, workerID, eventType, since string, limit int) ([]store.WorkerEventRow, error)
}

// EventsHandler holds the dependency on the events read store.
type EventsHandler struct {
	reader EventsReader
}

// NewEventsHandler wires the handler. Returns nil when reader is
// nil so the caller can skip route registration on a no-store
// configuration.
func NewEventsHandler(reader EventsReader) *EventsHandler {
	if reader == nil {
		return nil
	}
	return &EventsHandler{reader: reader}
}

// ListWorkerEvents returns GET /api/v1/workers/:worker_id/events
//
// Query parameters:
//
//	limit      — optional page size, 1..MaxListLimit (default
//	             DefaultListLimit).
//	event_type — optional exact match on event_type. Unknown types
//	             are still queryable (handler does not enforce a
//	             whitelist) so a future event-type addition does not
//	             require a handler change.
//	since      — optional RFC3339 lower bound on created_at.
//
// Response: 200 WorkerEventsListResponse, 400 on missing :worker_id,
// 503 on a nil reader.
//
// @Summary       List worker events (audit ledger)
// @Description   Per-worker audit ledger for dashboards / on-call tooling.
// @Description   SECURITY (canonical ownership §3): details_json is parsed
// @Description   into a map and every string value is routed through
// @Description   sanitiseHostname() so embedded IPs / paths / long-hex
// @Description   cannot leak. Parse failures fall back to the raw string
// @Description   (still sanitised) so the audit ledger is never silently
// @Description   dropped.
// @Tags          workers
// @Produce       json
// @Param         worker_id  path  string true  "Worker ID"
// @Param         limit      query int    false "Optional page size, 1..1000 (default 100)"
// @Param         event_type query string false "Optional exact match on event_type (no whitelist enforced)"
// @Param         since      query string false "Optional RFC3339 lower bound on created_at"
// @Success       200        {object} WorkerEventsListResponse   "Events payload"
// @Failure       400        {object} map[string]string         "worker_id is required"
// @Failure       500        {object} map[string]string         "list worker events error"
// @Failure       503        {object} map[string]string         "events reader not available"
// @Router        /api/v1/workers/{worker_id}/events [get]
func (h *EventsHandler) ListWorkerEvents() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h == nil || h.reader == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "events reader not available"})
			return
		}
		workerID := strings.TrimSpace(c.Param("worker_id"))
		if workerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id is required"})
			return
		}
		limit := clampLimit(c.Query("limit"))
		eventType := strings.TrimSpace(c.Query("event_type"))
		since := strings.TrimSpace(c.Query("since"))

		rows, err := h.reader.ListWorkerEvents(c.Request.Context(), workerID, eventType, since, limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "list worker events: " + err.Error()})
			return
		}
		resp := WorkerEventsListResponse{
			WorkerID: workerID,
			Count:    len(rows),
			Events:   make([]EventResponse, 0, len(rows)),
		}
		for _, r := range rows {
			resp.Events = append(resp.Events, sanitizeEvent(r))
		}
		c.JSON(http.StatusOK, resp)
	}
}

// sanitizeEvent converts a store.WorkerEventRow into the
// operator-facing EventResponse. details_json is parsed into a map
// and every string value is routed through sanitiseHostname() so
// embedded IPs / paths / long-hex cannot leak. A parse failure
// falls back to a single sanitised string under the Details key
// so the audit ledger is never silently dropped.
func sanitizeEvent(r store.WorkerEventRow) EventResponse {
	out := EventResponse{
		EventID:   r.EventID,
		EventType: r.EventType,
		Severity:  r.Severity,
		CreatedAt: r.CreatedAt,
	}
	if r.WorkerID.Valid {
		out.WorkerID = r.WorkerID.String
	}
	if r.SessionID.Valid {
		out.SessionID = r.SessionID.String
	}
	if r.JobID.Valid {
		out.JobID = r.JobID.String
	}
	if r.TaskID.Valid {
		out.TaskID = r.TaskID.String
	}
	if r.AttemptID.Valid {
		out.AttemptID = r.AttemptID.String
	}
	if r.ReasonCode.Valid {
		out.ReasonCode = r.ReasonCode.String
	}
	if r.DetailsJSON != "" {
		out.Details = parseAndSanitiseDetails(r.DetailsJSON)
	}
	return out
}

// parseAndSanitiseDetails parses a JSON details blob into a map and
// routes every string value through sanitiseHostname(). On parse
// failure the raw string is returned under a single "_raw" key so
// the audit row is never silently dropped, and the raw string itself
// is also run through the redaction surface so embedded IPs / paths
// / long hex cannot leak via the fallback path either.
//
// The result type is map[string]any (matching the EventResponse
// DTO field) even on the fallback path so JSON shape is stable
// across the success / failure branches.
func parseAndSanitiseDetails(raw string) map[string]any {
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return map[string]any{"_raw": sanitiseHostname(raw)}
	}
	for k, v := range m {
		if s, ok := v.(string); ok {
			m[k] = sanitiseHostname(s)
		}
	}
	return m
}
