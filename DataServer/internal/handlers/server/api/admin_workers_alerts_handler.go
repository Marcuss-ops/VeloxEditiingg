package api

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/store"
)

// admin_workers_alerts_handler.go — Step 16/15 admin alerts
// handler. Three routes:
//   GET /api/v1/admin/workers/:worker_id/alerts
//     → most-recent N rows (ACTIVE+RESOLVED) for the worker,
//       ordered fired_at DESC. Limit defaults to 100, cap 1000.
//   GET /api/v1/admin/alerts/active
//     → ACTIVE rows across the fleet, grouped by severity
//       (CRITICAL first, WARNING second, INFO third), order
//       fired_at ASC. Limit defaults to 200, cap 1000.
//   GET /api/v1/admin/alerts/recent
//     → most-recent N rows (state-mixed) for "recent events"
//       drill-down. Limit defaults to 50, cap 500.
//
// All routes are adminAuth-gated (mounted under
// /api/v1/admin/workers/{id} by RegisterRoutes in app/workers.go).
// The handler is nil-tolerant — RegisterRoutes skips the routes
// when the handler wasn't injected via SetAlertsHandler.
//
// Distinct from the legacy /worker handlers: this surface
// serves the operator dashboard (worker.oncall), NOT the
// workersreg.Registry directly. The dashboard reads the
// persisted alert_events table (migration 107).

// AdminWorkersAlertsHandler serves the Step 16/15 alert REST
// surface. Backed by *store.SQLiteStore directly so a partial
// FleetController wire-up does not gate the read path.
type AdminWorkersAlertsHandler struct {
	store          AlertEventStore
	maxPerWorker   int
	maxFleetActive int
	maxRecent      int
	staleAfter     time.Duration
}

// AlertEventStore is the read surface the handler needs.
// Production passes *store.SQLiteStore; tests pass a stub.
//   - reqCtx is a type alias for context.Context so the interface
//     signatures match the SQLite store's methods directly
//     (structural typing); handlers call c.Request.Context()
//     without re-wrapping.
type AlertEventStore interface {
	ListAlertEventsForWorker(ctx reqCtx, workerID string, limit int) ([]store.AlertEvent, error)
	ListActiveAlertEvents(ctx reqCtx, limit int) ([]store.AlertEvent, error)
	ListRecentAlertEvents(ctx reqCtx, limit int) ([]store.AlertEvent, error)
}

// reqCtx is the canonical context.Context alias used across the
// alert-event handler/store interface boundary. Aliased rather
// than imported as a fresh type so *store.SQLiteStore's existing
// context.Context method signatures satisfy the interface via
// structural typing without an adapter shim.
type reqCtx = context.Context

// NewAdminWorkersAlertsHandler builds the handler with sane caps.
func NewAdminWorkersAlertsHandler(s AlertEventStore) *AdminWorkersAlertsHandler {
	if s == nil {
		return nil
	}
	return &AdminWorkersAlertsHandler{
		store:          s,
		maxPerWorker:   1000,
		maxFleetActive: 1000,
		maxRecent:      500,
		staleAfter:     30 * 24 * time.Hour,
	}
}

// ListWorkerAlerts serves GET
// /api/v1/admin/workers/:worker_id/alerts.
func (h *AdminWorkersAlertsHandler) ListWorkerAlerts() gin.HandlerFunc {
	return h.listWorkerAlertsHandler
}

func (h *AdminWorkersAlertsHandler) listWorkerAlertsHandler(c *gin.Context) {
	if h == nil || h.store == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "alerts store not wired"})
		return
	}
	workerID := c.Param("worker_id")
	if workerID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id missing"})
		return
	}
	limit := parseLimit(c.Query("limit"), 100)
	if limit > h.maxPerWorker {
		limit = h.maxPerWorker
	}
	rows, err := h.store.ListAlertEventsForWorker(c.Request.Context(), workerID, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"data":      rows,
		"count":     len(rows),
		"has_more":  len(rows) == limit,
		"worker_id": workerID,
	})
}

// ListFleetActiveAlerts serves GET /api/v1/admin/alerts/active.
func (h *AdminWorkersAlertsHandler) ListFleetActiveAlerts() gin.HandlerFunc {
	return h.listFleetActiveAlertsHandler
}

func (h *AdminWorkersAlertsHandler) listFleetActiveAlertsHandler(c *gin.Context) {
	if h == nil || h.store == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "alerts store not wired"})
		return
	}
	limit := parseLimit(c.Query("limit"), 200)
	if limit > h.maxFleetActive {
		limit = h.maxFleetActive
	}
	rows, err := h.store.ListActiveAlertEvents(c.Request.Context(), limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// Group by severity for the dashboard's at-a-glance counters.
	bySev := groupBySeverity(rows)
	c.JSON(http.StatusOK, gin.H{
		"data":        rows,
		"count":       len(rows),
		"has_more":    len(rows) == limit,
		"by_severity": bySev,
	})
}

// ListRecentAlerts serves GET /api/v1/admin/alerts/recent.
func (h *AdminWorkersAlertsHandler) ListRecentAlerts() gin.HandlerFunc {
	return h.listRecentAlertsHandler
}

func (h *AdminWorkersAlertsHandler) listRecentAlertsHandler(c *gin.Context) {
	if h == nil || h.store == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "alerts store not wired"})
		return
	}
	limit := parseLimit(c.Query("limit"), 50)
	if limit > h.maxRecent {
		limit = h.maxRecent
	}
	rows, err := h.store.ListRecentAlertEvents(c.Request.Context(), limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"data":     rows,
		"count":    len(rows),
		"has_more": len(rows) == limit,
	})
}

// parseLimit clamps the ?limit= query param. Defaults to def when
// missing or invalid; min 1, max 5000.
func parseLimit(raw string, def int) int {
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return def
	}
	if n > 5000 {
		n = 5000
	}
	return n
}

// groupBySeverity counts active alerts per severity bucket for
// the dashboard's at-a-glance counters. Returns
// {CRITICAL:N, WARNING:N, INFO:N} with absent keys omitted.
func groupBySeverity(rows []store.AlertEvent) map[string]int {
	out := make(map[string]int, 3)
	for _, r := range rows {
		out[r.Severity]++
	}
	return out
}
