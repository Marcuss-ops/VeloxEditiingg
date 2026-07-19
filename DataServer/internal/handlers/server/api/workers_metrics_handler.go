// Package api — RW-PROD-005 metrics endpoint.
//
// workers_metrics_handler.go owns the GET /api/v1/workers/:worker_id/metrics
// handler. It exposes a read-only time-series projection over the
// worker_metric_samples table for dashboards / on-call tooling.
//
// File layout in this package (handlers split by concern, DTOs + mappers
// shared):
//
//	workers_metrics_handler.go  — this file: HTTP entry point + Reader interface.
//	workers_sessions_handler.go — sessions HTTP entry point.
//	workers_events_handler.go   — events HTTP entry point.
//	workers_read_adapters.go    — production *sql.DB-backed adapters.
//	workers_dto.go              — shared DTOs (MetricSampleResponse + list wrapper).
//	workers_mapper.go           — sanitisation / numeric-type tolerant parsers.
//
// SECURITY posture (canonical, see OWNERSHIP.md §3): no token hashes,
// no IPs, no creds on this surface — the metric table carries only
// numeric counters + connection status + sampled_at. The only
// sanitisation needed is the NULL handling for the optional columns
// (load_average / process_rss_bytes / network_*_bytes) which the
// pointer-typed DTO fields express naturally (nil = omitted from
// the JSON response).
package api

import (
	"context"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/store"
)

// MetricsReader abstracts the underlying store so tests can pass a
// fake and production wires a *sql.DB-backed adapter. The interface
// mirrors store.ListWorkerMetrics one-to-one.
type MetricsReader interface {
	ListWorkerMetrics(ctx context.Context, workerID, since string, limit int) ([]store.WorkerMetricSampleRow, error)
}

// MetricsHandler holds the dependency on the metrics read store.
// It mirrors the shape of WorkersHandler (single dependency + thin
// handler surface).
type MetricsHandler struct {
	reader MetricsReader
}

// NewMetricsHandler wires the handler. Returns nil when reader is
// nil so the caller (WorkersModule.RegisterRoutes) can safely
// skip route registration on a no-store configuration (matches
// the nil-registry short-circuit pattern in WorkersHandler).
func NewMetricsHandler(reader MetricsReader) *MetricsHandler {
	if reader == nil {
		return nil
	}
	return &MetricsHandler{reader: reader}
}

// ListWorkerMetrics returns GET /api/v1/workers/:worker_id/metrics
//
// Query parameters:
//
//	limit  — optional page size, 1..MaxListLimit (default
//	          DefaultListLimit). Non-numeric or non-positive values
//	          fall back to the default.
//	since  — optional RFC3339 lower bound on sampled_at. When
//	          omitted, the most recent `limit` rows are returned
//	          regardless of age. Invalid timestamps are treated as
//	          "no filter" so a malformed client query never returns
//	          an error 400 (operators tolerate benign client drift).
//
// Response: 200 WorkerMetricsListResponse, 400 on missing
// :worker_id path param, 503 on a nil reader.
//
// @Summary       List worker metrics samples
// @Description   Read-only time-series projection over `worker_metric_samples`
// @Description   for a single worker. NULL optional columns (load_average,
// @Description   process_rss_bytes, network_rx_bytes, network_tx_bytes) are
// @Description   pinned to nil pointers so the JSON render surface omits them
// @Description   cleanly. No IPs / creds / TLS paths ever leave the store on
// @Description   this surface (RW-PROD-005 §2).
// @Tags          workers
// @Produce       json
// @Param         worker_id path     string true  "Worker ID"
// @Param         limit     query    int    false "Optional page size, 1..1000 (default 100)"
// @Param         since     query    string false "Optional RFC3339 lower bound on sampled_at"
// @Success       200       {object} WorkerMetricsListResponse "Metrics payload"
// @Failure       400       {object} map[string]string         "worker_id is required"
// @Failure       500       {object} map[string]string         "list worker metrics error"
// @Failure       503       {object} map[string]string         "metrics reader not available"
// @Router        /api/v1/workers/{worker_id}/metrics [get]
func (h *MetricsHandler) ListWorkerMetrics() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h == nil || h.reader == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "metrics reader not available"})
			return
		}
		workerID := strings.TrimSpace(c.Param("worker_id"))
		if workerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id is required"})
			return
		}
		limit := clampLimit(c.Query("limit"))
		since := strings.TrimSpace(c.Query("since"))

		rows, err := h.reader.ListWorkerMetrics(c.Request.Context(), workerID, since, limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "list worker metrics: " + err.Error()})
			return
		}
		resp := WorkerMetricsListResponse{
			WorkerID: workerID,
			Count:    len(rows),
			Metrics:  make([]MetricSampleResponse, 0, len(rows)),
		}
		for _, r := range rows {
			resp.Metrics = append(resp.Metrics, sanitizeMetricSample(r))
		}
		c.JSON(http.StatusOK, resp)
	}
}

// sanitizeMetricSample converts a store.WorkerMetricSampleRow into
// the operator-facing MetricSampleResponse. NULL optional fields are
// surfaced via nil pointers so the JSON render path can omit them
// cleanly (load_average, process_rss_bytes, network_rx_bytes,
// network_tx_bytes).
func sanitizeMetricSample(r store.WorkerMetricSampleRow) MetricSampleResponse {
	out := MetricSampleResponse{
		SampledAt:           r.SampledAt,
		ConnectionStatus:    r.ConnectionStatus,
		ActiveTasks:         r.ActiveTasks,
		TaskSlots:           r.TaskSlots,
		CPUUtilizationRatio: r.CPUUtilizationRatio,
		MemoryUsedBytes:     r.MemoryUsedBytes,
		DiskFreeBytes:       r.DiskFreeBytes,
	}
	if r.SessionID.Valid {
		out.SessionID = r.SessionID.String
	}
	if r.LoadAverage.Valid {
		v := r.LoadAverage.Float64
		out.LoadAverage = &v
	}
	if r.ProcessRSSBytes.Valid {
		v := r.ProcessRSSBytes.Int64
		out.ProcessRSSBytes = &v
	}
	if r.NetworkRxBytes.Valid {
		v := r.NetworkRxBytes.Int64
		out.NetworkRxBytes = &v
	}
	if r.NetworkTxBytes.Valid {
		v := r.NetworkTxBytes.Int64
		out.NetworkTxBytes = &v
	}
	return out
}
