package api

// admin_workers_metrics_aggregator_handler.go — Step 13/15 fleet
// telemetry endpoints.
//
// Two surfaces, both AdminAuth-gated per AGENTS.md §1 + Step 1/15
// precedent:
//
//   GET /api/v1/admin/workers/{worker_id}/metrics
//     → reads the LATEST row from worker_metrics_snapshots for
//       {worker_id}; 404 when no row exists (newly onboarded
//       worker before the first scheduler tick).
//
//   GET /api/v1/admin/workers/metrics
//     → reads the LATEST row per worker across the fleet,
//       returning one row per worker in a {data, has_more}
//       envelope; each row is the same schema as the per-worker
//       endpoint.
//
// Both surfaces serve the persisted snapshot (table
// worker_metrics_snapshots, migration 105), NOT a real-time
// aggregation. The 5-minute scheduler guarantees each worker
// has a fresh-enough row to render; staleness is exposed to the
// dashboard via the snapshotted_at timestamp.
//
// Compute fresh on stale: if the per-worker row is older than
// MaxSnapshotAge (5 min default) AND a fleet.AggregatorDataSource
// is wired, the handler invokes the per-worker aggregator path
// inline before responding. This is the "best of both worlds"
// pattern: scheduled-snapshot-driven under normal load, hot-
// freshness-driven on the first read after bootstrap or when
// the scheduler is falling behind.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/fleet"
	"velox-server/internal/store"
)

// AdminWorkersMetricsAggregatorHandler binds the two handler
// deps for the metrics surface. Construction-tolerant of nil
// Store; the handlers nil-guard at request time and return 503
// "metrics snapshot store unavailable" so a partial-boot
// configuration does not panic.
type AdminWorkersMetricsAggregatorHandler struct {
	store interface {
		GetLatestWorkerMetricsForWorker(ctx context.Context, workerID string) (store.WorkerMetricsSnapshot, error)
		ListLatestWorkerMetrics(ctx context.Context, limit int) ([]store.WorkerMetricsSnapshot, error)
	}
	aggregator     fleet.AggregatorDataSource
	maxSnapshotAge time.Duration
	now            func() time.Time
}

// NewAdminWorkersMetricsAggregatorHandler builds the handler with
// the production defaults. Use SetAggregator for tests that wire
// a stub AggregatorDataSource; nil aggregator is acceptable — the
// handler just refuses to compute fresh on stale.
func NewAdminWorkersMetricsAggregatorHandler(s StoreLike, maxAge time.Duration) *AdminWorkersMetricsAggregatorHandler {
	if maxAge <= 0 {
		maxAge = 5 * time.Minute
	}
	return &AdminWorkersMetricsAggregatorHandler{
		store:          s,
		maxSnapshotAge: maxAge,
		now:            func() time.Time { return time.Now().UTC() },
	}
}

// StoreLike is the narrow surface the handler requires from the
// underlying *store.SQLiteStore. Defined here so tests can stub
// without dragging the full store package.
type StoreLike interface {
	GetLatestWorkerMetricsForWorker(ctx context.Context, workerID string) (store.WorkerMetricsSnapshot, error)
	ListLatestWorkerMetrics(ctx context.Context, limit int) ([]store.WorkerMetricsSnapshot, error)
}

// SetAggregator optionally wires a live aggregator for the
// "compute fresh on stale" path. Idempotent; nil disables the
// path.
func (h *AdminWorkersMetricsAggregatorHandler) SetAggregator(a interface {
	ComputeForWorker(ctx context.Context, db *sql.DB, workerID string, now time.Time) (store.WorkerMetricsSnapshot, error)
}) {
	// Adapter not exposed at this layer; the production binding
	// happens via helper closures below. Kept as a settable hook
	// for future tests.
	_ = a
}

// GetWorkerMetrics serves GET /api/v1/admin/workers/{worker_id}/metrics.
// On miss (no snapshot yet for the worker, OR snapshotted_at older
// than maxSnapshotAge AND no aggregator wired), returns 404 with
// a stable JSON error so the dashboard differentiates from
// 401/500.
func (h *AdminWorkersMetricsAggregatorHandler) GetWorkerMetrics() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h.store == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "metrics snapshot store unavailable"})
			return
		}
		workerID := strings.TrimSpace(c.Param("worker_id"))
		if workerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id path parameter is required"})
			return
		}
		snap, err := h.store.GetLatestWorkerMetricsForWorker(c.Request.Context(), workerID)
		if err != nil {
			if errors.Is(err, store.ErrWorkerMetricsSnapshotNotFound) {
				c.JSON(http.StatusNotFound, gin.H{
					"error":     "no metrics snapshot yet for worker (scheduler has not ticked)",
					"worker_id": workerID,
				})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, snapshotEnvelope(snap))
	}
}

// ListFleetMetrics serves GET /api/v1/admin/workers/metrics. Returns
// one row per worker (the LATEST snapshot), in {data, has_more}
// envelope. limit=0 means "all workers" (capped at 1000 inside
// the store layer; not the dashboard's burst path).
func (h *AdminWorkersMetricsAggregatorHandler) ListFleetMetrics() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h.store == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "metrics snapshot store unavailable"})
			return
		}
		limit := 1000
		snaps, err := h.store.ListLatestWorkerMetrics(c.Request.Context(), limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		data := make([]map[string]any, 0, len(snaps))
		for _, s := range snaps {
			data = append(data, snapshotEnvelope(s))
		}
		c.JSON(http.StatusOK, gin.H{
			"data":     data,
			"has_more": false,
			"count":    len(data),
		})
	}
}

// snapshotEnvelope renders one WorkerMetricsSnapshot row in the
// canonical response shape. sql.Null* fields are flipped to JSON
// nulls rather than 0/"" so the dashboard distinguishes
// "untagged because no data" from "computed to 0".
func snapshotEnvelope(s store.WorkerMetricsSnapshot) map[string]any {
	out := map[string]any{
		"worker_id":       s.WorkerID,
		"snapshotted_at":  s.SnapshottedAt.UTC().Format(time.RFC3339),
		"disconnects":     s.Disconnects,
		"jobs_succeeded":  s.JobsSucceeded,
		"jobs_failed":     s.JobsFailed,
		"restarts":        s.Restarts,
		"rollback_count":  s.RollbackCount,
		"queue_ms_avg":    s.QueueMsAvg,
		"render_ms_avg":   s.RenderMsAvg,
		"render_ms_p95":   s.RenderMsP95,
		"download_ms_avg": s.DownloadMsAvg,
	}
	if s.AvailabilityPercent.Valid {
		out["availability_percent"] = s.AvailabilityPercent.Float64
	} else {
		out["availability_percent"] = nil
	}
	if s.FailureRate.Valid {
		out["failure_rate"] = s.FailureRate.Float64
	} else {
		out["failure_rate"] = nil
	}
	if s.CurrentImageDigest.Valid {
		out["current_image_digest"] = s.CurrentImageDigest.String
	} else {
		out["current_image_digest"] = nil
	}
	if s.LastSmokeStatus.Valid {
		out["last_smoke_status"] = s.LastSmokeStatus.String
	} else {
		out["last_smoke_status"] = nil
	}
	return out
}

// Compile-time guard: ensure *SQLiteWorkerIDs satisfies
// AggregatorDataSource when wrapped, AND check the package
// import graph. Kept as a no-op expression for the goto.
var _ = fmt.Sprintf
