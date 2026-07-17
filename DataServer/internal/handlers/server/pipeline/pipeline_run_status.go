package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/pipelineruns"
	"velox-server/internal/store"
)

// PipelineRunStatus exposes one read-only projection of the durable handoff,
// worker, artifact and delivery state.
//
// The :id param accepts, in priority order:
//  1. a pipeline_run id (run_...) — looked up in the pipeline_runs table;
//  2. a request_id (req_...) — looked up in pipeline_runs by request_id;
//  3. a forwarding_id — looked up in creator_forwardings (legacy);
//  4. a remote_job_id — looked up in creator_forwardings by source_job_id (legacy).
//
// The legacy fallbacks (3, 4) keep the endpoint useful for requests issued
// before pipeline_runs was introduced while the projection is backfilled.
func (h *Handlers) PipelineRunStatus() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h.store == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": "pipeline status store not wired"})
			return
		}
		idParam := strings.TrimSpace(c.Param("id"))
		if idParam == "" {
			// Backwards-compat: the old route used :request_id.
			idParam = strings.TrimSpace(c.Param("request_id"))
		}
		if idParam == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "id is required"})
			return
		}

		ctx := c.Request.Context()

		// ── 1. Try the pipeline_runs table by primary key ─────────────
		if pr, err := h.store.GetPipelineRun(ctx, idParam); err == nil && pr != nil {
			c.JSON(http.StatusOK, h.buildPipelineRunProjection(ctx, pr))
			return
		}

		// ── 2. Try by request_id ──────────────────────────────────────
		if pr, err := h.store.GetPipelineRunByRequestID(ctx, idParam); err == nil && pr != nil {
			c.JSON(http.StatusOK, h.buildPipelineRunProjection(ctx, pr))
			return
		}

		// ── 3-4. Legacy fallback: creator_forwardings ─────────────────
		forwarding, err := h.store.GetCreatorForwarding(ctx, idParam)
		if errors.Is(err, store.ErrCreatorForwardingNoRow) {
			forwarding, err = h.store.GetCreatorForwardingByRemoteJob(ctx, "remote_engine", idParam)
		}
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) || errors.Is(err, store.ErrCreatorForwardingNoRow) {
				c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "pipeline run not found"})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
			return
		}

		response := gin.H{
			"id":         idParam,
			"request_id": idParam,
			"status":     forwardingStatus(forwarding),
			"remote": gin.H{
				"provider": forwarding.SourceProvider,
				"job_id":   forwarding.SourceJobID,
				"status":   forwarding.SourceStatus,
			},
			"forwarding": forwarding,
		}
		if forwarding.TargetJobID == "" {
			c.JSON(http.StatusOK, response)
			return
		}

		job, jobErr := h.store.GetJob(ctx, forwarding.TargetJobID)
		if jobErr == nil {
			response["worker"] = gin.H{"job_id": forwarding.TargetJobID, "status": job["status"]}
			artifacts, _ := h.store.GetArtifactsByJob(forwarding.TargetJobID, 1)
			if len(artifacts) > 0 {
				a := artifacts[0]
				response["artifact"] = gin.H{"artifact_id": a.ID, "status": a.Status, "sha256": a.SHA256, "storage_url": a.StorageURL}
				deliveries, _ := h.store.ListJobDeliveriesByJob(forwarding.TargetJobID)
				if len(deliveries) > 0 {
					d := deliveries[0]
					response["delivery"] = gin.H{"delivery_id": d.DeliveryID, "provider": d.DestinationID, "status": d.Status, "remote_id": d.RemoteID, "remote_url": d.RemoteURL}
				}
			}
		} else if !errors.Is(jobErr, sql.ErrNoRows) {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": jobErr.Error()})
			return
		}
		c.JSON(http.StatusOK, response)
	}
}

// buildPipelineRunProjection builds the full status projection from a
// pipeline_runs row. When the row has a velox_job_id, it enriches the
// response with worker/artifact/delivery state from the job layer.
func (h *Handlers) buildPipelineRunProjection(ctx context.Context, pr *pipelineruns.PipelineRun) gin.H {
	response := gin.H{
		"id":              pr.ID,
		"request_id":      pr.RequestID,
		"idempotency_key": pr.IdempotencyKey,
		"status":          string(pr.Status),
		"current_stage":   pr.CurrentStage,
		"remote_provider": pr.RemoteProvider,
		"remote_job_id":   pr.RemoteJobID,
		"forwarding_id":   pr.ForwardingID,
		"velox_job_id":    pr.VeloxJobID,
		"artifact_id":     pr.ArtifactID,
		"delivery_id":     pr.DeliveryID,
		"created_at":      pr.CreatedAt,
		"updated_at":      pr.UpdatedAt,
	}
	if !pr.CompletedAt.IsZero() {
		response["completed_at"] = pr.CompletedAt
	}
	if pr.ErrorCode != "" {
		response["error_code"] = pr.ErrorCode
		response["error_message"] = pr.ErrorMessage
		response["failed_stage"] = pr.FailedStage
	}

	// Enrich with worker/artifact/delivery when a velox_job_id is set.
	if pr.VeloxJobID == "" {
		return response
	}
	job, jobErr := h.store.GetJob(ctx, pr.VeloxJobID)
	if jobErr == nil {
		response["worker"] = gin.H{"job_id": pr.VeloxJobID, "status": job["status"]}
		artifacts, _ := h.store.GetArtifactsByJob(pr.VeloxJobID, 1)
		if len(artifacts) > 0 {
			a := artifacts[0]
			response["artifact"] = gin.H{"artifact_id": a.ID, "status": a.Status, "sha256": a.SHA256, "storage_url": a.StorageURL}
			deliveries, _ := h.store.ListJobDeliveriesByJob(pr.VeloxJobID)
			if len(deliveries) > 0 {
				d := deliveries[0]
				response["delivery"] = gin.H{"delivery_id": d.DeliveryID, "provider": d.DestinationID, "status": d.Status, "remote_id": d.RemoteID, "remote_url": d.RemoteURL}
			}
		}
	}
	return response
}

func forwardingStatus(f *store.CreatorForwarding) string {
	switch f.Status {
	case string(store.CFStatusForwarded):
		return "WORKER_QUEUED"
	case string(store.CFStatusFailed), string(store.CFStatusBlocked):
		return "FAILED"
	default:
		return "REMOTE_QUEUED"
	}
}
