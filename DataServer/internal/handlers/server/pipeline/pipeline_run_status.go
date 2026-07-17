package pipeline

import (
	"database/sql"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

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


