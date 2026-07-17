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
// worker, artifact and delivery state. The request_id may be the forwarding
// ID or the remote job ID; this keeps the endpoint useful for requests issued
// before pipeline_runs was introduced while the projection is backfilled.
func (h *Handlers) PipelineRunStatus() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h.store == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": "pipeline status store not wired"})
			return
		}
		requestID := strings.TrimSpace(c.Param("request_id"))
		if requestID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "request_id is required"})
			return
		}

		ctx := c.Request.Context()
		forwarding, err := h.store.GetCreatorForwarding(ctx, requestID)
		if errors.Is(err, store.ErrCreatorForwardingNoRow) {
			forwarding, err = h.store.GetCreatorForwardingByRemoteJob(ctx, "remote_engine", requestID)
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
			"request_id": requestID,
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
