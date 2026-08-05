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
// worker, artifact and delivery state. Ownership is enforced by the shared
// lookup when an M2M client ID is present in the request context.
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
		clientID := strings.TrimSpace(ClientIDFromContext(c))
		pr, forwarding, err := h.lookupPipelineRun(ctx, idParam, clientID)
		if err != nil {
			if errors.Is(err, errPipelineRunNotFound) {
				if clientID != "" {
					writeM2MJobNotFound(c)
				} else {
					c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "pipeline run not found"})
				}
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
			return
		}

		if forwarding != nil {
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
			var job map[string]any
			var jobErr error
			var artifacts []store.Artifact
			if clientID != "" {
				job, jobErr = h.store.GetJobForClient(ctx, forwarding.TargetJobID, clientID)
				artifacts, _ = h.store.GetArtifactsByJobForClient(ctx, forwarding.TargetJobID, clientID, 1)
			} else {
				job, jobErr = h.store.GetJob(ctx, forwarding.TargetJobID)
				artifacts, _ = h.store.GetArtifactsByJob(forwarding.TargetJobID, 1)
			}
			if jobErr == nil {
				response["worker"] = gin.H{"job_id": forwarding.TargetJobID, "status": job["status"]}
				if len(artifacts) > 0 {
					a := artifacts[0]
					response["artifact"] = gin.H{"artifact_id": a.ID, "status": a.Status, "sha256": a.SHA256, "storage_url": a.StorageURL}
					var deliveries []store.JobDelivery
					if clientID != "" {
						deliveries, _ = h.store.ListJobDeliveriesByJobForClient(ctx, forwarding.TargetJobID, clientID)
					} else {
						deliveries, _ = h.store.ListJobDeliveriesByJob(forwarding.TargetJobID)
					}
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
			return
		}

		c.JSON(http.StatusOK, h.buildPipelineRunProjection(ctx, pr, clientID))
	}
}
