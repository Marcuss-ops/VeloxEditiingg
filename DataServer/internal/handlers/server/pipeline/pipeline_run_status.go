package pipeline

import (
	"database/sql"
	"errors"
	"net/http"

	"velox-server/internal/deliverystore"
	"velox-server/internal/repository"

	"github.com/gin-gonic/gin"
)

// PipelineRunStatus exposes one read-only projection of the durable handoff,
// worker, artifact and delivery state. Ownership is enforced by the shared
// lookup when an M2M client ID is present in the request context.
func (h *Handlers) PipelineRunStatus() gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx, idParam, clientID, pr, forwarding, ok := h.lookupPipelineRunRequest(c, "pipeline status store not wired")
		if !ok {
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
			var artifacts []repository.Artifact
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
					var deliveries []deliverystore.JobDelivery
					if clientID != "" {
						deliveries, _ = h.store.ListJobDeliveriesByJobForClient(ctx, forwarding.TargetJobID, clientID)
					} else {
						deliveries, _ = h.store.Delivery().ListJobDeliveriesByJob(forwarding.TargetJobID)
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
