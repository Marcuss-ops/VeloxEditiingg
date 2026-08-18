// Package pipeline / pipeline_run_completion.go — pipeline run completion
// phase. Extracted from pipeline_run_actions.go: artifacts and deliveries
// produced by the Velox job.
package pipeline

import (
	"errors"
	"net/http"
	"strings"

	"velox-server/internal/store"

	"github.com/gin-gonic/gin"
)

// PipelineRunArtifacts handles GET /api/v1/pipeline-runs/:id/artifacts.
//
// Returns the artifacts produced by the Velox job associated with the
// pipeline run. When no velox_job_id is set, returns an empty list.
func (h *Handlers) PipelineRunArtifacts() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h.store == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": "pipeline store not wired"})
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

		veloxJobID := pr.VeloxJobID
		if veloxJobID == "" && forwarding != nil {
			veloxJobID = forwarding.TargetJobID
		}
		if veloxJobID == "" {
			c.JSON(http.StatusOK, gin.H{
				"ok":              true,
				"pipeline_run_id": pr.ID,
				"artifacts":       []interface{}{},
				"count":           0,
			})
			return
		}

		var artifacts []store.Artifact
		if clientID != "" {
			artifacts, _ = h.store.GetArtifactsByJobForClient(ctx, veloxJobID, clientID, 50)
		} else {
			artifacts, _ = h.store.GetArtifactsByJob(veloxJobID, 50)
		}
		result := make([]gin.H, 0, len(artifacts))
		for _, a := range artifacts {
			result = append(result, gin.H{
				"artifact_id": a.ID,
				"job_id":      a.JobID,
				"type":        a.Type,
				"status":      a.Status,
				"sha256":      a.SHA256,
				"size_bytes":  a.SizeBytes,
				"storage_url": a.StorageURL,
				"mime_type":   a.MimeType,
				"verified_at": a.VerifiedAt,
				"created_at":  a.CreatedAt,
			})
		}

		c.JSON(http.StatusOK, gin.H{
			"ok":              true,
			"pipeline_run_id": pr.ID,
			"artifacts":       result,
			"count":           len(result),
		})
	}
}

// PipelineRunDeliveries handles GET /api/v1/pipeline-runs/:id/deliveries.
//
// Returns the delivery rows associated with the Velox job's artifacts.
// When no velox_job_id is set, returns an empty list.
func (h *Handlers) PipelineRunDeliveries() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h.store == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": "pipeline store not wired"})
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

		veloxJobID := pr.VeloxJobID
		if veloxJobID == "" && forwarding != nil {
			veloxJobID = forwarding.TargetJobID
		}
		if veloxJobID == "" {
			c.JSON(http.StatusOK, gin.H{
				"ok":              true,
				"pipeline_run_id": pr.ID,
				"deliveries":      []interface{}{},
				"count":           0,
			})
			return
		}

		var deliveries []store.JobDelivery
		if clientID != "" {
			deliveries, _ = h.store.ListJobDeliveriesByJobForClient(ctx, veloxJobID, clientID)
		} else {
			deliveries, _ = h.store.Delivery().ListJobDeliveriesByJob(veloxJobID)
		}
		result := make([]gin.H, 0, len(deliveries))
		for _, d := range deliveries {
			item := gin.H{
				"delivery_id":    d.DeliveryID,
				"artifact_id":    d.ArtifactID,
				"destination_id": d.DestinationID,
				"status":         d.Status,
				"remote_id":      d.RemoteID,
				"remote_url":     d.RemoteURL,
				"created_at":     d.CreatedAt,
				"updated_at":     d.UpdatedAt,
			}
			if d.IdempotencyKey != "" {
				item["idempotency_key"] = d.IdempotencyKey
			}
			result = append(result, item)
		}

		c.JSON(http.StatusOK, gin.H{
			"ok":              true,
			"pipeline_run_id": pr.ID,
			"deliveries":      result,
			"count":           len(result),
		})
	}
}
