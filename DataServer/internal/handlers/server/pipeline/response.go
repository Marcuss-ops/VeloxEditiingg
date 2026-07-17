// Package pipeline: HTTP handlers for the remote pipeline-run API.
//
// File: response.go
// -----------------------------------------------------------------------------
// PR-DI-pipeline — Step 7 of the pipeline.go split.
//
// What lives here
//   - buildCreateResponse            — 202 envelope for idempotent duplicates
//     (POST /api/v1/pipeline-runs).
//   - buildCreateResponseFromSyncForward
//     — 202 envelope when the remote engine
//     returned a complete result that was
//     synchronously forwarded to the Velox
//     worker queue (POST /api/v1/pipeline-runs).
//   - buildPipelineRunProjection    — receiver method on *Handlers that
//     builds the full status projection
//     from a pipeline_runs row, enriching
//     with worker/artifact/delivery state
//     when a velox_job_id is set
//     (GET /api/v1/pipeline-runs/:id).
//   - forwardingStatus              — derives a top-level pipeline-run
//     status string from the legacy
//     creator_forwardings row (used by
//     the same GET endpoint).
//
// All four are pure data-shapers: they take an existing row + a few flags
// and return a `gin.H` map for the JSON response. They do not mutate
// state, do not call the store, and do not issue side effects.
//
// They were previously scattered:
//   - buildCreateResponse + buildCreateResponseFromSyncForward → pipeline_create.go
//   - buildPipelineRunProjection + forwardingStatus → pipeline_run_status.go
//
// They are now consolidated here so that all response-shape logic lives
// in a single file, which makes it easy to keep the wire format
// consistent across the create / status / sync-forward / legacy
// endpoints.
//
// No signature changes, no body changes, no behaviour changes. All
// call sites are in the same `package pipeline`, so no callers need
// to change: same-package symbols resolve by name without import.
// -----------------------------------------------------------------------------
package pipeline

import (
	"context"

	"github.com/gin-gonic/gin"

	"velox-server/internal/pipelineruns"
	"velox-server/internal/store"
)

// buildCreateResponse builds the 202 response for an idempotent duplicate
// (a request whose idempotency_key already existed). The isDuplicate
// flag is included so the client can distinguish a fresh run from a
// replayed one.
func buildCreateResponse(pr *pipelineruns.PipelineRun, isDuplicate bool) gin.H {
	resp := gin.H{
		"ok":              true,
		"pipeline_run_id": pr.ID,
		"request_id":      pr.RequestID,
		"status":          string(pr.Status),
		"status_url":      "/api/v1/pipeline-runs/" + pr.ID,
		"is_duplicate":    isDuplicate,
	}
	if pr.RemoteJobID != "" {
		resp["remote_job_id"] = pr.RemoteJobID
	}
	if pr.ForwardingID != "" {
		resp["forwarding_id"] = pr.ForwardingID
	}
	return resp
}

// buildCreateResponseFromSyncForward builds the 202 response when the
// remote engine returned a complete result that was synchronously
// forwarded to the Velox worker queue. `forwarded` is the worker
// response map returned by forwardPipelineResultToWorker (which wraps
// Resolver.Resolve's ResolveOutput.Response).
func buildCreateResponseFromSyncForward(pr *pipelineruns.PipelineRun, forwarded map[string]interface{}) gin.H {
	resp := gin.H{
		"ok":              true,
		"pipeline_run_id": pr.ID,
		"request_id":      pr.RequestID,
		"status":          string(pr.Status),
		"status_url":      "/api/v1/pipeline-runs/" + pr.ID,
		"is_duplicate":    false,
	}
	if pr.RemoteJobID != "" {
		resp["remote_job_id"] = pr.RemoteJobID
	}
	if forwarded != nil {
		if wjID, ok := forwarded["job_id"].(string); ok && wjID != "" {
			resp["velox_job_id"] = wjID
		}
	}
	return resp
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
	case string(store.CFStatusCancelled):
		return "CANCELLED"
	default:
		return "REMOTE_QUEUED"
	}
}
