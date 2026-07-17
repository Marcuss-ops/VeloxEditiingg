package pipeline

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/pipelineruns"
)

// CreatePipelineRunRequest is the typed, versioned API contract for
// POST /api/v1/pipeline-runs. It is the canonical entry point for a
// client-initiated generation pipeline.
//
// The `generation` block drives the remote script-generation service;
// `output` describes the desired video format; `video_metadata` carries
// the YouTube-facing metadata; `delivery_plan` is the list of
// destinations the finished artifact should be delivered to.
type CreatePipelineRunRequest struct {
	IdempotencyKey string             `json:"idempotency_key" binding:"required"`
	UserID         string             `json:"user_id"`
	CampaignID     string             `json:"campaign_id"`
	CampaignItemID string             `json:"campaign_item_id"`
	Generation     *GenerationSpec    `json:"generation"`
	Output         *OutputSpec        `json:"output"`
	VideoMetadata  *VideoMetadataSpec `json:"video_metadata"`
	DeliveryPlan   []DeliveryPlanItem `json:"delivery_plan"`
}

// GenerationSpec describes the script-generation parameters sent to the
// remote engine.
type GenerationSpec struct {
	Topic      string `json:"topic"`
	Language   string `json:"language"`
	Style      string `json:"style"`
	SceneCount int    `json:"scene_count"`
	SourceText string `json:"source_text"`
}

// OutputSpec describes the desired video output format.
type OutputSpec struct {
	Format string `json:"format"`
	Width  int    `json:"width"`
	Height int    `json:"height"`
	FPS    int    `json:"fps"`
}

// VideoMetadataSpec carries the YouTube-facing metadata for the
// finished video.
type VideoMetadataSpec struct {
	Title         string   `json:"title"`
	Description   string   `json:"description"`
	Tags          []string `json:"tags"`
	PrivacyStatus string   `json:"privacy_status"`
}

// DeliveryPlanItem is a single destination in the delivery plan.
type DeliveryPlanItem struct {
	Provider    string `json:"provider"`
	ChannelID   string `json:"channel_id"`
	PublishAt   string `json:"publish_at"`
	Destination string `json:"destination"`
}

// CreatePipelineRun handles POST /api/v1/pipeline-runs.
//
// Contract:
//
//  1. idempotency_key is required. Two requests with the same key MUST
//     return the same pipeline_run_id without starting a second remote
//     generation.
//  2. A pipeline_run row is created BEFORE the remote call is made, so
//     the resource exists durably even if the remote engine is slow or
//     the connection drops.
//  3. On success the handler returns HTTP 202 Accepted with a
//     status_url the client can poll.
//
// The handler delegates the remote-engine call to the existing
// h.client.StartPipeline path and the durable forwarding to
// h.resolver.PersistPendingRemoteForwarding, converging on the same
// creator_forwardings row the CreatorForwardingRunner picks up.
func (h *Handlers) CreatePipelineRun() gin.HandlerFunc {
	return func(c *gin.Context) {
		// ── Dependency guard ───────────────────────────────────────────
		if h.store == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"ok":    false,
				"error": "pipeline store not wired",
			})
			return
		}

		// ── Parse + validate request ──────────────────────────────────
		var req CreatePipelineRunRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"ok":    false,
				"error": "invalid JSON: " + err.Error(),
			})
			return
		}

		req.IdempotencyKey = strings.TrimSpace(req.IdempotencyKey)
		if req.IdempotencyKey == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"ok":    false,
				"error": "idempotency_key is required",
				"code":  "REQUIRED",
				"field": "idempotency_key",
			})
			return
		}

		// ── Validate request BEFORE creating the pipeline_run ─────────
		// All business-rule checks run here so a rejected request never
		// creates a pipeline_run row. The validator returns a structured
		// *ValidationError with field/code/message for a 400 response.
		if valErr := ValidateCreateRequest(c.Request.Context(), h.store, &req, DefaultValidationConfig()); valErr != nil {
			pipelineLog("CREATE: validation FAILED idem=%s field=%s code=%s: %s",
				req.IdempotencyKey, valErr.Field, valErr.Code, valErr.Message)
			c.JSON(http.StatusBadRequest, gin.H{
				"ok":     false,
				"error":  valErr.Message,
				"code":   valErr.Code,
				"field":  valErr.Field,
			})
			return
		}

		// ── Serialize the requested payload for durable storage ───────
		requestedJSON, _ := json.Marshal(req)
		if requestedJSON == nil {
			requestedJSON = []byte("{}")
		}

		// ── Create pipeline_run BEFORE the remote call ────────────────
		// A fresh UUID-shaped id. The idempotency_key UNIQUE index is
		// the authoritative dedup; INSERT OR IGNORE + lookup ensures
		// concurrent or retried requests converge on the same row.
		runID := "run_" + uuid.NewString()
		requestID := "req_" + uuid.NewString()
		now := time.Now().UTC()

		insertResult, err := h.store.InsertPipelineRun(c.Request.Context(), &pipelineruns.PipelineRun{
			ID:                   runID,
			RequestID:            requestID,
			IdempotencyKey:       req.IdempotencyKey,
			UserID:               req.UserID,
			CampaignID:           req.CampaignID,
			CampaignItemID:       req.CampaignItemID,
			Status:               pipelineruns.StatusAccepted,
			RequestedPayloadJSON: string(requestedJSON),
			CreatedAt:            now,
			UpdatedAt:            now,
		})
		if err != nil {
			pipelineLog("CREATE: failed to insert pipeline_run idem=%s: %v", req.IdempotencyKey, err)
			c.JSON(http.StatusInternalServerError, gin.H{
				"ok":    false,
				"error": "failed to create pipeline run",
			})
			return
		}

		// ── Idempotent duplicate: return the existing row ─────────────
		// The same idempotency_key MUST return the same pipeline_run_id
		// without starting a second remote generation.
		if !insertResult.Created {
			existing := insertResult.Run
			pipelineLog("CREATE: idempotent duplicate idem=%s → run=%s (no new remote call)",
				req.IdempotencyKey, existing.ID)
			c.JSON(http.StatusAccepted, buildCreateResponse(existing, true))
			return
		}

		pr := insertResult.Run
		pipelineLog("CREATE: created pipeline_run id=%s idem=%s", pr.ID, req.IdempotencyKey)

		// ── Remote engine guard ───────────────────────────────────────
		if h.client == nil || !h.client.IsConfigured() {
			// Mark the run as FAILED — the remote engine is required.
			if err := h.store.UpdatePipelineRunError(c.Request.Context(), pr.ID,
				"REMOTE_UNCONFIGURED", "remote engine not configured", "REMOTE_SUBMITTING"); err != nil {
				pipelineLog("CREATE: failed to mark REMOTE_UNCONFIGURED run=%s: %v", pr.ID, err)
			}
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"ok":              false,
				"pipeline_run_id": pr.ID,
				"request_id":      pr.RequestID,
				"status":          string(pipelineruns.StatusFailed),
				"error":           "remote engine not configured",
				"hint":            "set VELOX_REMOTE_ENGINE_URL",
				"status_url":      "/api/v1/pipeline-runs/" + pr.ID,
			})
			return
		}

		// ── Transition to REMOTE_SUBMITTING ───────────────────────────
		if err := h.store.UpdatePipelineRunStatus(c.Request.Context(), pr.ID,
			pipelineruns.StatusRemoteSubmitting, "submitting to remote engine"); err != nil {
			pipelineLog("CREATE: failed to transition to REMOTE_SUBMITTING run=%s: %v", pr.ID, err)
		}

		// ── Build the remote payload from the typed request ───────────
		remotePayload := buildRemotePayload(&req)

		// ── Call the remote engine ────────────────────────────────────
		result, err := h.client.StartPipeline(c.Request.Context(), remotePayload, pr.ID)
		if err != nil {
			pipelineLog("CREATE: remote call FAILED run=%s: %v", pr.ID, err)
			if markErr := h.store.UpdatePipelineRunError(c.Request.Context(), pr.ID,
				"REMOTE_CALL_FAILED", err.Error(), "REMOTE_SUBMITTING"); markErr != nil {
				pipelineLog("CREATE: failed to mark REMOTE_CALL_FAILED run=%s: %v", pr.ID, markErr)
			}
			c.JSON(http.StatusBadGateway, gin.H{
				"ok":              false,
				"pipeline_run_id": pr.ID,
				"request_id":      pr.RequestID,
				"status":          string(pipelineruns.StatusFailed),
				"error":           err.Error(),
				"status_url":      "/api/v1/pipeline-runs/" + pr.ID,
			})
			return
		}

		// ── Extract remote job id ─────────────────────────────────────
		jobID := firstStringResolver(result, "job_id", "trace_id", "id")
		status := firstStringResolver(result, "status")
		if jobID != "" {
			pipelineLog("CREATE: remote response run=%s job_id=%s status=%s",
				pr.ID, jobID, status)
		}

		// ── Stamp remote_job_id on the pipeline_run ───────────────────
		if jobID != "" {
			pr.RemoteJobID = jobID
			pr.RemoteProvider = "remote_engine"
			if err := h.store.UpdatePipelineRunRemoteJob(c.Request.Context(), pr.ID,
				"remote_engine", jobID); err != nil {
				pipelineLog("CREATE: failed to stamp remote_job_id run=%s: %v", pr.ID, err)
			}
		}

		// ── Sync forward if the result is already complete ────────────
		if enqueue.ShouldForwardPipelineResult(result) {
			pipelineLog("CREATE: result complete — forwarding to Velox workers (sync) run=%s", pr.ID)
			forwarded, forwardErr := h.forwardPipelineResultToWorker(c.Request.Context(), result)
			if forwardErr != nil {
				pipelineLog("CREATE: sync forward FAILED run=%s: %v", pr.ID, forwardErr)
				// Non-fatal: the result is complete but forwarding failed.
				// The reconciler (Area 3) will pick it up. We still
				// return 202 so the client can poll.
				if err := h.store.UpdatePipelineRunStatus(c.Request.Context(), pr.ID,
					pipelineruns.StatusForwarding, "sync forward failed"); err != nil {
					pipelineLog("CREATE: failed to mark FORWARDING after sync forward failure run=%s: %v", pr.ID, err)
				}
			} else if forwarded != nil {
				workerJobID, _ := forwarded["job_id"].(string)
				pipelineLog("CREATE: sync forward SUCCESS run=%s worker_job=%s", pr.ID, workerJobID)
				if workerJobID != "" {
					// Stamp velox_job_id + advance to WORKER_QUEUED.
					// forwarding_id is left empty in the sync path —
					// Resolver.Resolve creates the forwarding row
					// internally but does not surface it back here.
					// The reconciler (Area 3) will backfill it.
					pr.VeloxJobID = workerJobID
					if err := h.store.UpdatePipelineRunVeloxJob(c.Request.Context(), pr.ID,
						workerJobID, pipelineruns.StatusWorkerQueued); err != nil {
						pipelineLog("CREATE: failed to stamp velox_job_id run=%s: %v", pr.ID, err)
					}
				}
			}
			// Update the run with the result JSON for audit.
			if resultJSON, mErr := json.Marshal(result); mErr == nil {
				if err := h.store.UpdatePipelineRunResult(c.Request.Context(), pr.ID, string(resultJSON)); err != nil {
					pipelineLog("CREATE: failed to stamp result_json run=%s: %v", pr.ID, err)
				}
			}
			c.JSON(http.StatusAccepted, buildCreateResponseFromSyncForward(pr, forwarded))
			return
		}

		// ── Async path: persist a PENDING forwarding row ──────────────
		if jobID != "" {
			if h.resolver == nil || !h.resolver.HasDBAccess() {
				pipelineLog("CREATE: durable resolver unavailable run=%s job=%s", pr.ID, jobID)
				if markErr := h.store.UpdatePipelineRunError(c.Request.Context(), pr.ID,
					"RESOLVER_UNAVAILABLE", "durable forwarding is not configured", "FORWARDING"); markErr != nil {
					pipelineLog("CREATE: failed to mark RESOLVER_UNAVAILABLE run=%s: %v", pr.ID, markErr)
				}
				c.JSON(http.StatusServiceUnavailable, gin.H{
					"ok":              false,
					"pipeline_run_id": pr.ID,
					"request_id":      pr.RequestID,
					"status":          string(pipelineruns.StatusFailed),
					"error":           "durable forwarding is not configured",
					"status_url":      "/api/v1/pipeline-runs/" + pr.ID,
				})
				return
			}

			targetExecutor := firstStringResolver(result, "executor_id", "pipeline_id")
			forwarding, persistErr := h.resolver.PersistPendingRemoteForwarding(
				c.Request.Context(), "remote_engine", jobID, targetExecutor,
			)
			if persistErr != nil {
				pipelineLog("CREATE: failed to persist forwarding run=%s job=%s: %v",
					pr.ID, jobID, persistErr)
				if markErr := h.store.UpdatePipelineRunError(c.Request.Context(), pr.ID,
					"FORWARDING_PERSIST_FAILED", persistErr.Error(), "FORWARDING"); markErr != nil {
					pipelineLog("CREATE: failed to mark FORWARDING_PERSIST_FAILED run=%s: %v", pr.ID, markErr)
				}
				c.JSON(http.StatusInternalServerError, gin.H{
					"ok":              false,
					"pipeline_run_id": pr.ID,
					"request_id":      pr.RequestID,
					"status":          string(pipelineruns.StatusFailed),
					"error":           persistErr.Error(),
					"status_url":      "/api/v1/pipeline-runs/" + pr.ID,
				})
				return
			}

			pipelineLog("CREATE: persisted forwarding run=%s forwarding_id=%s status=%s",
				pr.ID, forwarding.ForwardingID, forwarding.Status)

			// Stamp forwarding_id + advance to REMOTE_QUEUED.
			pr.ForwardingID = forwarding.ForwardingID
			if err := h.store.UpdatePipelineRunForwarding(c.Request.Context(), pr.ID,
				forwarding.ForwardingID, pipelineruns.StatusRemoteQueued); err != nil {
				pipelineLog("CREATE: failed to stamp forwarding_id run=%s: %v", pr.ID, err)
			}

			// Update the run with the result JSON for audit.
			if resultJSON, mErr := json.Marshal(result); mErr == nil {
				if err := h.store.UpdatePipelineRunResult(c.Request.Context(), pr.ID, string(resultJSON)); err != nil {
					pipelineLog("CREATE: failed to stamp result_json run=%s: %v", pr.ID, err)
				}
			}

			c.JSON(http.StatusAccepted, gin.H{
				"ok":              true,
				"pipeline_run_id": pr.ID,
				"request_id":      pr.RequestID,
				"remote_job_id":   jobID,
				"forwarding_id":   forwarding.ForwardingID,
				"status":          string(pipelineruns.StatusRemoteQueued),
				"status_url":      "/api/v1/pipeline-runs/" + pr.ID,
			})
			return
		}

		// ── No job_id in the response — contract violation ────────────
		pipelineLog("CREATE: remote response missing job_id run=%s", pr.ID)
		if markErr := h.store.UpdatePipelineRunError(c.Request.Context(), pr.ID,
			"REMOTE_CONTRACT", "remote response missing job_id", "REMOTE_SUBMITTING"); markErr != nil {
			pipelineLog("CREATE: failed to mark REMOTE_CONTRACT run=%s: %v", pr.ID, markErr)
		}
		c.JSON(http.StatusBadGateway, gin.H{
			"ok":              false,
			"pipeline_run_id": pr.ID,
			"request_id":      pr.RequestID,
			"status":          string(pipelineruns.StatusFailed),
			"error":           "remote response missing job_id",
			"status_url":      "/api/v1/pipeline-runs/" + pr.ID,
		})
	}
}

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
		"status":          string(pipelineruns.StatusWorkerQueued),
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

// buildRemotePayload converts the typed CreatePipelineRunRequest into
// the map[string]interface{} shape the remote engine's
// /api/script/generate-with-images endpoint expects. The remote engine
// still consumes the legacy flat shape; this mapping isolates the
// versioned API contract from the remote engine's wire format.
func buildRemotePayload(req *CreatePipelineRunRequest) map[string]interface{} {
	payload := map[string]interface{}{}

	if req.Generation != nil {
		if req.Generation.Topic != "" {
			payload["topic"] = req.Generation.Topic
		}
		if req.Generation.SourceText != "" {
			payload["source_text"] = req.Generation.SourceText
		}
		if req.Generation.Language != "" {
			payload["language"] = req.Generation.Language
		}
		if req.Generation.Style != "" {
			payload["style"] = req.Generation.Style
		}
		if req.Generation.SceneCount > 0 {
			payload["scene_count"] = req.Generation.SceneCount
		}
	}

	if req.Output != nil {
		if req.Output.Format != "" {
			payload["format"] = req.Output.Format
		}
		if req.Output.Width > 0 {
			payload["width"] = req.Output.Width
		}
		if req.Output.Height > 0 {
			payload["height"] = req.Output.Height
		}
		if req.Output.FPS > 0 {
			payload["fps"] = req.Output.FPS
		}
	}

	if req.VideoMetadata != nil {
		meta := map[string]interface{}{}
		if req.VideoMetadata.Title != "" {
			meta["title"] = req.VideoMetadata.Title
		}
		if req.VideoMetadata.Description != "" {
			meta["description"] = req.VideoMetadata.Description
		}
		if len(req.VideoMetadata.Tags) > 0 {
			meta["tags"] = req.VideoMetadata.Tags
		}
		if req.VideoMetadata.PrivacyStatus != "" {
			meta["privacy_status"] = req.VideoMetadata.PrivacyStatus
		}
		if len(meta) > 0 {
			payload["video_metadata"] = meta
		}
	}

	if len(req.DeliveryPlan) > 0 {
		plan := make([]interface{}, 0, len(req.DeliveryPlan))
		for _, d := range req.DeliveryPlan {
			item := map[string]interface{}{}
			if d.Provider != "" {
				item["provider"] = d.Provider
			}
			if d.ChannelID != "" {
				item["channel_id"] = d.ChannelID
			}
			if d.PublishAt != "" {
				item["publish_at"] = d.PublishAt
			}
			if d.Destination != "" {
				item["destination_id"] = d.Destination
			}
			plan = append(plan, item)
		}
		payload["delivery_plan"] = plan
	}

	return payload
}
