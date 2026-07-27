package pipeline

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/creatorflow"
)

// SubmitJobRequest is the simplified, versioned API contract for
// POST /api/v1/jobs. It allows external systems to submit complete
// video jobs without going through the Creator intermediary.
//
// The format is intentionally flat and intuitive:
//   - No nested "payload" envelope
//   - No source_provider/source_job_id/target_executor_id ceremony
//   - idempotency_key is the single dedup handle
//
// The system derives the Creator-compatible identity tuple
// (source_provider, source_job_id, target_executor_id) automatically.
type SubmitJobRequest struct {
	// IdempotencyKey is required. Two requests with the same key
	// converge on the same Velox job (idempotent).
	IdempotencyKey string `json:"idempotency_key" binding:"required"`

	// VideoName is the display name for the resulting video.
	VideoName string `json:"video_name"`

	// ScriptText is the plain-text script used for TTS / overlay.
	ScriptText string `json:"script_text"`

	// VoiceoverPaths are voiceover audio references. Each entry MUST
	// be a velox-asset:// URI or a fully-qualified reachable URL.
	VoiceoverPaths []string `json:"voiceover_paths"`

	// Scenes is the scene list. Each scene drives one composited segment.
	Scenes []SubmitScene `json:"scenes"`

	// DeliveryPlan is the ordered list of delivery targets.
	DeliveryPlan []SubmitDeliveryPlanEntry `json:"delivery_plan"`
}

// SubmitScene is a single scene in the simplified job submission format.
type SubmitScene struct {
	// Text is the narration / overlay text for this scene.
	Text string `json:"text"`

	// ClipLink is a velox-asset:// clip URI or reachable URL.
	ClipLink string `json:"clip_link,omitempty"`

	// ImageLink is an optional image fallback.
	ImageLink string `json:"image_link,omitempty"`

	// DurationSeconds is the intended duration of the scene.
	DurationSeconds float64 `json:"duration_seconds"`
}

// SubmitDeliveryPlanEntry is a single destination in the delivery plan.
type SubmitDeliveryPlanEntry struct {
	DestinationID string `json:"destination_id"`
	Priority      int    `json:"priority,omitempty"`
	RetryBudget   int    `json:"retry_budget,omitempty"`
	Metadata      any    `json:"metadata,omitempty"`
}

// SubmitJob handles POST /api/v1/jobs.
//
// This is the simplified, external-friendly entry point for job
// submission. It converts the flat request into the Creator-push
// format and delegates to the same resolver machinery.
//
// The idempotency_key is used to derive:
//   - source_provider: "api_<key_prefix>" (first 32 chars of key)
//   - source_job_id: the full key
//   - target_executor_id: "scene.composite.v1" (default)
func (h *Handlers) SubmitJob() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req SubmitJobRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"ok":      false,
				"error":   "invalid_json",
				"message": "request body must be valid JSON",
			})
			return
		}

		req.IdempotencyKey = strings.TrimSpace(req.IdempotencyKey)
		if req.IdempotencyKey == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"ok":      false,
				"error":   "invalid_payload",
				"message": "idempotency_key is required",
			})
			return
		}

		if len(req.Scenes) == 0 {
			c.JSON(http.StatusUnprocessableEntity, gin.H{
				"ok":      false,
				"error":   "invalid_payload",
				"message": "at least one scene is required",
			})
			return
		}

		for i, scene := range req.Scenes {
			if scene.DurationSeconds <= 0 {
				c.JSON(http.StatusUnprocessableEntity, gin.H{
					"ok":      false,
					"error":   "invalid_payload",
					"message": fmt.Sprintf("scenes[%d].duration_seconds must be > 0", i),
				})
				return
			}
		}

		// Derive Creator-compatible identity from idempotency_key.
		sourceProvider := "api_" + truncateKey(req.IdempotencyKey, 32)
		sourceJobID := req.IdempotencyKey
		targetExecutorID := "scene.composite.v1"

		// Build the worker payload from the simplified request.
		workerPayload := buildWorkerPayloadFromSubmit(&req)

		// Delegate to the same resolver used by CreatorPush.
		forwarded, err := h.resolveCompletedPayload(
			c.Request.Context(),
			sourceProvider,
			sourceJobID,
			targetExecutorID,
			workerPayload,
		)
		if err != nil {
			// P0 #2 contract: every resolver-layer error is mapped
			// to the canonical HTTP envelope by the shared helper
			// in package creatorflow. Previously this branch was
			// missing the enqueue.ValidationErrorField mapping
			// entirely, so any enqueue-layer validation error
			// (missing delivery_plan entry, missing retry_budget,
			// malformed destination_id, …) silently downgraded
			// to a 500.
			creatorflow.WriteResolverError(c, err, "idempotency_key")
			return
		}
		if forwarded == nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"ok":      false,
				"error":   "resolver_failure",
				"message": "job resolved without an enqueue response",
			})
			return
		}

		response := gin.H{}
		for key, value := range forwarded {
			response[key] = value
		}
		response["ok"] = true
		response["accepted_from"] = "api_v1_jobs"
		response["idempotency_key"] = req.IdempotencyKey
		if _, owned := response["dispatch_status"]; !owned {
			response["dispatch_status"] = "queued_for_workers"
		}

		h.intakeSinkOrNoop().IncAccepted("api_v1_jobs")
		jobID, _ := response["job_id"].(string)
		pipelineLog(
			"API_V1_JOBS_ACCEPTED idem=%s job_id=%s",
			req.IdempotencyKey,
			jobID,
		)

		c.JSON(http.StatusAccepted, response)
	}
}

// buildWorkerPayloadFromSubmit converts a SubmitJobRequest into the
// map[string]interface{} shape that the resolver expects.
func buildWorkerPayloadFromSubmit(req *SubmitJobRequest) map[string]interface{} {
	m := map[string]interface{}{
		"status": "completed",
		"job_id": req.IdempotencyKey,
	}

	if req.VideoName != "" {
		m["video_name"] = req.VideoName
	}
	if req.ScriptText != "" {
		m["script_text"] = req.ScriptText
	}
	if len(req.VoiceoverPaths) > 0 {
		m["voiceover_paths"] = req.VoiceoverPaths
	}

	if len(req.Scenes) > 0 {
		scenes := make([]interface{}, 0, len(req.Scenes))
		for _, s := range req.Scenes {
			scene := map[string]interface{}{
				"text":            s.Text,
				"duration_seconds": s.DurationSeconds,
			}
			if s.ClipLink != "" {
				scene["clip_link"] = s.ClipLink
			}
			if s.ImageLink != "" {
				scene["image_link"] = s.ImageLink
			}
			scenes = append(scenes, scene)
		}
		m["scenes"] = scenes
	}

	if len(req.DeliveryPlan) > 0 {
		plan := make([]interface{}, 0, len(req.DeliveryPlan))
		for _, d := range req.DeliveryPlan {
			entry := map[string]interface{}{
				"destination_id": d.DestinationID,
			}
			if d.Priority > 0 {
				entry["priority"] = d.Priority
			}
			if d.RetryBudget > 0 {
				entry["retry_budget"] = d.RetryBudget
			}
			if d.Metadata != nil {
				entry["metadata"] = d.Metadata
			}
			plan = append(plan, entry)
		}
		m["delivery_plan"] = plan
	}

	return m
}

// truncateKey returns the first n characters of s, or s if shorter.
func truncateKey(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
