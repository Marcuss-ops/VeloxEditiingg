package pipeline

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/creatorflow"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/remoteengine"
)

const defaultCreatorSourceProvider = "creator"

type creatorPushRequest struct {
	SourceProvider   string                 `json:"source_provider"`
	SourceJobID      string                 `json:"source_job_id"`
	TargetExecutorID string                 `json:"target_executor_id"`
	Payload          map[string]interface{} `json:"payload"`
}

type normalizedCreatorPush struct {
	SourceProvider   string
	SourceJobID      string
	TargetExecutorID string
	WorkerPayload    map[string]interface{}
}

// normalizeCreatorPushRequest converts the external creator envelope through
// the same typed remote-engine DTO used by the master-initiated flow. The raw
// creator map is never forwarded directly to the worker.
func normalizeCreatorPushRequest(req creatorPushRequest) (*normalizedCreatorPush, error) {
	if req.Payload == nil {
		return nil, fmt.Errorf("payload is required")
	}

	dto, err := remoteengine.ParseRemotePipelineResult(req.Payload)
	if err != nil {
		return nil, fmt.Errorf("parse creator payload: %w", err)
	}
	workerPayload := dto.ToWorkerPayload()

	sourceProvider := strings.TrimSpace(req.SourceProvider)
	if sourceProvider == "" {
		sourceProvider = defaultCreatorSourceProvider
	}

	sourceJobID := strings.TrimSpace(req.SourceJobID)
	if sourceJobID == "" {
		sourceJobID = strings.TrimSpace(dto.RemoteJobID)
	}
	if sourceJobID == "" {
		sourceJobID = firstStringResolver(workerPayload, "job_id", "trace_id", "id")
	}
	if sourceJobID == "" {
		return nil, fmt.Errorf("source_job_id is required (set it in the envelope or payload.job_id)")
	}

	targetExecutorID := strings.TrimSpace(req.TargetExecutorID)
	if targetExecutorID == "" {
		targetExecutorID = firstStringResolver(workerPayload, "executor_id", "pipeline_id")
	}
	if targetExecutorID == "" {
		targetExecutorID = "scene.composite.v1"
	}

	return &normalizedCreatorPush{
		SourceProvider:   sourceProvider,
		SourceJobID:      sourceJobID,
		TargetExecutorID: targetExecutorID,
		WorkerPayload:    workerPayload,
	}, nil
}

// CreatorPush handles POST /api/v1/creator/jobs.
//
// A creator machine calls this endpoint only after it has assembled the full
// render payload (voiceover, scenes/clips/stock references and delivery plan).
// The master does not initiate or poll the creator. The completed payload enters
// the existing creatorflow.Resolver and is atomically converted into the same
// Job+Task shape used by every other producer.
func (h *Handlers) CreatorPush() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req creatorPushRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"ok":    false,
				"error": "invalid JSON",
			})
			return
		}

		normalized, err := normalizeCreatorPushRequest(req)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"ok":    false,
				"error": err.Error(),
			})
			return
		}

		forwarded, err := h.resolveCompletedPayload(
			c.Request.Context(),
			normalized.SourceProvider,
			normalized.SourceJobID,
			normalized.TargetExecutorID,
			normalized.WorkerPayload,
		)
		if err != nil {
			if err == creatorflow.ErrResolverNotComplete {
				c.JSON(http.StatusUnprocessableEntity, gin.H{
					"ok":     false,
					"error":  "creator payload is not complete enough to dispatch",
					"status": "PAYLOAD_INCOMPLETE",
				})
				return
			}
			if field := enqueue.ValidationErrorField(err); field != "" {
				c.JSON(http.StatusUnprocessableEntity, gin.H{
					"ok":    false,
					"error": err.Error(),
					"field": field,
				})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{
				"ok":    false,
				"error": "failed to enqueue creator payload",
			})
			return
		}
		if forwarded == nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"ok":    false,
				"error": "creator payload resolved without an enqueue response",
			})
			return
		}

		response := gin.H{}
		for key, value := range forwarded {
			response[key] = value
		}
		response["ok"] = true
		response["accepted_from"] = "creator_push"
		response["source_provider"] = normalized.SourceProvider
		response["source_job_id"] = normalized.SourceJobID
		response["target_executor_id"] = normalized.TargetExecutorID

		c.JSON(http.StatusAccepted, response)
	}
}
