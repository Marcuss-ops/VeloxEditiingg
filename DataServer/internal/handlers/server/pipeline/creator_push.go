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

// CreatorIntakeSink records the accepted payload count by intake path.
// Nil values are treated as a noop sink (safe default for tests and
// for callers that have not yet wired the metric). The canonical
// implementation lives in velox-server/internal/metrics.
type CreatorIntakeSink interface {
	IncAccepted(path string)
}

// noopCreatorIntakeSink is the safe default when no sink is wired.
// The handler falls back to it so a missing wiring never panics and
// never silently drops a metric event.
type noopCreatorIntakeSink struct{}

func (noopCreatorIntakeSink) IncAccepted(string) {}

// intakeSinkOrNoop returns the wired sink or a noop if not set. This
// is the single observation point the handler uses; the test suite
// asserts against the same accessor.
func (h *Handlers) intakeSinkOrNoop() CreatorIntakeSink {
	if h.intakeSink == nil {
		return noopCreatorIntakeSink{}
	}
	return h.intakeSink
}

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
		// Surface the dispatch status so callers can split creator_push
		// traffic from any other producer at the wire level. The Resolver
		// already emits job status (PENDING); this overlay adds the
		// dispatch-side marker that docs/CREATOR-PUSH.md and the creator
		// contract both declare. GUARDED: only stamp when the Resolver
		// response envelope does not already carry dispatch_status, so
		// a future Resolver that emits "dispatching" / "dispatched"
		// states is not silently clobbered back to "queued_for_workers".
		// The empty-present case (Resolver returning dispatch_status=""
		// empty) is intentionally treated as Resolver-claimed — the
		// handler does NOT re-stamp. Don't add a value=="" guard.
		if _, owned := response["dispatch_status"]; !owned {
			response["dispatch_status"] = "queued_for_workers"
		}

		// Fail-closed observation point (runtime-invariants.md §4.4):
		// the log + counter are stamped ONLY after the atomic CAS
		// committed (resolveCompletedPayload returned success), so we
		// never log a "success" that the database has not seen. The
		// increment is the canonical signal that the new creator_push
		// intake path was exercised; the legacy async
		// CreatorForwardingRunner stamps the same counter with
		// path="creator_forwarder" so the push/forwarder adoption
		// ratio is comparable.
		h.intakeSinkOrNoop().IncAccepted("creator_push")
		jobID, _ := response["job_id"].(string)
		pipelineLog(
			"CREATOR_PUSH_ACCEPTED path=creator_push source_provider=%s source_job_id=%s target_executor_id=%s job_id=%s",
			normalized.SourceProvider,
			normalized.SourceJobID,
			normalized.TargetExecutorID,
			jobID,
		)

		c.JSON(http.StatusAccepted, response)
	}
}
