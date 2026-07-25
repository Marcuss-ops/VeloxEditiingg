package pipeline

import (
	"context"
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

// normalizeCreatorPushRequest is the canonical typed-DTO adapter
// for POST /api/v1/creator/jobs. It runs the raw creator map through
// ParseRemotePipelineResult, derives the worker payload via
// ToWorkerPayload, and produces the resolver identity tuple
// (source_provider + source_job_id + target_executor_id) with the
// documented fallback chain.
//
// source_provider defaults to defaultCreatorSourceProvider when
// empty. source_job_id falls back to dto.RemoteJobID and then to the
// worker payload's (job_id|trace_id|id) keys. target_executor_id
// falls back to the worker payload's (executor_id|pipeline_id) keys,
// then to the hardcoded "scene.composite.v1" only when both the
// request and the payload are silent — callers SHOULD pass an
// explicit target to avoid silent fallback.
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

// resolveCompletedPayload is the canonical HTTP-side adapter into
// creatorflow.Resolver.Resolve. It is intentionally provider-agnostic
// so any future creator intake (creator_push today, plus any new
// producer that wants the same atomic forwarding + Job+Task write
// path) converges on the same code. Previously this lived in
// forwarding.go alongside the now-removed legacy sync-forward path;
// after that path was retired the helper moved here as the single
// remaining Module's resolver entry call.
func (h *Handlers) resolveCompletedPayload(
	ctx context.Context,
	sourceProvider string,
	sourceJobID string,
	targetExecutorID string,
	result map[string]interface{},
) (map[string]interface{}, error) {
	if h.resolver == nil {
		return nil, fmt.Errorf("pipeline handler requires a wired resolver (composition root MUST pass creatorflow.Resolver)")
	}

	sourceProvider = strings.TrimSpace(sourceProvider)
	if sourceProvider == "" {
		return nil, fmt.Errorf("source_provider is required")
	}
	sourceJobID = strings.TrimSpace(sourceJobID)
	if sourceJobID == "" {
		return nil, fmt.Errorf("source_job_id is required")
	}
	if result == nil {
		return nil, fmt.Errorf("payload is required")
	}

	out, err := h.resolver.Resolve(ctx, creatorflow.ResolveRequest{
		ForwardingID:     "",
		SourceProvider:   sourceProvider,
		SourceJobID:      sourceJobID,
		TargetExecutorID: strings.TrimSpace(targetExecutorID),
		Payload:          result,
	})
	if err != nil {
		pipelineLog("FORWARD: Resolver.Resolve FAILED provider=%s source_job=%s: %v", sourceProvider, sourceJobID, err)
		return nil, err
	}
	if out == nil {
		return nil, nil
	}

	pipelineLog(
		"FORWARD: enqueued via Resolver provider=%s source_job=%s job_id=%s forwarding_id=%s",
		sourceProvider,
		sourceJobID,
		out.JobID,
		out.ForwardingID,
	)
	return out.Response, nil
}

// firstStringResolver reads the first non-empty string value from a
// map across the provided keys. Package-private helper (no tests
// outside this package should depend on it directly).
func firstStringResolver(m map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if v, ok := m[key]; ok {
			if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
				return strings.TrimSpace(s)
			}
		}
	}
	return ""
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
		if _, owned := response["dispatch_status"]; !owned {
			response["dispatch_status"] = "queued_for_workers"
		}

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
