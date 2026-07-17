// Package pipeline: generate.go isolates the legacy POST /api/remote/pipeline/generate
// handler into its own file. Same package as the rest of the split pipeline
// package (so package-internal symbols — pipelineLog, firstStringResolver,
// forwardPipelineResultToWorker, isTerminalStatus — are visible without
// re-export).
//
// What this file owns:
//   - h.Generate(): the handler.
//   - request parsing: c.ShouldBindJSON + payload.FirstString for topic,
//     language, style and direct read of scene_count / idempotency_key.
//   - inline validation: enqueuer-wired check (returns 503 if nil),
//     remote-engine-configured check (returns 503 if !IsConfigured),
//     JSON-syntax check (returns 400).
//   - the typed-DTO adoption (Area 2): the raw result map returned by
//     remoteengine.Client.StartPipeline is parsed via
//     remoteengine.ParseRemotePipelineResult and the worker payload is
//     derived from `dto.ToWorkerPayload()`. The sync-forward,
//     async-forward persistence, and response-envelope branches
//     operate exclusively on the typed payload (with the legacy map
//     consulted only for `status`).
//   - voiceover asset acquisition error mapping
//     (voiceoverassets.AsAcquisitionError → 422 with structured fields).
//
// What stays / what got moved:
//   - forwardPipelineResultToWorker   → forwarding.go (Step 4 done).
//   - firstStringResolver             → forwarding.go (Step 4 done).
//   - pipelineLog                     → logging.go (Step 9 done) — the
//     cross-file logging convention is preserved by keeping it package-
//     internal and callable from any sibling file.
//   - RegisterRoutes                  → routes.go (Step 2 done).
//
// The handler runs alongside POST /api/v1/pipeline-runs
// (CreatePipelineRun, in pipeline_create.go) which is the durable,
// idempotent, versioned entry point; legacy Generate remains for
// callers that still POST the raw map and don't persist a
// pipeline_run row.
package pipeline

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"velox-shared/payload"

	voiceoverassets "velox-server/internal/assets"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/remoteengine"
)

// Generate handles POST /api/remote/pipeline/generate.
//
// Dependencies (enqueuer, remote engine client) are read from the
// receiver `h` rather than from package-level globals, so two
// concurrent tests or two pipelines mounted on different admin groups
// cannot collide through shared state.
//
// Area 2: the typed RemotePipelineResult DTO adoption is in force.
// The raw `result map[string]interface{}` returned by StartPipeline
// is parsed via ParseRemotePipelineResult and the worker payload is
// derived from `dto.ToWorkerPayload()`. The sync-forward and async-
// forward branches operate on the typed payload.
func (h *Handlers) Generate() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h.enqueuer == nil {
			pipelineLog("REQUEST: enqueuer not wired — returning 503")
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"ok":    false,
				"error": "pipeline enqueuer not wired (call NewHandlers(... enqueuer) at composition root)",
			})
			return
		}

		var reqPayload map[string]interface{}
		if err := c.ShouldBindJSON(&reqPayload); err != nil {
			pipelineLog("REQUEST: invalid JSON from %s: %v", c.ClientIP(), err)
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid JSON"})
			return
		}

		topic := payload.FirstString(reqPayload, "topic", "title", "source_text")
		language := payload.FirstString(reqPayload, "language")
		style := payload.FirstString(reqPayload, "style")
		sceneCount := reqPayload["scene_count"]
		pipelineLog("REQUEST: received topic=%q language=%s style=%s scenes=%v", topic, language, style, sceneCount)

		if h.client == nil || !h.client.IsConfigured() {
			pipelineLog("REQUEST: remote engine NOT configured — returning 503")
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"ok":    false,
				"error": "remote engine not configured",
				"hint":  "set VELOX_REMOTE_ENGINE_URL",
			})
			return
		}

		pipelineLog("REMOTE: forwarding to %s/api/script/generate-with-images", h.cfg.Render.RemoteEngineURL)
		// Legacy Generate path: no pipeline_run row exists, so we extract
		// the idempotency key from the payload if the client sent one.
		idemKey, _ := reqPayload["idempotency_key"].(string)
		result, err := h.client.StartPipeline(c.Request.Context(), reqPayload, idemKey)
		if err != nil {
			pipelineLog("REMOTE: request FAILED: %v", err)
			c.JSON(http.StatusBadGateway, gin.H{"ok": false, "error": err.Error()})
			return
		}

		// Area 2: Parse the raw result into the typed DTO and derive
		// the worker payload. The remote result must NOT be passed
		// raw to the worker — it must first be converted to a typed
		// RemotePipelineResult.
		dto, _ := remoteengine.ParseRemotePipelineResult(result)
		workerPayload := dto.ToWorkerPayload()

		jobID, _ := workerPayload["job_id"].(string)
		status, _ := result["status"].(string)
		if jobID != "" {
			pipelineLog("REMOTE: response job_id=%s status=%s", jobID, status)
		} else {
			pipelineLog("REMOTE: response ok=%v status=%s", result["ok"], status)
		}

		response := gin.H{}
		for k, v := range result {
			response[k] = v
		}

		// Try synchronous forward if the remote already returned a
		// complete result.
		if enqueue.ShouldForwardPipelineResult(workerPayload) {
			pipelineLog("FORWARD: result complete — forwarding to Velox workers (sync)")
			if forwarded, forwardErr := h.forwardPipelineResultToWorker(c.Request.Context(), workerPayload); forwardErr != nil {
				if assetErr, ok := voiceoverassets.AsAcquisitionError(forwardErr); ok {
					c.JSON(http.StatusUnprocessableEntity, gin.H{
						"ok":          false,
						"code":        assetErr.Code,
						"field":       assetErr.Field,
						"message":     assetErr.Message,
						"source_type": assetErr.SourceType,
					})
					return
				}
				pipelineLog("FORWARD: FAILED: %v", forwardErr)
				response["worker_forwarded"] = false
				response["worker_forward_error"] = forwardErr.Error()
			} else {
				workerJobID, _ := forwarded["job_id"].(string)
				pipelineLog("FORWARD: SUCCESS job_id=%s", workerJobID)
				response["worker_forwarded"] = true
				response["worker_forward_result"] = forwarded
			}
		} else if jobID != "" && !isTerminalStatus(status) {
			if h.resolver == nil || !h.resolver.HasDBAccess() {
				pipelineLog("FORWARD: durable resolver unavailable for remote job=%s", jobID)
				c.JSON(http.StatusServiceUnavailable, gin.H{
					"ok":    false,
					"error": "durable forwarding is not configured",
				})
				return
			}

			targetExecutor := firstStringResolver(workerPayload, "executor_id", "pipeline_id")
			forwarding, persistErr := h.resolver.PersistPendingRemoteForwarding(
				c.Request.Context(), "remote_engine", jobID, targetExecutor,
			)
			if persistErr != nil {
				pipelineLog("FORWARD: failed to persist remote job=%s: %v", jobID, persistErr)
				c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": persistErr.Error()})
				return
			}

			pipelineLog("FORWARD: persisted remote job=%s forwarding_id=%s status=%s",
				jobID, forwarding.ForwardingID, forwarding.Status)
			response["ok"] = true
			response["remote_job_id"] = jobID
			response["forwarding_id"] = forwarding.ForwardingID
			response["forwarding_status"] = forwarding.Status
			response["worker_forwarded"] = false
			response["worker_forward_error"] = "remote result is pending; durable forwarding runner will resume it"
			c.JSON(http.StatusAccepted, response)
			return
		} else if jobID != "" {
			pipelineLog("FORWARD: result NOT complete for job %s (status=%s) — missing scenes/voiceover", jobID, status)
			response["worker_forwarded"] = false
			response["worker_forward_error"] = "pipeline result is not complete enough for worker handoff — missing scenes/voiceover"
		}

		c.JSON(http.StatusOK, response)
	}
}
