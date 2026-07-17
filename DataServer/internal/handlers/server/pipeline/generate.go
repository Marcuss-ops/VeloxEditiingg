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
//     derived from `dto.ToWorkerPayload()`. The async-forward
//     persistence and response-envelope branches operate exclusively on
//     the typed payload (with the legacy map consulted only for
//     `status`).
//   - Area 3: the handler now creates a pipeline_run, calls
//     StartPipeline, stamps remote_job_id, persists a PENDING
//     creator_forwardings row, and returns 202. No volatile polling or
//     synchronous forwarding happens here — the durable
//     CreatorForwardingRunner owns polling and forwarding.
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
// callers that still POST the raw map.
package pipeline

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"velox-shared/payload"

	"velox-server/internal/pipelineruns"
	"velox-server/internal/remoteengine"
)

// Generate handles POST /api/remote/pipeline/generate.
//
// Dependencies (store, enqueuer, remote engine client, resolver) are
// read from the receiver `h` rather than from package-level globals, so
// two concurrent tests or two pipelines mounted on different admin
// groups cannot collide through shared state.
//
// Area 3: the handler creates a pipeline_run, calls StartPipeline,
// stamps remote_job_id, persists a PENDING creator_forwardings row,
// and returns 202. No polling or synchronous forwarding happens here —
// the durable CreatorForwardingRunner owns the rest of the lifecycle.
func (h *Handlers) Generate() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h.store == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"ok":    false,
				"error": "pipeline store not wired",
			})
			return
		}
		if h.client == nil || !h.client.IsConfigured() {
			pipelineLog("REQUEST: remote engine NOT configured — returning 503")
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"ok":    false,
				"error": "remote engine not configured",
				"hint":  "set VELOX_REMOTE_ENGINE_URL",
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

		// Area 3: create a pipeline_run before calling the remote engine.
		idemKey, _ := reqPayload["idempotency_key"].(string)
		if idemKey == "" {
			idemKey = "gen_" + uuid.NewString()
		}
		now := time.Now().UTC()
		runID := "run_" + uuid.NewString()
		requestID := "req_" + uuid.NewString()

		insertResult, err := h.store.InsertPipelineRun(c.Request.Context(), &pipelineruns.PipelineRun{
			ID:                   runID,
			RequestID:            requestID,
			IdempotencyKey:       idemKey,
			Status:               pipelineruns.StatusAccepted,
			RequestedPayloadJSON: "{}",
			CreatedAt:            now,
			UpdatedAt:            now,
		})
		if err != nil {
			pipelineLog("REQUEST: failed to insert pipeline_run: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "failed to create pipeline run"})
			return
		}
		pr := insertResult.Run
		if !insertResult.Created {
			pipelineLog("REQUEST: idempotent duplicate idem=%s → run=%s", idemKey, pr.ID)
		}

		pipelineLog("REMOTE: forwarding to %s/api/script/generate-with-images", h.cfg.Render.RemoteEngineURL)
		result, err := h.client.StartPipeline(c.Request.Context(), reqPayload, pr.ID)
		if err != nil {
			pipelineLog("REMOTE: request FAILED run=%s: %v", pr.ID, err)
			_ = h.store.UpdatePipelineRunError(c.Request.Context(), pr.ID,
				"REMOTE_CALL_FAILED", err.Error(), "REMOTE_SUBMITTING")
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

		// Area 2: Parse the raw result into the typed DTO and derive
		// the worker payload. The remote result must NOT be passed
		// raw to the worker — it must first be converted to a typed
		// RemotePipelineResult.
		dto, _ := remoteengine.ParseRemotePipelineResult(result)
		workerPayload := dto.ToWorkerPayload()

		jobID := firstStringResolver(workerPayload, "job_id", "trace_id", "id")
		status := firstStringResolver(result, "status")
		if jobID != "" {
			pipelineLog("REMOTE: response run=%s job_id=%s status=%s", pr.ID, jobID, status)
		} else {
			pipelineLog("REMOTE: response run=%s ok=%v status=%s", pr.ID, result["ok"], status)
		}

		// Stamp remote_job_id on the pipeline_run.
		if jobID != "" {
			pr.RemoteJobID = jobID
			pr.RemoteProvider = "remote_engine"
			if err := h.store.UpdatePipelineRunRemoteJob(c.Request.Context(), pr.ID, "remote_engine", jobID); err != nil {
				pipelineLog("REQUEST: failed to stamp remote_job_id run=%s: %v", pr.ID, err)
			}
		}

		// Area 3: persist a PENDING forwarding row and return 202.
		// The handler never polls or forwards synchronously.
		if jobID != "" {
			if h.resolver == nil || !h.resolver.HasDBAccess() {
				pipelineLog("FORWARD: durable resolver unavailable for remote job=%s", jobID)
				_ = h.store.UpdatePipelineRunError(c.Request.Context(), pr.ID,
					"RESOLVER_UNAVAILABLE", "durable forwarding is not configured", "FORWARDING")
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

			targetExecutor := firstStringResolver(workerPayload, "executor_id", "pipeline_id")
			forwarding, persistErr := h.resolver.PersistPendingRemoteForwarding(
				c.Request.Context(), "remote_engine", jobID, targetExecutor,
			)
			if persistErr != nil {
				pipelineLog("FORWARD: failed to persist remote job=%s: %v", jobID, persistErr)
				_ = h.store.UpdatePipelineRunError(c.Request.Context(), pr.ID,
					"FORWARDING_PERSIST_FAILED", persistErr.Error(), "FORWARDING")
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

			pipelineLog("FORWARD: persisted remote job=%s forwarding_id=%s status=%s",
				jobID, forwarding.ForwardingID, forwarding.Status)

			pr.ForwardingID = forwarding.ForwardingID
			if err := h.store.UpdatePipelineRunForwarding(c.Request.Context(), pr.ID,
				forwarding.ForwardingID, pipelineruns.StatusRemoteQueued); err != nil {
				pipelineLog("REQUEST: failed to stamp forwarding_id run=%s: %v", pr.ID, err)
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

		// No job_id in the response — contract violation.
		pipelineLog("REQUEST: remote response missing job_id run=%s", pr.ID)
		_ = h.store.UpdatePipelineRunError(c.Request.Context(), pr.ID,
			"REMOTE_CONTRACT", "remote response missing job_id", "REMOTE_SUBMITTING")
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
