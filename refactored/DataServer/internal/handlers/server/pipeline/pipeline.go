package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"velox-shared/payload"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/creatorflow"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/queue"
	"velox-server/internal/remoteengine"
)

var remoteEngineClient *remoteengine.Client

// structured log helper
func pipelineLog(format string, args ...interface{}) {
	log.Printf("[PIPELINE] "+format, args...)
}

// InitRemoteEngine initializes the remote engine client
func InitRemoteEngine(cfg *config.Config) {
	if cfg.Render.RemoteEngineURL == "" {
		pipelineLog("INIT: remote engine NOT configured (VELOX_REMOTE_ENGINE_URL empty)")
		return
	}
	remoteEngineClient = remoteengine.NewClient(remoteengine.Config{
		URL:       cfg.Render.RemoteEngineURL,
		Token:     cfg.Render.RemoteEngineToken,
		TimeoutMS: cfg.Render.RemoteEngineTimeoutMS,
		Retries:   cfg.Render.RemoteEngineRetries,
	})
	pipelineLog("INIT: remote engine configured url=%s timeout_ms=%d retries=%d poll_interval=%ds",
		cfg.Render.RemoteEngineURL, cfg.Render.RemoteEngineTimeoutMS, cfg.Render.RemoteEngineRetries, cfg.Render.RemoteEnginePollInterval)
}

// PipelineGenerate handles POST /api/remote/pipeline/generate
func PipelineGenerate(cfg *config.Config, q *queue.FileQueue) gin.HandlerFunc {
	return func(c *gin.Context) {
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

		if remoteEngineClient == nil || !remoteEngineClient.IsConfigured() {
			pipelineLog("REQUEST: remote engine NOT configured — returning 503")
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"ok":    false,
				"error": "remote engine not configured",
				"hint":  "set VELOX_REMOTE_ENGINE_URL",
			})
			return
		}

		pipelineLog("REMOTE: forwarding to %s/api/script/generate-with-images", cfg.Render.RemoteEngineURL)
		result, err := remoteEngineClient.StartPipeline(c.Request.Context(), reqPayload)
		if err != nil {
			pipelineLog("REMOTE: request FAILED: %v", err)
			c.JSON(http.StatusBadGateway, gin.H{"ok": false, "error": err.Error()})
			return
		}

		jobID, _ := result["job_id"].(string)
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

		// Try synchronous forward if result is already complete
		if enqueue.ShouldForwardPipelineResult(result) {
			pipelineLog("FORWARD: result complete — forwarding to Velox workers (sync)")
			if forwarded, forwardErr := forwardPipelineResultToWorker(c.Request.Context(), q, result); forwardErr != nil {
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
			// Async: start background polling
			pollInterval := cfg.Render.RemoteEnginePollInterval
			if pollInterval <= 0 {
				pollInterval = 30
			}
			maxPolls := (1800 + pollInterval - 1) / pollInterval
			if maxPolls > 120 {
				maxPolls = 120
			}
			pipelineLog("POLL: starting background polling job_id=%s status=%s interval=%ds max_polls=%d (~%d min timeout)",
				jobID, status, pollInterval, maxPolls, pollInterval*maxPolls/60)
			startPipelinePolling(remoteEngineClient, q, jobID, pollInterval)
			response["polling_enabled"] = true
			response["poll_interval_sec"] = pollInterval
			response["worker_forwarded"] = false
			response["worker_forward_error"] = "pipeline result is not complete yet — background polling started"
		} else if jobID != "" {
			pipelineLog("FORWARD: result NOT complete for job %s (status=%s) — missing scenes/voiceover", jobID, status)
			response["worker_forwarded"] = false
			response["worker_forward_error"] = "pipeline result is not complete enough for worker handoff — missing scenes/voiceover"
		}

		c.JSON(http.StatusOK, response)
	}
}

func forwardPipelineResultToWorker(ctx context.Context, q *queue.FileQueue, result map[string]interface{}) (map[string]interface{}, error) {
	pipelineLog("FORWARD: building worker payload...")
	enqueued, err := creatorflow.ForwardCompletedResult(ctx, q, result)
	if err != nil {
		pipelineLog("FORWARD: ForwardCompletedResult FAILED: %v", err)
		return nil, err
	}
	if enqueued == nil {
		return nil, fmt.Errorf("forward completed result returned no enqueue response")
	}

	jobPayload, buildErr := enqueue.BuildPipelinePayload(result)
	if buildErr == nil {
		payloadJSON, _ := json.Marshal(jobPayload)
		if len(payloadJSON) > 500 {
			pipelineLog("FORWARD: payload built size=%d bytes title=%s scenes=%v",
				len(payloadJSON), jobPayload["title"], jobPayload["scene_count"])
		} else {
			pipelineLog("FORWARD: payload built: %s", string(payloadJSON))
		}
	}
	pipelineLog("FORWARD: enqueued to Velox queue job_id=%v", enqueued["job_id"])
	return enqueued, nil
}
