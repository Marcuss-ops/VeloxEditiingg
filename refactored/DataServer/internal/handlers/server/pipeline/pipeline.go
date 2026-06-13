package pipeline

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"velox-server/internal/config"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/queue"
	"velox-server/internal/remoteengine"
)

var remoteEngineClient *remoteengine.Client

// InitRemoteEngine initializes the remote engine client
func InitRemoteEngine(cfg *config.Config) {
	remoteEngineClient = remoteengine.NewClient(remoteengine.Config{
		URL:       cfg.RemoteEngineURL,
		Token:     cfg.RemoteEngineToken,
		TimeoutMS: cfg.RemoteEngineTimeoutMS,
		Retries:   cfg.RemoteEngineRetries,
	})
}

// PipelineGenerate handles POST /api/remote/pipeline/generate
func PipelineGenerate(cfg *config.Config, q *queue.FileQueue) gin.HandlerFunc {
	return func(c *gin.Context) {
		var payload map[string]interface{}
		if err := c.ShouldBindJSON(&payload); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid JSON"})
			return
		}

		// Use remote engine if configured
		if remoteEngineClient != nil && remoteEngineClient.IsConfigured() {
			result, err := remoteEngineClient.StartPipeline(c.Request.Context(), payload)
			if err != nil {
				c.JSON(http.StatusBadGateway, gin.H{"ok": false, "error": err.Error()})
				return
			}
			response := gin.H{}
			for k, v := range result {
				response[k] = v
			}

			if enqueue.ShouldForwardPipelineResult(result) {
				if forwarded, forwardErr := forwardPipelineResultToWorker(c.Request.Context(), q, result); forwardErr != nil {
					response["worker_forwarded"] = false
					response["worker_forward_error"] = forwardErr.Error()
				} else {
					response["worker_forwarded"] = true
					response["worker_forward_result"] = forwarded
				}
			} else {
				response["worker_forwarded"] = false
				response["worker_forward_error"] = "pipeline result is not complete enough for worker handoff"
			}

			c.JSON(http.StatusOK, response)
			return
		}

		// Fallback: not configured
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"ok":    false,
			"error": "remote engine not configured",
			"hint":  "set VELOX_REMOTE_ENGINE_URL",
		})
	}
}

func forwardPipelineResultToWorker(ctx context.Context, q *queue.FileQueue, result map[string]interface{}) (map[string]interface{}, error) {
	jobPayload, err := enqueue.BuildPipelinePayload(result)
	if err != nil {
		return nil, err
	}
	return enqueue.EnqueueSceneVideoJob(ctx, q, jobPayload)
}

// PipelineStatus handles GET /api/remote/pipeline/status/<trace_id>
func PipelineStatus(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		traceID := c.Param("trace_id")

		// Use remote engine if configured
		if remoteEngineClient != nil && remoteEngineClient.IsConfigured() {
			result, err := remoteEngineClient.GetPipelineStatus(c.Request.Context(), traceID)
			if err != nil {
				c.JSON(http.StatusBadGateway, gin.H{"ok": false, "error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, result)
			return
		}

		// Fallback: not configured
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"ok":       false,
			"trace_id": traceID,
			"error":    "remote engine not configured",
			"hint":     "set VELOX_REMOTE_ENGINE_URL",
		})
	}
}

// ScriptSimple handles POST /api/script-simple
func ScriptSimple(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req remoteengine.SimpleScriptRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid JSON"})
			return
		}

		// Use remote engine if configured
		if remoteEngineClient != nil && remoteEngineClient.IsConfigured() {
			result, err := remoteEngineClient.GenerateSimpleScript(c.Request.Context(), req)
			if err != nil {
				c.JSON(http.StatusBadGateway, gin.H{"ok": false, "error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, result)
			return
		}

		// Fallback: not configured
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"ok":    false,
			"error": "remote engine not configured",
			"hint":  "set VELOX_REMOTE_ENGINE_URL",
		})
	}
}

// ScriptMultiple handles POST /api/script-multiple
func ScriptMultiple(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req remoteengine.BatchScriptRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid JSON"})
			return
		}

		// Use remote engine if configured
		if remoteEngineClient != nil && remoteEngineClient.IsConfigured() {
			result, err := remoteEngineClient.GenerateBatchScripts(c.Request.Context(), req)
			if err != nil {
				c.JSON(http.StatusBadGateway, gin.H{"ok": false, "error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, result)
			return
		}

		// Fallback: not configured
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"ok":    false,
			"error": "remote engine not configured",
			"hint":  "set VELOX_REMOTE_ENGINE_URL",
		})
	}
}
