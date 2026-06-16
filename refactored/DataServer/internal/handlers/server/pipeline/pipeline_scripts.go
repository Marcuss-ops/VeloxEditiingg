package pipeline

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/remoteengine"
)

// ScriptSimple handles POST /api/script-simple
func ScriptSimple(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req remoteengine.SimpleScriptRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid JSON"})
			return
		}

		pipelineLog("SCRIPT_SIMPLE: topic=%q language=%s", req.Topic, req.Language)

		if remoteEngineClient != nil && remoteEngineClient.IsConfigured() {
			result, err := remoteEngineClient.GenerateSimpleScript(c.Request.Context(), req)
			if err != nil {
				pipelineLog("SCRIPT_SIMPLE: FAILED: %v", err)
				c.JSON(http.StatusBadGateway, gin.H{"ok": false, "error": err.Error()})
				return
			}
			pipelineLog("SCRIPT_SIMPLE: ok=%v trace_id=%s", result.OK, result.TraceID)
			c.JSON(http.StatusOK, result)
			return
		}

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

		pipelineLog("SCRIPT_MULTIPLE: topics=%d language=%s", len(req.Topics), req.Language)

		if remoteEngineClient != nil && remoteEngineClient.IsConfigured() {
			result, err := remoteEngineClient.GenerateBatchScripts(c.Request.Context(), req)
			if err != nil {
				pipelineLog("SCRIPT_MULTIPLE: FAILED: %v", err)
				c.JSON(http.StatusBadGateway, gin.H{"ok": false, "error": err.Error()})
				return
			}
			pipelineLog("SCRIPT_MULTIPLE: ok=%v scripts=%d", result.OK, len(result.Scripts))
			c.JSON(http.StatusOK, result)
			return
		}

		c.JSON(http.StatusServiceUnavailable, gin.H{
			"ok":    false,
			"error": "remote engine not configured",
			"hint":  "set VELOX_REMOTE_ENGINE_URL",
		})
	}
}
