package pipeline

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"velox-server/internal/remoteengine"
)

// ScriptSimple handles POST /api/script-simple.
//
// PR-DI-pipeline: the *remoteengine.Client dependency is now read
// from the receiver `h` rather than from the previous package-level
// `remoteEngineClient` global. Returns 503 cleanly when the client is
// nil or unconfigured (i.e. VELOX_REMOTE_ENGINE_URL was not set).
func (h *Handlers) ScriptSimple() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req remoteengine.SimpleScriptRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid JSON"})
			return
		}

		pipelineLog("SCRIPT_SIMPLE: topic=%q language=%s", req.Topic, req.Language)

		if h.client != nil && h.client.IsConfigured() {
			result, err := h.client.GenerateSimpleScript(c.Request.Context(), req)
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

// ScriptBatch handles POST /api/script-multiple.
//
// PR-DI-pipeline: dependency moved from the previous
// `remoteEngineClient` global to the receiver `h.client`.
func (h *Handlers) ScriptBatch() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req remoteengine.BatchScriptRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid JSON"})
			return
		}

		pipelineLog("SCRIPT_MULTIPLE: topics=%d language=%s", len(req.Topics), req.Language)

		if h.client != nil && h.client.IsConfigured() {
			result, err := h.client.GenerateBatchScripts(c.Request.Context(), req)
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
