// Status endpoint + remote-status classification helper for the pipeline
// HTTP layer.
//
// PR-DI-pipeline: these two symbols were previously co-located in
// pipeline_lifecycle.go alongside Cancel(). They are extracted here so
// each handler -- status, cancel -- lives in its own focused file with
// the imports it actually needs. isTerminalStatus is a pure
// package-level helper (no receiver) that classifies a remote-engine
// status string into "stop polling" vs "keep polling" buckets; it is
// consumed by the forwarding loop in generate.go.
//
// Step 5 of the original pipeline.go split. Diff is move-only.
package pipeline

import (
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// isTerminalStatus is the pure helper that classifies a remote-engine
// status string into "stop polling" vs "keep polling". Kept package-level
// (not a method) because it takes no receiver dependency.
func isTerminalStatus(status string) bool {
	s := strings.ToLower(strings.TrimSpace(status))
	return s == "completed" || s == "succeeded" || s == "done" || s == "failed" || s == "error"
}

// Status handles GET /api/remote/pipeline/status/:trace_id.
//
// PR-DI-pipeline: client dependency now read from receiver `h`.
func (h *Handlers) Status() gin.HandlerFunc {
	return func(c *gin.Context) {
		traceID := c.Param("trace_id")
		pipelineLog("STATUS: requested job_id=%s", traceID)

		if h.client == nil || !h.client.IsConfigured() {
			pipelineLog("STATUS: remote engine NOT configured")
			writeHTTPError(c, http.StatusServiceUnavailable,
				errors.New("remote engine not configured \u2014 set VELOX_REMOTE_ENGINE_URL"))
			return
		}

		status, err := h.client.GetPipelineStatus(c.Request.Context(), traceID)
		if err != nil {
			pipelineLog("STATUS: ERROR job_id=%s: %v", traceID, err)
			writeHTTPError(c, http.StatusBadGateway, err)
			return
		}

		pipelineLog("STATUS: job_id=%s status=%s progress=%.0f%%", traceID, status.Status, status.Progress)
		c.JSON(http.StatusOK, status)
	}
}
