// Package pipeline — pipeline-run creation HTTP handler.
//
// pipeline_create.go is the thin HTTP layer for POST /api/v1/pipeline-runs:
// bind the JSON body, delegate to the PipelineRunService, and write the
// returned status + body verbatim. All validation, durable pipeline_run
// creation, remote submission and forwarding persistence live in
// pipeline_run_service.go. The typed request contract (CreatePipelineRunRequest
// + spec structs) and the remote payload builder (buildRemotePayload) live in
// pipeline_create_payload.go.
package pipeline

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"velox-server/internal/creatorflow"
	velmetrics "velox-server/internal/metrics"
)

// CreatePipelineRun handles POST /api/v1/pipeline-runs.
func (h *Handlers) CreatePipelineRun() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req CreatePipelineRunRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"ok":    false,
				"error": "invalid JSON: " + err.Error(),
			})
			return
		}
		resp := h.pipelineRuns.Create(c.Request.Context(), ClientIDFromContext(c), req)
		// The durable pipeline-run surface is a distinct intake source from
		// the canonical job submitter; record it on an accepted create so
		// alias usage is measurable before any convergence work.
		if resp.Status >= 200 && resp.Status < 300 {
			velmetrics.RecordIntakeSource(creatorflow.IntakeSourcePipelineRun)
		}
		c.JSON(resp.Status, resp.Body)
	}
}
