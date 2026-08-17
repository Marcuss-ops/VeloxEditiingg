package jobs

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/creatorflow"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/jobs/ingress"
	velmetrics "velox-server/internal/metrics"
	"velox-server/internal/routing"
	"velox-shared/payload"
)

type Handler struct {
	registry *ingress.Registry
	enqueuer *enqueue.Enqueuer
}

func NewHandler(
	registry *ingress.Registry,
	enqueuer *enqueue.Enqueuer,
) *Handler {
	return &Handler{
		registry: registry,
		enqueuer: enqueuer,
	}
}

func (h *Handler) Submit() gin.HandlerFunc {
	return func(c *gin.Context) {
		h.submit(c, strings.TrimSpace(c.Param("kind")))
	}
}

func (h *Handler) SubmitFixed(kind string) gin.HandlerFunc {
	return func(c *gin.Context) {
		h.submit(c, kind)
	}
}

func (h *Handler) submit(c *gin.Context, kind string) {
	if h.registry == nil || h.enqueuer == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"ok":    false,
			"error": "job ingress unavailable",
		})
		return
	}

	definition, found := h.registry.Resolve(kind)
	if !found {
		c.JSON(http.StatusNotFound, gin.H{
			"ok":    false,
			"error": "unknown job kind",
			"kind":  kind,
		})
		return
	}

	var raw map[string]any
	if err := c.ShouldBindJSON(&raw); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"ok":    false,
			"error": "invalid JSON body",
		})
		return
	}
	if raw == nil {
		raw = map[string]any{}
	}

	canonicalPayload, err := definition.Builder(c.Request.Context(), raw)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"ok":    false,
			"error": err.Error(),
			"kind":  kind,
		})
		return
	}
	applyDefinition(canonicalPayload, definition)

	result, err := h.enqueuer.Enqueue(
		c.Request.Context(),
		canonicalPayload,
		definition.Requirements,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"ok":    false,
			"error": err.Error(),
			"kind":  kind,
		})
		return
	}
	// The script ingress is a direct-enqueue surface (it does not route
	// through the canonical submitter because it creates jobs from scratch
	// rather than forwarding a completed result). Record its intake source
	// so alias usage is measurable before any convergence work.
	velmetrics.RecordIntakeSource(scriptIntakeSourceForKind(kind))

	response := gin.H{
		"ok":                  true,
		"kind":                kind,
		"job_id":              result["job_id"],
		"job_run_id":          result["job_run_id"],
		"correlation_id":      result["correlation_id"],
		"job_type":            result["job_type"],
		"status":              result["status"],
		"dispatch_status":     result["dispatch_status"],
		"scene_count":         result["scene_count"],
		"voiceover_count":     result["voiceover_count"],
		"video_mode":          payload.FirstString(canonicalPayload, "video_mode"),
		"video_name":          canonicalPayload["video_name"],
		"output_path":         canonicalPayload["output_path"],
		"drive_output_folder": canonicalPayload["drive_output_folder"],
		"enqueue":             result,
	}

	if clips := payload.NormalizeToStrings(canonicalPayload["clips"]); len(clips) > 0 {
		response["clips"] = clips
		response["clip_count"] = len(clips)
	}

	c.JSON(http.StatusOK, response)
}

// scriptIntakeSourceForKind maps a script-ingress kind to its bounded
// intake-source label. The fixed "generate" kind is the unified
// /api/v1/script/generate surface; every other kind is the dynamic
// /api/v1/script/jobs/:kind surface.
func scriptIntakeSourceForKind(kind string) string {
	if strings.TrimSpace(kind) == "generate" {
		return creatorflow.IntakeSourceScriptGenerate
	}
	return creatorflow.IntakeSourceScriptKind
}

func applyDefinition(payloadMap map[string]any, def ingress.Definition) {
	if payloadMap == nil {
		return
	}
	if strings.TrimSpace(def.ExecutorID) != "" {
		payloadMap[routing.KeyExecutorID] = strings.TrimSpace(def.ExecutorID)
	}
	if def.ExecutorVersion > 0 {
		payloadMap[routing.KeyExecutorVersion] = def.ExecutorVersion
	}
	if strings.TrimSpace(def.PipelineID) != "" {
		payloadMap[routing.KeyPipelineID] = strings.TrimSpace(def.PipelineID)
	}
}
