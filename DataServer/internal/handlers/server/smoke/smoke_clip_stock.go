package smoke

import (
	"encoding/json"
	"net/http"
	"time"

	"velox-shared/payload"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"velox-server/internal/config"
	"velox-server/internal/jobs"
	"velox-server/internal/store"
	"velox-server/internal/taskgraph"
)

// CreateSmokeClipStock accepts POST /api/v1/video/smoke-clip-stock and enqueues
// a minimal process_video job for the clip+stock pipeline.
func CreateSmokeClipStock(cfg *config.Config, atomic *store.AtomicJobTaskCreator) gin.HandlerFunc {
	return func(c *gin.Context) {
		if atomic == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": "queue unavailable"})
			return
		}

		var body map[string]interface{}
		if err := c.ShouldBindJSON(&body); err != nil && err.Error() != "EOF" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid JSON"})
			return
		}
		if body == nil {
			body = map[string]interface{}{}
		}

		videoName := payload.FirstString(body, "video_name", "title", "project_name")
		if videoName == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "missing video_name"})
			return
		}
		scriptText := payload.FirstString(body, "script_text", "script")
		if scriptText == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "missing script_text"})
			return
		}
		videoMode := payload.FirstString(body, "video_mode")
		if videoMode == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "missing video_mode"})
			return
		}

		voiceoverPaths := payload.NormalizeToStrings(body["voiceover_paths"])
		if len(voiceoverPaths) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "missing voiceover_paths"})
			return
		}
		introClipPaths := payload.NormalizeToStrings(body["intro_clip_paths"])
		if len(introClipPaths) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "missing intro_clip_paths"})
			return
		}
		stockClipPaths := payload.NormalizeToStrings(body["stock_clip_paths"])
		if len(stockClipPaths) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "missing stock_clip_paths"})
			return
		}
		clipSegments := body["clip_segments"]

		jobID := payload.FirstString(body, "job_id", "id")
		if jobID == "" {
			jobID = "smoke_clip_stock_" + uuid.NewString()
		}
		jobRunID := payload.FirstString(body, "job_run_id", "run_id")
		if jobRunID == "" {
			jobRunID = "run_" + uuid.NewString()
		}
		correlationID := payload.FirstString(body, "correlation_id")
		if correlationID == "" {
			correlationID = "corr_" + uuid.NewString()
		}
		now := time.Now().UTC().Format(time.RFC3339)
		outputPath := payload.FirstString(body, "output_path", "output")
		if outputPath == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "missing output_path"})
			return
		}
		driveOutputFolder := payload.FirstString(body, "drive_output_folder", "output_directory")

		normalized := map[string]interface{}{
			"job_id":                 jobID,
			"id":                     jobID,
			"job_run_id":             jobRunID,
			"run_id":                 jobRunID,
			"correlation_id":         correlationID,
			"job_type":               "process_video",
			"version":                "v1",
			"created_at":             payload.EnsureRFC3339(payload.FirstString(body, "created_at"), now),
			"updated_at":             payload.EnsureRFC3339(payload.FirstString(body, "updated_at"), now),
			"video_name":             videoName,
			"title":                  videoName,
			"script_text":            scriptText,
			"video_mode":             videoMode,
			"voiceover_paths":        voiceoverPaths,
			"voiceover_path":         voiceoverPaths[0],
			"audio_path":             voiceoverPaths[0],
			"intro_clip_paths":       introClipPaths,
			"stock_clip_paths":       stockClipPaths,
			"clip_segments":          clipSegments,
			"scenes":                 []interface{}{},
			"scenes_json":            "[]",
			"output_path":            outputPath,
			"drive_output_folder":    driveOutputFolder,
			"audio_language_for_srt": payload.FirstString(body, "audio_language_for_srt", "audio_lang"),
			"priority":               payload.EnsureInt(body["priority"], 1),
			"timeout_secs":           payload.EnsureInt(body["timeout_secs"], 3600),
			"submitted_via":          "api_v1_smoke_clip_stock",
			"source":                 "smoke_clip_stock_api",
			"scene_count":            0,
			"voiceover_count":        len(voiceoverPaths),
		}

		normalized["parameters"] = map[string]interface{}{
			"version":                "v1",
			"job_id":                 jobID,
			"job_run_id":             jobRunID,
			"run_id":                 jobRunID,
			"correlation_id":         correlationID,
			"job_type":               "process_video",
			"video_name":             videoName,
			"script_text":            scriptText,
			"video_mode":             videoMode,
			"voiceover_paths":        voiceoverPaths,
			"voiceover_path":         voiceoverPaths[0],
			"audio_path":             voiceoverPaths[0],
			"intro_clip_paths":       introClipPaths,
			"stock_clip_paths":       stockClipPaths,
			"clip_segments":          clipSegments,
			"output_path":            outputPath,
			"drive_output_folder":    driveOutputFolder,
			"audio_language_for_srt": payload.FirstString(body, "audio_language_for_srt", "audio_lang"),
			"priority":               payload.EnsureInt(body["priority"], 1),
			"timeout_secs":           payload.EnsureInt(body["timeout_secs"], 3600),
			"submitted_via":          "api_v1_smoke_clip_stock",
			"source":                 "smoke_clip_stock_api",
		}

		raw, _ := json.Marshal(normalized)
		job := &jobs.Job{
			ID:         jobID,
			Status:     jobs.StatusPending,
			VideoName:  videoName,
			MaxRetries: 3,
			Payload:    string(raw),
		}
		spec := &taskgraph.TaskSpec{
			ExecutorID: "scene.composite.v1@1",
			Version:    taskgraph.SpecVersion,
		}
		if err := atomic.CreateJobWithTask(c.Request.Context(), job, spec, 1); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"ok":                  true,
			"job_id":              jobID,
			"job_run_id":          jobRunID,
			"correlation_id":      correlationID,
			"job_type":            "process_video",
			"video_mode":          videoMode,
			"output_path":         outputPath,
			"drive_output_folder": driveOutputFolder,
			"voiceover_paths":     voiceoverPaths,
			"intro_clip_paths":    introClipPaths,
			"stock_clip_paths":    stockClipPaths,
			"clip_segments":       clipSegments,
			"status":              "PENDING",
			"queue":               "queued_for_workers",
		})
	}
}
