package script

import (
	"context"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"velox-server/internal/config"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/queue"
	"velox-server/internal/store"
)

const scriptSceneMode = "scene_image"

// ScriptHandlers exposes the script-with-images workflow.
type ScriptHandlers struct {
	queue    *queue.FileQueue
	sqliteDB *store.SQLiteStore
	dataDir  string
}

func NewScriptHandlers(cfg *config.Config, q *queue.FileQueue, sqliteDB *store.SQLiteStore) *ScriptHandlers {
	dataDir := ""
	if cfg != nil {
		dataDir = strings.TrimSpace(cfg.DataDir)
	}
	return &ScriptHandlers{
		queue:    q,
		sqliteDB: sqliteDB,
		dataDir:  dataDir,
	}
}

// RegisterRoutes wires the public script routes on the given group.
func RegisterRoutes(group gin.IRoutes, cfg *config.Config, q *queue.FileQueue, sqliteDB *store.SQLiteStore) *ScriptHandlers {
	handlers := NewScriptHandlers(cfg, q, sqliteDB)
	group.POST("/generate-with-images", handlers.GenerateWithImagesHandler(cfg))
	group.GET("/jobs/:job_id", handlers.ScriptJobHandler(false))
	group.GET("/jobs/:job_id/full", handlers.ScriptJobHandler(true))
	group.GET("/:script_id", handlers.ScriptByIDHandler())
	return handlers
}

// GenerateWithImagesHandler accepts a job payload built from scenes or images,
// then enqueues a process_video job for the remote worker.
func (h *ScriptHandlers) GenerateWithImagesHandler(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		if h.queue == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": "queue unavailable"})
			return
		}

		var payload map[string]interface{}
		if err := c.ShouldBindJSON(&payload); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid JSON body"})
			return
		}
		if payload == nil {
			payload = map[string]interface{}{}
		}

		normalized, err := enqueue.BuildSceneImagePayload(payload, h.dataDir, cfg.VideosDir)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
			return
		}

		response, err := enqueue.EnqueueSceneVideoJob(c.Request.Context(), h.queue, normalized)
		if err != nil {
			status := http.StatusInternalServerError
			if strings.Contains(strings.ToLower(err.Error()), "queue unavailable") {
				status = http.StatusServiceUnavailable
			}
			c.JSON(status, gin.H{"ok": false, "error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"ok":                  true,
			"job_id":              response["job_id"],
			"job_run_id":          response["job_run_id"],
			"correlation_id":      response["correlation_id"],
			"job_type":            response["job_type"],
			"status":              response["status"],
			"video_mode":          scriptSceneMode,
			"video_name":          normalized["video_name"],
			"output_path":         normalized["output_path"],
			"drive_output_folder": normalized["drive_output_folder"],
			"scene_count":         response["scene_count"],
			"voiceover_count":     response["voiceover_count"],
			"scene_image_paths":   normalized["scene_image_paths"],
			"enqueue":             response,
		})
	}
}

func (h *ScriptHandlers) ScriptJobHandler(full bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := strings.TrimSpace(c.Param("job_id"))
		if jobID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "job_id required"})
			return
		}
		job, err := h.loadJob(c.Request.Context(), jobID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
			return
		}
		if job == nil {
			c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "job not found"})
			return
		}
		c.JSON(http.StatusOK, enqueue.RenderJobResponse(job, full))
	}
}

func (h *ScriptHandlers) ScriptByIDHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		scriptID := strings.TrimSpace(c.Param("script_id"))
		if scriptID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "script_id required"})
			return
		}
		job, err := h.loadJob(c.Request.Context(), scriptID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
			return
		}
		if job == nil {
			c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "script not found"})
			return
		}
		c.JSON(http.StatusOK, enqueue.RenderJobResponse(job, true))
	}
}

func (h *ScriptHandlers) loadJob(ctx context.Context, jobID string) (map[string]interface{}, error) {
	if h.sqliteDB == nil {
		return nil, nil
	}
	job, err := h.sqliteDB.GetJob(ctx, jobID)
	if err != nil {
		return nil, nil
	}
	return job, nil
}
