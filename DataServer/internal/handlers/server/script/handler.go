package script

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	voiceoverassets "velox-server/internal/assets"
	"velox-server/internal/config"
	"velox-server/internal/costmodel"
	"velox-server/internal/creatorflow"
	remoteansible "velox-server/internal/handlers/remote/ansible"
	jobshandler "velox-server/internal/handlers/server/jobs"
	driveintegration "velox-server/internal/integrations/drive"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/jobs/ingress"
	"velox-server/internal/store"
	"velox-server/internal/translation"
)

const scriptSceneMode = "scene_image"

// errScriptHandlerNotConfigured is returned by loadJob when the handler's
// SQLiteStore dependency was never wired up. It is a distinct sentinel so
// operators can tell handler-misconfiguration apart from real DB failures.
var errScriptHandlerNotConfigured = errors.New("script handler sqliteDB not configured")

type GoogleDocCreator interface {
	CreateGoogleDoc(context.Context, string, string, string, string) (*driveintegration.UploadResult, error)
}

// ScriptHandlers exposes the script-with-images workflow.
//
// PR15.7a: the *enqueue.Enqueuer replaces both the package-level voiceover
// global and the legacy free-function EnqueueSceneVideoJob. Constructed
// once at composition-root (cmd/server/bootstrap) and threaded through.
type ScriptHandlers struct {
	enqueuer   *enqueue.Enqueuer
	sqliteDB   *store.SQLiteStore
	dataDir    string
	creator    *creatorflow.Service
	docCreator GoogleDocCreator
}

func NewScriptHandlers(cfg *config.Config, sqliteDB *store.SQLiteStore, enqueuer *enqueue.Enqueuer) *ScriptHandlers {
	dataDir := ""
	if cfg != nil {
		dataDir = strings.TrimSpace(cfg.Runtime.DataDir)
	}
	return &ScriptHandlers{
		enqueuer: enqueuer,
		sqliteDB: sqliteDB,
		dataDir:  dataDir,
		// creatorflow.New takes only (cfg, enqueuer) post-PR15.7a:
		// the Enqueuer owns the queue so passing q again would be redundant
		// and risks drift between two parallel references.
		creator: creatorflow.New(cfg, enqueuer, sqliteDB),
	}
}

// RegisterRoutes wires the public script routes on the given group.
//
// PR15.7a: a *enqueue.Enqueuer is now mandatory alongside sqliteDB.
func RegisterRoutes(group gin.IRoutes, cfg *config.Config, sqliteDB *store.SQLiteStore, enqueuer *enqueue.Enqueuer, docCreators ...GoogleDocCreator) *ScriptHandlers {
	handlers := NewScriptHandlers(cfg, sqliteDB, enqueuer)
	if len(docCreators) > 0 {
		handlers.docCreator = docCreators[0]
	}
	registry := newScriptIngressRegistry(cfg, handlers.dataDir, handlers.docCreator)
	ingressHandler := jobshandler.NewHandler(registry, enqueuer)
	group.POST("/generate-with-images", handlers.GenerateWithImagesHandler(cfg))
	group.POST("/generate", ingressHandler.SubmitFixed("generate"))
	group.POST("/jobs/:kind", ingressHandler.Submit())
	group.GET("/jobs/:job_id", handlers.ScriptJobHandler(false))
	group.GET("/jobs/:job_id/full", handlers.ScriptJobHandler(true))
	group.GET("/:script_id", handlers.ScriptByIDHandler())
	return handlers
}

func newScriptIngressRegistry(cfg *config.Config, dataDir string, docCreator GoogleDocCreator) *ingress.Registry {
	registry := ingress.NewRegistry()
	registry.MustRegister(ingress.Definition{
		Kind:            "generate",
		ExecutorID:      "scene.composite.v1",
		ExecutorVersion: 1,
		PipelineID:      "hybrid.v1",
		Builder: func(ctx context.Context, raw map[string]any) (map[string]any, error) {
			normalized, err := buildUnifiedGeneratePayload(raw, dataDir, cfg.Runtime.VideosDir)
			if err != nil {
				return nil, err
			}
			translated, err := translation.TranslateScenes(ctx, normalized, translation.Client{
				BaseURL: cfg.Pipeline.OllamaURL,
				Model:   cfg.Pipeline.OllamaModel,
			})
			if err != nil {
				return nil, err
			}
			if folder := strings.TrimSpace(firstStringValue(translated, "drive_output_folder", "output_directory")); folder != "" {
				if docCreator == nil {
					return nil, fmt.Errorf("google doc requested by drive_output_folder but Drive is not configured")
				}
				content, err := translation.RenderGoogleDocContent(translated)
				if err != nil {
					return nil, err
				}
				title := firstStringValue(translated, "video_name", "title", "topic")
				doc, err := docCreator.CreateGoogleDoc(ctx, title, content, enqueue.ResolveDriveOutputFolderReference(dataDir, folder), firstStringValue(translated, "correlation_id"))
				if err != nil {
					return nil, fmt.Errorf("create script google doc: %w", err)
				}
				metadata := map[string]interface{}{}
				if existing, ok := translated["video_metadata"].(map[string]interface{}); ok {
					for key, value := range existing {
						metadata[key] = value
					}
				}
				metadata["google_doc"] = map[string]interface{}{
					"id":    doc.FileID,
					"link":  doc.WebViewLink,
					"title": title,
				}
				translated["video_metadata"] = metadata
			}
			return enqueue.BuildClipPayloadForMaster(translated, dataDir, cfg.Runtime.VideosDir, "")
		},
		Requirements: costmodel.DefaultRequirements(),
	})
	registry.MustRegister(ingress.Definition{
		Kind:            "slideshow-video",
		ExecutorID:      "scene.composite.v1",
		ExecutorVersion: 1,
		PipelineID:      "images.v1",
		Builder: func(ctx context.Context, raw map[string]any) (map[string]any, error) {
			return enqueue.BuildSlideshowPayloadForMaster(raw, dataDir, cfg.Runtime.VideosDir, "")
		},
		Requirements: costmodel.DefaultRequirements(),
	})
	return registry
}

// buildUnifiedGeneratePayload is the single public POST /script/generate
// dispatcher. source.type selects the canonical input normalizer without
// exposing a separate endpoint for each source family.
func buildUnifiedGeneratePayload(raw map[string]any, dataDir, videosDir string) (map[string]any, error) {
	if raw == nil {
		return nil, fmt.Errorf("request body is required")
	}

	sourceValue, ok := raw["source"]
	if !ok {
		return nil, fmt.Errorf("source is required")
	}
	source, ok := sourceValue.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("source must be an object")
	}
	sourceType, _ := source["type"].(string)
	sourceType = strings.ToLower(strings.TrimSpace(sourceType))
	if sourceType == "" {
		return nil, fmt.Errorf("source.type is required")
	}

	switch sourceType {
	case "clips":
		// Accept both the canonical nested source payload and the current
		// render-ready top-level fields during the contract cutover. Top-level
		// values win so a caller cannot silently override explicit request data.
		merged := make(map[string]any, len(raw)+len(source))
		for key, value := range raw {
			merged[key] = value
		}
		for key, value := range source {
			if key == "type" {
				continue
			}
			if _, exists := merged[key]; !exists {
				merged[key] = value
			}
		}

		normalized, err := enqueue.BuildClipPayloadForMaster(merged, dataDir, videosDir, "")
		if err != nil {
			return nil, err
		}
		sourceCopy := make(map[string]any, len(source))
		for key, value := range source {
			sourceCopy[key] = value
		}
		normalized["source"] = sourceCopy
		return normalized, nil
	default:
		return nil, fmt.Errorf("unsupported source.type %q", sourceType)
	}
}

// GenerateWithImagesHandler accepts a job payload built from scenes or images,
// then enqueues a process_video job for the remote worker.
func (h *ScriptHandlers) GenerateWithImagesHandler(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		if h.enqueuer == nil {
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

		// MasterURL resolution is mandatory in production. The shell-out
		// `hostname -I` discovery was removed in Blocco 4 step #3: the
		// creatorflow domain no longer shells out. Production deployments
		// set cfg.Workers.MasterURL or VELOX_MASTER_URL via the
		// remoteansible.ResolveMasterURL helper. Local dev/test fall back
		// to an explicit localhost so the script handler remains
		// self-contained on a developer's laptop.
		resolvedMasterURL := remoteansible.ResolveMasterURL(cfg, c, "").URL
		if resolvedMasterURL == "" || remoteansible.IsLocalhostURL(resolvedMasterURL) {
			resolvedMasterURL = "http://127.0.0.1:8000"
		}
		if h.creator != nil && !shouldBypassCreator(payload) {
			if creatorResponse, used, err := h.creator.StartOrPersistForwarding(c.Request.Context(), payload); err != nil {
				if assetErr, ok := voiceoverassets.AsAcquisitionError(err); ok {
					c.JSON(http.StatusUnprocessableEntity, gin.H{
						"ok":          false,
						"code":        assetErr.Code,
						"field":       assetErr.Field,
						"message":     assetErr.Message,
						"source_type": assetErr.SourceType,
					})
					return
				}
				log.Printf("[SCRIPT] creator stage failed, falling back to local enqueue: %v", err)
			} else if used {
				c.JSON(http.StatusOK, creatorResponse)
				return
			}
		}

		normalized, err := enqueue.BuildSceneImagePayloadForMaster(payload, h.dataDir, cfg.Runtime.VideosDir, resolvedMasterURL)
		if err != nil {
			if assetErr, ok := voiceoverassets.AsAcquisitionError(err); ok {
				c.JSON(http.StatusUnprocessableEntity, gin.H{
					"ok":          false,
					"code":        assetErr.Code,
					"field":       assetErr.Field,
					"message":     assetErr.Message,
					"source_type": assetErr.SourceType,
				})
				return
			}
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
			return
		}

		response, err := h.enqueuer.Enqueue(c.Request.Context(), normalized, costmodel.DefaultRequirements())
		if err != nil {
			if assetErr, ok := voiceoverassets.AsAcquisitionError(err); ok {
				c.JSON(http.StatusUnprocessableEntity, gin.H{
					"ok":          false,
					"code":        assetErr.Code,
					"field":       assetErr.Field,
					"message":     assetErr.Message,
					"source_type": assetErr.SourceType,
				})
				return
			}
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

// detectPublicMasterURL was removed in Blocco 4 step #3: hostname
// discovery (`hostname -I`) was a creatorflow-domain shim. Operators
// set cfg.Workers.MasterURL or VELOX_MASTER_URL in production; dev/test
// paths use the explicit `http://127.0.0.1:8000` fallback in
// GenerateWithImagesHandler.

func shouldBypassCreator(payload map[string]interface{}) bool {
	if payload == nil {
		return false
	}
	if isTruthyFlag(payload, "skip_creator", "bypass_creator", "disable_creator", "use_creator") {
		return true
	}
	hasScenes := false
	if raw := strings.TrimSpace(firstStringValue(payload, "scenes_json")); raw != "" {
		hasScenes = true
	}
	hasVoiceover := false
	if raw := strings.TrimSpace(firstStringValue(payload, "voiceover_path", "audio_path")); raw != "" {
		hasVoiceover = true
	}
	if !hasVoiceover {
		switch v := payload["voiceover_paths"].(type) {
		case []string:
			hasVoiceover = len(v) > 0
		case []interface{}:
			hasVoiceover = len(v) > 0
		}
	}
	hasScript := strings.TrimSpace(firstStringValue(payload, "script_text", "script")) != ""
	return hasScenes && hasVoiceover && hasScript
}

func isTruthyFlag(payload map[string]interface{}, keys ...string) bool {
	for _, key := range keys {
		value, ok := payload[key]
		if !ok {
			continue
		}
		switch v := value.(type) {
		case bool:
			if key == "use_creator" {
				return !v
			}
			return v
		case string:
			trimmed := strings.ToLower(strings.TrimSpace(v))
			if trimmed == "" {
				continue
			}
			if key == "use_creator" {
				return trimmed == "false" || trimmed == "0" || trimmed == "no"
			}
			return trimmed == "true" || trimmed == "1" || trimmed == "yes"
		}
	}
	return false
}

func firstStringValue(payload map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if raw, ok := payload[key]; ok {
			if value, ok := raw.(string); ok {
				return value
			}
		}
	}
	return ""
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
			if errors.Is(err, sql.ErrNoRows) {
				c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "job not found"})
				return
			}
			log.Printf("[SCRIPT] loadJob failed for job %s: %v", jobID, err)
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "failed to load job"})
			return
		}
		c.JSON(http.StatusOK, enqueue.RenderHTTPBoundaryJobResponse(job, full))
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
			if errors.Is(err, sql.ErrNoRows) {
				c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "script not found"})
				return
			}
			log.Printf("[SCRIPT] loadJob failed for script %s: %v", scriptID, err)
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "failed to load script"})
			return
		}
		c.JSON(http.StatusOK, enqueue.RenderHTTPBoundaryJobResponse(job, true))
	}
}

func (h *ScriptHandlers) loadJob(ctx context.Context, jobID string) (map[string]interface{}, error) {
	if h.sqliteDB == nil {
		return nil, errScriptHandlerNotConfigured
	}
	job, err := h.sqliteDB.GetJob(ctx, jobID)
	if err != nil {
		return nil, err
	}
	if job == nil {
		return nil, sql.ErrNoRows
	}
	return job, nil
}
