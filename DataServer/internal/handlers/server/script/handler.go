package script

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	voiceoverassets "velox-server/internal/assets"
	"velox-server/internal/config"
	"velox-server/internal/costmodel"
	"velox-server/internal/creatorflow"
	jobshandler "velox-server/internal/handlers/server/jobs"
	driveintegration "velox-server/internal/integrations/drive"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/jobs/ingress"
	velmetrics "velox-server/internal/metrics"
	"velox-server/internal/store"
	"velox-shared/contract/domain"
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
	submission *creatorflow.CanonicalJobSubmitter
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
		creator: creatorflow.New(cfg, enqueuer, sqliteDB.Forwarding(), sqliteDB),
		// The from-scratch enqueue path routes through the canonical submitter
		// (SubmitScratch) so intake-source telemetry is recorded once by the
		// submitter instead of as a handler side-effect. RegisterRoutes
		// overrides this with a resolver built from the composition-root
		// resolver when one is supplied.
		submission: creatorflow.NewCanonicalJobSubmitter(creatorflow.NewResolverMinimal(enqueuer, sqliteDB.Forwarding())),
	}
}

// RegisterRoutes wires the public script routes on the given group.
//
// PR15.7a: a *enqueue.Enqueuer is now mandatory alongside sqliteDB.
func RegisterRoutes(group gin.IRoutes, cfg *config.Config, sqliteDB *store.SQLiteStore, enqueuer *enqueue.Enqueuer, resolver *creatorflow.Resolver, docCreators ...GoogleDocCreator) *ScriptHandlers {
	handlers := NewScriptHandlers(cfg, sqliteDB, enqueuer)
	if len(docCreators) > 0 {
		handlers.docCreator = docCreators[0]
	}
	// A nil resolver falls back to a minimal resolver built from the same
	// enqueuer + store so test mounts and partial wiring still drive the
	// from-scratch SubmitScratch path (the forwarding path is unused here).
	if resolver == nil {
		resolver = creatorflow.NewResolverMinimal(enqueuer, sqliteDB.Forwarding())
	}
	registry := newScriptIngressRegistry(cfg, handlers.dataDir, handlers.sqliteDB, handlers.docCreator)
	submission := creatorflow.NewCanonicalJobSubmitter(resolver)
	handlers.submission = submission
	ingressHandler := jobshandler.NewHandler(registry, submission)
	group.POST("/generate-with-images", handlers.GenerateWithImagesHandler(cfg))
	group.POST("/jobs/:kind", ingressHandler.Submit())
	group.GET("/jobs/:job_id", handlers.ScriptJobHandler(false))
	group.GET("/jobs/:job_id/full", handlers.ScriptJobHandler(true))
	group.GET("/:script_id", handlers.ScriptByIDHandler())
	return handlers
}

func newScriptIngressRegistry(cfg *config.Config, dataDir string, sqliteDB *store.SQLiteStore, docCreator GoogleDocCreator) *ingress.Registry {
	var resolver enqueue.DriveFolderResolver
	if sqliteDB != nil {
		resolver = sqliteDB
	}
	registry := ingress.NewRegistry()
	registry.MustRegister(ingress.Definition{
		Kind:            "slideshow-video",
		ExecutorID:      "scene.composite.v1",
		ExecutorVersion: 1,
		PipelineID:      "images.v1",
		Builder: func(ctx context.Context, raw map[string]any) (map[string]any, error) {
			return enqueue.BuildSlideshowPayloadForMaster(raw, dataDir, cfg.Runtime.VideosDir, "", resolver)
		},
		Requirements: costmodel.DefaultRequirements(),
	})
	return registry
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

		// The endpoint was validated and frozen at bootstrap. Never derive
		// it from request headers, hostname discovery, or another URL.
		resolvedMasterURL := ""
		if cfg != nil {
			resolvedMasterURL = string(cfg.ControlPlane.RESTPublic)
		}
		if h.creator != nil && !shouldBypassCreator(payload) {
			if creatorResponse, used, err := h.creator.StartOrPersistForwarding(c.Request.Context(), payload); err != nil {
				if assetErr, ok := voiceoverassets.AsAcquisitionError(err); ok {
					c.JSON(http.StatusUnprocessableEntity, gin.H{
						"ok":          false,
						"code":        assetErr.Code,
						"error_code":  assetErr.ErrorCode,
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

		// The clip/stock intake (video_mode=clip_stock) fed the retired
		// hybrid.v1 path; only the scene-image builder remains.
		normalized, err := enqueue.BuildSceneImagePayloadForMaster(payload, h.dataDir, cfg.Runtime.VideosDir, resolvedMasterURL, h.sqliteDB)
		if err != nil {
			if assetErr, ok := voiceoverassets.AsAcquisitionError(err); ok {
				c.JSON(http.StatusUnprocessableEntity, gin.H{
					"ok":          false,
					"code":        assetErr.Code,
					"error_code":  assetErr.ErrorCode,
					"field":       assetErr.Field,
					"message":     assetErr.Message,
					"source_type": assetErr.SourceType,
				})
				return
			}
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
			return
		}

		// Route the from-scratch enqueue through the canonical submitter so
		// intake-source telemetry is recorded once by the submitter (not as a
		// handler side-effect). A nil submitter (partial test wiring) falls
		// back to the direct enqueuer and records the source explicitly.
		var response map[string]interface{}
		if h.submission != nil {
			response, err = h.submission.SubmitScratch(c.Request.Context(), creatorflow.CanonicalJobSubmission{
				IntakeSource: creatorflow.IntakeSourceScriptGenerate,
				Payload:      normalized,
			}, costmodel.DefaultRequirements())
		} else {
			response, err = h.enqueuer.Enqueue(c.Request.Context(), normalized, costmodel.DefaultRequirements())
			if err == nil {
				velmetrics.RecordIntakeSource(creatorflow.IntakeSourceScriptGenerate)
			}
		}
		if err != nil {
			if assetErr, ok := voiceoverassets.AsAcquisitionError(err); ok {
				c.JSON(http.StatusUnprocessableEntity, gin.H{
					"ok":          false,
					"code":        assetErr.Code,
					"error_code":  assetErr.ErrorCode,
					"field":       assetErr.Field,
					"message":     assetErr.Message,
					"source_type": assetErr.SourceType,
				})
				return
			}
			// Single typed mapper: a DomainError carries the canonical HTTP
			// status (validation rejections surface as 422 with the typed
			// field path); untyped failures stay 500. The wire envelope keeps
			// the historical "error" field as the human-readable message —
			// error text is never inspected for classification.
			status := http.StatusInternalServerError
			if derr, ok := domain.AsDomainError(err); ok {
				status = derr.HTTPCode()
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
			"video_mode":          firstStringValue(normalized, "video_mode"),
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

// The master URL is exclusively supplied by Config.ControlPlane.RESTPublic.

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
		response, resolveErr := enqueue.RenderHTTPBoundaryJobResponse(c.Request.Context(), job, full, h.sqliteDB)
		if resolveErr != nil {
			log.Printf("[SCRIPT] render job response failed for job %s: %v", jobID, resolveErr)
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "failed to resolve job response"})
			return
		}
		c.JSON(http.StatusOK, response)
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
		response, resolveErr := enqueue.RenderHTTPBoundaryJobResponse(c.Request.Context(), job, true, h.sqliteDB)
		if resolveErr != nil {
			log.Printf("[SCRIPT] render script response failed for script %s: %v", scriptID, resolveErr)
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "failed to resolve script response"})
			return
		}
		c.JSON(http.StatusOK, response)
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
