package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
	"velox-server/internal/app"
	"velox-server/internal/config"
	remoteansible "velox-server/internal/handlers/remote/ansible"
	workersuploads "velox-server/internal/handlers/remote/workers/uploads"
	integrationsDrive "velox-server/internal/integrations/drive"
	"velox-server/internal/handlers/server/analytics"
	"velox-server/internal/handlers/server/api"
	"velox-server/internal/handlers/server/groups"
	"velox-server/internal/handlers/server/darkeditor"
	jobhandlers "velox-server/internal/handlers/server/jobs"
	pipelinehandler "velox-server/internal/handlers/server/pipeline"
	scripthandlers "velox-server/internal/handlers/server/script"
	ytservice "velox-server/internal/integrations/youtube"
	jobservice "velox-server/internal/services/jobs"
	"velox-server/internal/queue"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if strings.HasPrefix(origin, "http://localhost:3000") ||
			strings.HasPrefix(origin, "http://127.0.0.1:3000") ||
			strings.HasPrefix(origin, "http://localhost:3001") ||
			strings.HasPrefix(origin, "http://127.0.0.1:3001") {
			c.Header("Access-Control-Allow-Origin", origin)
			c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, PATCH")
			c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With, Accept, Origin")
			c.Header("Access-Control-Allow-Credentials", "true")
			c.Header("Access-Control-Max-Age", "86400")
		}
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	}
}

func newRouter(cfg *config.Config, deps *serverDeps, registry *app.Registry) *gin.Engine {
	var r *gin.Engine
	if os.Getenv("GIN_MODE") == "release" {
		gin.SetMode(gin.ReleaseMode)
		r = gin.New()
		r.Use(gin.Recovery())
	} else {
		r = gin.Default()
	}

	configureTrustedProxies(r)

	// Dark editor API rewrite middleware
	r.Use(func(c *gin.Context) {
		if strings.HasPrefix(c.Request.URL.Path, "/dark_editor_v2/api/v1/") && !strings.HasPrefix(c.Request.URL.Path, "/dark_editor_v2/api/v1/youtube") {
			c.Request.URL.Path = strings.Replace(c.Request.URL.Path, "/dark_editor_v2/api/v1/", "/api/v1/", 1)
		}
		c.Next()
	})

	r.Use(corsMiddleware())
	r.Use(requestIDMiddleware())
	r.Use(accessLogMiddleware())
	r.Use(addGzipHeaders())

	// ── Module routes (health, workers, youtube, drive, ansible, frontend) ──
	registry.RegisterRoutes(r)

	// Get ansible handlers from the module
	var ansibleHandlers *remoteansible.AnsibleHandlers
	if deps.ansibleModule != nil {
		ansibleHandlers = deps.ansibleModule.Handlers()
	}

	// ── Remaining routes not yet in modules ──────────────────────────────────
	registerAPIV1Routes(r, cfg, deps, ansibleHandlers)
	registerScriptRoutes(r, cfg, deps)
	registerPipelineRoutes(r, cfg, deps)

	// Initialize groups handlers with SQLite store
	groups.InitGroupsStore(deps.sqliteStore)

	// Analytics cache
	analytics.InitAnalyticsCache(deps.paths.dataDir, deps.sqliteStore)

	// Dark Editor API routes
	deCfg := &darkeditor.Config{
		TempDir:      filepath.Join(cfg.Runtime.DataDir, "dark_editor", "temp"),
		ProjectsDir:  filepath.Join(cfg.Runtime.DataDir, "dark_editor", "projects"),
		LogDir:       filepath.Join(cfg.Runtime.DataDir, "dark_editor", "logs"),
		NVIDIAAPIKey: cfg.NVIDIA.APIKey,
	}
	deHandler := darkeditor.NewHandler(deCfg)
	deHandler.SetDBStore(deps.sqliteStore)
	darkeditor.RegisterAPIRoutes(r, deHandler)

	return r
}

	jobRepo := store.NewSQLiteJobsRepository(deps.sqliteStore)
	tokenMgr := deps.workerLifecycle.GetTokenManager()
	jobSvc := jobservice.NewService(cfg, deps.fileQ, jobRepo, nil, deps.reg)
	if deps.workerUpdateHandler != nil {
		if hash := deps.workerUpdateHandler.ComputeBundleSHA256(); hash != "" {
			jobSvc.SetMasterBundleHash(hash)
		}
	}
	// (Placeholder for additional wiring — kept for compatibility.)
	jobAPI := jobhandlers.NewJobAPI(cfg, deps.fileQ, tokenMgr, jobSvc, deps.sqliteStore)
	jobSubmitHandler := jobhandlers.NewJobSubmissionHandler(cfg, deps.fileQ)
	var youtubeService *ytservice.Service
	if deps.youtubeModule != nil {
		youtubeService = deps.youtubeModule.Service()
	}
	var driveService *integrationsDrive.Service
	if deps.driveModule != nil {
		driveService = deps.driveModule.Service()
	}
	api.RegisterV1Routes(r, cfg, deps.fileQ, deps.reg, jobAPI, jobSubmitHandler, deps.workersRepo, deps.sqliteStore, deps.workerUpdateHandler, youtubeService, driveService, ansibleHandlers)

	// V2 job routes
	v2JobsGroup := r.Group("/api/v1")
	jobhandlers.RegisterV2JobRoutes(v2JobsGroup, cfg, deps.fileQ, deps.sqliteStore, jobSvc)

	// Orchestrator multi-step pipeline routes (PR 9 cutover: backed by
	// workflow.Repository rather than the legacy *queue.Orchestrator).
	orchAdmin := r.Group("/api/v1")
	orchAdmin.Use(api.AdminAuthMiddleware(cfg))
	registerOrchestratorRoutes(orchAdmin, deps.workflowRepo)

	// PR2b/PR4d: upload-completed now uses BlobStore + ArtifactFinalizationService
	// instead of the old direct-save + maybeAutoUpload pattern.
	// The blobStore handles staging → verification → final promotion.
	r.POST("/api/v1/video/upload-completed", workersuploads.UploadCompletedVideo(cfg, deps.fileQ, deps.blobStore))

	// Chunked upload routes (resumable worker→master video upload)
	r.POST("/api/v1/video/chunked/init", workersuploads.InitChunkedUpload())
	r.POST("/api/v1/video/chunked/:job_id/:chunk_index", workersuploads.UploadChunk(cfg))
	r.POST("/api/v1/video/chunked/:job_id/complete", workersuploads.CompleteChunkedUpload(cfg, deps.fileQ))

	// Bundle compat routes
	if deps.workerUpdateHandler != nil {
		r.GET("/api/bundle/info", deps.workerUpdateHandler.GetLatestBundleHandler())
		r.GET("/api/bundle/files", deps.workerUpdateHandler.GetBundleFilesHandler())
	}
}

func registerScriptRoutes(r *gin.Engine, cfg *config.Config, deps *serverDeps) {
	if deps == nil || deps.fileQ == nil {
		return
	}
	rootGroup := r.Group("/api/script")
	rootGroup.Use(api.AdminAuthMiddleware(cfg))
	scripthandlers.RegisterRoutes(rootGroup, cfg, deps.fileQ, deps.sqliteStore)

	v1Group := r.Group("/api/v1/script")
	v1Group.Use(api.AdminAuthMiddleware(cfg))
	scripthandlers.RegisterRoutes(v1Group, cfg, deps.fileQ, deps.sqliteStore)
}

func registerOrchestratorRoutes(v1Admin gin.IRoutes, repo workflow.Repository) {
	if repo == nil {
		return
	}

	v1Admin.POST("/orchestrator/jobs", func(c *gin.Context) {
		var req struct {
			JobID        string                   `json:"job_id"`
			PipelineType string                   `json:"pipeline_type"`
			Steps        []map[string]interface{} `json:"steps"`
			Metadata     map[string]interface{}   `json:"metadata,omitempty"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": "invalid JSON: " + err.Error()})
			return
		}
		if req.JobID == "" || len(req.Steps) == 0 {
			c.JSON(400, gin.H{"error": "job_id and steps required"})
			return
		}

		spec := workflow.WorkflowSpec{
			RunID:        req.JobID,
			WorkflowType: req.PipelineType,
			Input:        req.Metadata,
			Steps:        make([]workflow.WorkflowStepSpec, len(req.Steps)),
		}
		for i, s := range req.Steps {
			stepKey, _ := s["step_id"].(string)
			if stepKey == "" {
				stepKey = fmt.Sprintf("step-%d", i)
			}
			stepName, _ := s["step_name"].(string)
			if stepName == "" {
				stepName = stepKey
			}
			jobType, _ := s["job_type"].(string)
			payload, _ := s["payload"].(map[string]interface{})
			depsRaw, _ := s["dependencies"].([]interface{})
			var deps []string
			for _, d := range depsRaw {
				if ds, ok := d.(string); ok {
					deps = append(deps, ds)
				}
			}
			maxRetries := 2
			if mr, ok := s["max_retries"].(float64); ok {
				maxRetries = int(mr)
			}

			jobTypeBuf := jobType
			stepKeyVal := stepName // step_key defaults to step_name for stability
			_ = stepKey
			_ = jobTypeBuf
			spec.Steps[i] = workflow.WorkflowStepSpec{
				StepKey:       stepKeyVal,
				JobType:       jobType,
				Input:         payload,
				DependsOnKeys: deps,
				MaxAttempts:   maxRetries,
			}
		}

		run, err := repo.CreateRun(c.Request.Context(), spec)
		if err != nil {
			c.JSON(409, gin.H{"error": err.Error()})
			return
		}

		c.JSON(201, gin.H{
			"job_id":         run.RunID,
			"workflow_type":  run.WorkflowType,
			"total_steps":    len(run.Input),
			"steps_count":    len(spec.Steps),
			"status":         string(run.Status),
			"created_at":     run.CreatedAt,
		})
	})

	v1Admin.GET("/orchestrator/jobs/:id", func(c *gin.Context) {
		run, err := repo.GetRun(c.Request.Context(), c.Param("id"))
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		if run == nil {
			c.JSON(404, gin.H{"error": "workflow run not found"})
			return
		}
		steps, err := repo.ListSteps(c.Request.Context(), run.RunID)
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		c.JSON(200, gin.H{"run": run, "steps": steps})
	})

	v1Admin.GET("/orchestrator/jobs", func(c *gin.Context) {
		runs, err := repo.ListRuns(c.Request.Context(), 100)
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		c.JSON(200, gin.H{"jobs": runs, "total": len(runs)})
	})

	v1Admin.GET("/orchestrator/stats", func(c *gin.Context) {
		stats, err := repo.Stats(c.Request.Context())
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		c.JSON(200, stats)
	})
}

func registerPipelineRoutes(r *gin.Engine, cfg *config.Config, deps *serverDeps) {
	r.POST("/api/remote/pipeline/generate", pipelinehandler.PipelineGenerate(cfg, deps.fileQ))
	r.GET("/api/remote/pipeline/status/:trace_id", pipelinehandler.PipelineStatus(cfg))

	var cmdMgr *workersreg.CommandManager
	if deps.workerUpdateHandler != nil {
		cmdMgr = deps.workerUpdateHandler.CommandManager()
	}
	r.DELETE("/api/remote/pipeline/cancel/:trace_id", pipelinehandler.PipelineCancel(cfg, deps.fileQ, cmdMgr))

	r.POST("/api/script-simple", pipelinehandler.ScriptSimple(cfg))
	r.POST("/api/script-multiple", pipelinehandler.ScriptMultiple(cfg))
}
