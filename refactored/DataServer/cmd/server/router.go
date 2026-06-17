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
	integrationsDrive "velox-server/internal/integrations/drive" (fix: add missing jobs columns migration (023), fix CompleteJob CAS, patch UpdateJobFields whitelist)
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

	// Get ansible handlers from the module (created during RegisterRoutes)
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

func registerAPIV1Routes(r *gin.Engine, cfg *config.Config, deps *serverDeps, ansibleHandlers *remoteansible.AnsibleHandlers) {
	// TODO: migrate remaining V1 routes to dedicated api module
	jobRepo := store.NewSQLiteJobsRepository(deps.sqliteStore)
	tokenMgr := deps.workerLifecycle.GetTokenManager()
	jobSvc := jobservice.NewService(cfg, deps.fileQ, jobRepo, nil, deps.reg)
	if deps.workerUpdateHandler != nil {
		if hash := deps.workerUpdateHandler.ComputeBundleSHA256(); hash != "" {
			jobSvc.SetMasterBundleHash(hash)
		}
	}
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
	api.RegisterV1Routes(r, cfg, deps.fileQ, deps.reg, jobAPI, jobSubmitHandler, deps.workersRepo, deps.sqliteStore, deps.workerUpdateHandler, youtubeService, driveService, ansibleHandlers) (fix: add missing jobs columns migration (023), fix CompleteJob CAS, patch UpdateJobFields whitelist)

	// V2 job routes: /api/v1/jobs/{id}/lease|complete|fail|progress|attempts|artifacts|events
	v2JobsGroup := r.Group("/api/v1")
	jobhandlers.RegisterV2JobRoutes(v2JobsGroup, cfg, deps.fileQ, deps.sqliteStore, jobSvc)

	// Orchestrator multi-step pipeline routes (admin-protected, same as v1Admin)
	orchAdmin := r.Group("/api/v1")
	orchAdmin.Use(api.AdminAuthMiddleware(cfg))
	registerOrchestratorRoutes(orchAdmin, deps)

	// Chunked upload routes (resumable worker→master video upload)
	r.POST("/api/v1/video/chunked/init", workersuploads.InitChunkedUpload())
	r.POST("/api/v1/video/chunked/:job_id/:chunk_index", workersuploads.UploadChunk(cfg))
	r.POST("/api/v1/video/chunked/:job_id/complete", workersuploads.CompleteChunkedUpload(cfg, deps.fileQ)) (fix: add missing jobs columns migration (023), fix CompleteJob CAS, patch UpdateJobFields whitelist)

	// Bundle compat routes (frontend calls /api/bundle/* without /v1/)
	if deps.workerUpdateHandler != nil {
		r.GET("/api/bundle/info", deps.workerUpdateHandler.GetLatestBundleHandler())
		r.GET("/api/bundle/files", deps.workerUpdateHandler.GetBundleFilesHandler())
	}

	// Compat: commands endpoint for workers (registered by workers module above)
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

func registerOrchestratorRoutes(v1Admin gin.IRoutes, deps *serverDeps) {
	if deps.orchestrator == nil {
		return
	}
	orch := deps.orchestrator

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

		steps := make([]*queue.JobStep, len(req.Steps))
		for i, s := range req.Steps {
			stepID, _ := s["step_id"].(string)
			if stepID == "" {
				stepID = fmt.Sprintf("step-%d", i)
			}
			stepName, _ := s["step_name"].(string)
			if stepName == "" {
				stepName = stepID
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

			steps[i] = &queue.JobStep{
				StepID:       stepID,
				StepName:     stepName,
				StepOrder:    i,
				JobType:      jobType,
				Payload:      payload,
				Dependencies: deps,
				MaxRetries:   maxRetries,
			}
		}

		if err := orch.SubmitMultiStepJob(c.Request.Context(), req.JobID, steps, req.PipelineType, req.Metadata); err != nil {
			c.JSON(409, gin.H{"error": err.Error()})
			return
		}

		c.JSON(201, gin.H{"job_id": req.JobID, "total_steps": len(steps), "status": "PENDING"})
	})

	v1Admin.GET("/orchestrator/jobs/:id", func(c *gin.Context) {
		job := orch.GetJob(c.Param("id"))
		if job == nil {
			c.JSON(404, gin.H{"error": "orchestrator job not found"})
			return
		}
		c.JSON(200, job)
	})

	v1Admin.GET("/orchestrator/jobs", func(c *gin.Context) {
		jobs := orch.ListJobs()
		c.JSON(200, gin.H{"jobs": jobs, "total": len(jobs)})
	})

	v1Admin.GET("/orchestrator/stats", func(c *gin.Context) {
		c.JSON(200, orch.Stats())
	})
}

func registerPipelineRoutes(r *gin.Engine, cfg *config.Config, deps *serverDeps) {
	// Public pipeline endpoint: forwards to remote engine (77.93.152.122) then to workers
	r.POST("/api/remote/pipeline/generate", pipelinehandler.PipelineGenerate(cfg, deps.fileQ))

	// Pipeline status check
	r.GET("/api/remote/pipeline/status/:trace_id", pipelinehandler.PipelineStatus(cfg))

	// Cancel a running pipeline job — cancels on remote engine, local queue, and worker
	var cmdMgr *workersreg.CommandManager
	if deps.workerUpdateHandler != nil {
		cmdMgr = deps.workerUpdateHandler.CommandManager()
	}
	r.DELETE("/api/remote/pipeline/cancel/:trace_id", pipelinehandler.PipelineCancel(cfg, deps.fileQ, cmdMgr))

	// Simple script generation (single topic)
	r.POST("/api/script-simple", pipelinehandler.ScriptSimple(cfg))

	// Batch script generation (multiple topics)
	r.POST("/api/script-multiple", pipelinehandler.ScriptMultiple(cfg))
}
