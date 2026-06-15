package main

import (
	"os"
	"strings"

	"velox-server/internal/app"
	"velox-server/internal/config"
	remoteansible "velox-server/internal/handlers/remote/ansible"
	"velox-server/internal/handlers/server/analytics"
	"velox-server/internal/handlers/server/api"
	"velox-server/internal/handlers/server/groups"
	jobhandlers "velox-server/internal/handlers/server/jobs"
	pipelinehandler "velox-server/internal/handlers/server/pipeline"
	scripthandlers "velox-server/internal/handlers/server/script"
	jobservice "velox-server/internal/services/jobs"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
	"github.com/gin-gonic/gin"
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
	jobAPI := jobhandlers.NewJobAPI(cfg, deps.fileQ, tokenMgr, jobSvc)
	jobSubmitHandler := jobhandlers.NewJobSubmissionHandler(cfg, deps.fileQ)
	api.RegisterV1Routes(r, cfg, deps.fileQ, deps.reg, jobAPI, jobSubmitHandler, deps.workersRepo, deps.sqliteStore, deps.workerUpdateHandler, ansibleHandlers)
	r.POST("/api/jobs/get", jobAPI.GetJobCompatHandler())
	r.POST("/api/jobs/result", jobAPI.SubmitResultCompatHandler())
	r.GET("/api/jobs/get", jobAPI.GetJobCompatHandler())
	r.POST("/api/jobs/complete", jobAPI.CompleteJobHandler())
	r.POST("/api/jobs/fail", jobAPI.FailJobHandler())

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
