package main

import (
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
	"velox-server/internal/app"
	"velox-server/internal/config"
	workersuploads "velox-server/internal/handlers/remote/workers/uploads"
	"velox-server/internal/handlers/server/api"
	"velox-server/internal/handlers/server/darkeditor"
	"velox-server/internal/handlers/server/groups"
	"velox-server/internal/handlers/server/pipeline"
	scripthandlers "velox-server/internal/handlers/server/script"
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
	if cfg.Server.GinMode == "release" {
		gin.SetMode(gin.ReleaseMode)
		r = gin.New()
		r.Use(gin.Recovery())
	} else {
		r = gin.Default()
	}

	// Admin auth (regular routes) — used by the groups + pipeline
	// groups registered below. The auth is built here so its scope
	// spans every RegisterRoutes call; bootstrap_modules.go also
	// creates one for the worker module path, both are identical.
	auth := api.AdminAuthMiddleware(cfg)

	configureTrustedProxies(r)

	r.Use(corsMiddleware())
	r.Use(requestIDMiddleware())
	r.Use(accessLogMiddleware())
	r.Use(addGzipHeaders())

	// ── Module routes (health, workers, youtube, drive, ansible, frontend) ──
	registry.RegisterRoutes(r)

	// ── Remaining routes not yet in modules ──────────────────────────────────
	registerScriptRoutes(r, cfg, deps)

	// Initialize groups handlers with SQLite store (constructor DI).
	// PR-DI-groups: replaces the previous `groups.InitGroupsStore`
	// package-level mutator. Handlers now owns its dependency on the
	// struct so two `/api/v1/groups` mounts (e.g. prod + admin canary)
	// cannot collide through shared state.
	if deps.sqliteStore != nil {
		groupsGroup := r.Group("/api/v1/groups")
		groupsGroup.Use(auth)
		groups.NewHandlers(deps.sqliteStore).RegisterRoutes(groupsGroup)
	}

	// Pipeline (constructor DI).
	// PR-DI-pipeline: replaces the previous
	// `pipeline.InitRemoteEngine` + `pipeline.InitPipelineEnqueuer`
	// package-level mutators. NewHandlersFull wires every dep the
	// pipeline endpoints need (cfg, enqueuer, remote engine client,
	// jobs reader/writer for pipeline cancellation cleanup, worker
	// CommandManager for per-worker cancel notifications).
	if deps.enqueuer != nil && deps.lifecycleSvc != nil {
		jobsRepo := deps.lifecycleSvc.Jobs()
		pipeline.NewHandlersFull(
			cfg,
			deps.enqueuer,
			pipeline.NewRemoteClientFromConfig(cfg),
			jobsRepo, jobsRepo, deps.cmdMgr,
		).RegisterRoutes(r, auth)
	}

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

	// Note: orchestrator /api/v1/group pipeline routes are registered by
	// registerOrchestratorAdminRoutes above (avoids gin double-registration
	// of the same POST /orchestrator/jobs paths from a single gin IRoute).

	// PR 3.5-c: upload-completed uses the canonical artifacts.Service pipeline
	// (BeginUpload → Receive → Finalize) — single-writer SUCCEEDED gate.
	r.POST("/api/v1/video/upload-completed", workersuploads.UploadCompletedVideo(cfg, deps.artifactSvc))

	// Chunked upload routes (resumable worker→master video upload)
	// Uses the persistent ChunkedUploadService (artifact pipeline) instead of
	// the old global in-memory map — survives master restarts mid-upload.
	if deps.chunkedHandler != nil {
		r.POST("/api/v1/video/chunked/init", deps.chunkedHandler.InitChunkedUpload())
		r.POST("/api/v1/video/chunked/:job_id/:chunk_index", deps.chunkedHandler.UploadChunk())
		r.POST("/api/v1/video/chunked/:job_id/complete", deps.chunkedHandler.CompleteChunkedUpload())
	}

	// Scorecard v1 / PR-5: Prometheus /metrics exporter mount. Wired
	// ONLY when deps.metricsRegistry is non-nil (tests may disable);
	// the route is intentionally unauthenticated at the gin layer to
	// match the Prometheus convention (k8s ingress must gate this
	// route via Helm-level canary ingress). Content-type is
	// text/plain; version=0.0.4 so Prometheus rooms scrape cleanly.
	if deps.metricsRegistry != nil {
		r.GET("/metrics", gin.WrapH(deps.metricsRegistry.Handler()))
	}

	return r
}

func registerScriptRoutes(r *gin.Engine, cfg *config.Config, deps *serverDeps) {
	if deps == nil || deps.enqueuer == nil {
		return
	}

	v1Group := r.Group("/api/v1/script")
	v1Group.Use(api.AdminAuthMiddleware(cfg))
	// PR15.7a: thread the *enqueue.Enqueuer through RegisterRoutes so the
	// script endpoint can submit jobs without any package-level state.
	scripthandlers.RegisterRoutes(v1Group, cfg, deps.sqliteStore, deps.enqueuer)
}
