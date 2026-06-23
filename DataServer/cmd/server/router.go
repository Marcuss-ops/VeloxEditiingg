package main

import (
	"log"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
	"velox-server/internal/app"
	"velox-server/internal/config"
	workersuploads "velox-server/internal/handlers/remote/workers/uploads"
	"velox-server/internal/handlers/server/api"
	"velox-server/internal/handlers/server/darkeditor"
	"velox-server/internal/handlers/server/groups"
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

	configureTrustedProxies(r)

	r.Use(corsMiddleware())
	r.Use(requestIDMiddleware())
	r.Use(accessLogMiddleware())
	r.Use(addGzipHeaders())

	// ── Module routes (health, workers, youtube, drive, ansible, frontend) ──
	registry.RegisterRoutes(r)

	// ── Remaining routes not yet in modules ──────────────────────────────────
	registerOrchestratorAdminRoutes(r, cfg, deps)
	registerScriptRoutes(r, cfg, deps)

	// Initialize groups handlers with SQLite store
	groups.InitGroupsStore(deps.sqliteStore)

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

// registerOrchestratorAdminRoutes is a thin wrapper that mounts
// registerOrchestratorRoutes under the /api/v1 admin sub-group. Kept as a
// distinct entry point so caller sites that already hold an *gin.Engine +
// *serverDeps (router.go bootstrap path) don't have to know the
// orchestratorLegacyAdapter indirection.
//
// PR-operation 01 / Fase 3 — this function now builds the cutover adapter
// (creatorflow.CreateJobWithPlan for POST, Job→Run projection for GET) and
// falls back to logging if any of the Fase 3 wiring pieces is nil. Fase 8
// deletes the file entirely.
func registerOrchestratorAdminRoutes(r *gin.Engine, cfg *config.Config, deps *serverDeps) {
	if deps == nil {
		log.Printf("[ORCHESTRATOR] admin routes disabled: nil serverDeps")
		return
	}
	adapter, err := newOrchestratorLegacyAdapter(deps)
	if err != nil {
		log.Printf("[ORCHESTRATOR] admin routes disabled: %v", err)
		return
	}
	v1Admin := r.Group("/api/v1")
	v1Admin.Use(api.AdminAuthMiddleware(cfg))
	registerOrchestratorRoutes(v1Admin, adapter)
}

func registerOrchestratorRoutes(v1Admin gin.IRoutes, adapter *orchestratorLegacyAdapter) {
	if adapter == nil {
		return
	}
	v1Admin.POST("/orchestrator/jobs", adapter.postJob)
	v1Admin.GET("/orchestrator/jobs/:id", adapter.getJob)
	v1Admin.GET("/orchestrator/jobs", adapter.listJobs)
	v1Admin.GET("/orchestrator/stats", adapter.getStats)
}
