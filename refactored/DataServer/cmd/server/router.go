package main

import (
	"os"
	"strings"

	"velox-server/internal/app"
	"velox-server/internal/config"
	"velox-server/internal/handlers/server/analytics"
	"velox-server/internal/handlers/server/api"
	jobhandlers "velox-server/internal/handlers/server/jobs"
	scripthandlers "velox-server/internal/handlers/server/script"
	jobservice "velox-server/internal/services/jobs"
	"velox-server/internal/store"
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

	// ── Remaining routes not yet in modules ──────────────────────────────────
	registerAPIV1Routes(r, cfg, deps)
	registerNativeV1Routes(r, deps)
	registerScriptRoutes(r, cfg, deps)

	// Analytics cache
	analytics.InitAnalyticsCache(deps.paths.dataDir, deps.sqliteStore)

	return r
}

func registerAPIV1Routes(r *gin.Engine, cfg *config.Config, deps *serverDeps) {
	// TODO: migrate remaining V1 routes to dedicated api module
	jobRepo := store.NewSQLiteJobsRepository(deps.sqliteStore)
	tokenMgr := deps.workerLifecycle.GetTokenManager()
	jobSvc := jobservice.NewService(cfg, deps.fileQ, deps.redisQ, jobRepo, nil, deps.reg)
	jobAPI := jobhandlers.NewJobAPI(cfg, deps.fileQ, tokenMgr, jobSvc)
	jobSubmitHandler := jobhandlers.NewJobSubmissionHandler(cfg, deps.fileQ)
	api.RegisterV1Routes(r, cfg, deps.fileQ, deps.redisQ, deps.reg, jobAPI, jobSubmitHandler, deps.workersRepo, deps.sqliteStore, deps.workerUpdateHandler, deps.workerLifecycle, nil)
	r.POST("/api/jobs/get", jobAPI.GetJobCompatHandler())
	r.POST("/api/jobs/result", jobAPI.SubmitResultCompatHandler())
	r.GET("/api/jobs/get", jobAPI.GetJobCompatHandler())
	r.POST("/api/jobs/complete", jobAPI.CompleteJobHandler())
	r.POST("/api/jobs/fail", jobAPI.FailJobHandler())
}

func registerNativeV1Routes(r *gin.Engine, deps *serverDeps) {
	// V1 native routes need the youtube service from the youtube module.
	// Since we don't have a reference here, we pass nil for now.
	// TODO: extract youtube service from registry
	api.RegisterV1NativeRoutes(r, deps.streamsQ, deps.redisQ, deps.reg, nil, nil, deps.paths.dataDir)
}

func registerScriptRoutes(r *gin.Engine, cfg *config.Config, deps *serverDeps) {
	if deps == nil || deps.fileQ == nil {
		return
	}
	rootGroup := r.Group("/api/script")
	rootGroup.Use(adminAuthMiddleware(cfg))
	scripthandlers.RegisterRoutes(rootGroup, cfg, deps.fileQ, deps.sqliteStore)

	v1Group := r.Group("/api/v1/script")
	v1Group.Use(adminAuthMiddleware(cfg))
	scripthandlers.RegisterRoutes(v1Group, cfg, deps.fileQ, deps.sqliteStore)
}
