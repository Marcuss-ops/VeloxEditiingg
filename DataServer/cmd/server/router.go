package main

import (
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
	"velox-server/internal/artifacts"
	"velox-server/internal/config"
	"velox-server/internal/creatorflow"
	workerhandlersuploads "velox-server/internal/handlers/remote/workers/uploads"
	"velox-server/internal/handlers/server/api"
	"velox-server/internal/handlers/server/darkeditor"
	"velox-server/internal/handlers/server/groups"
	"velox-server/internal/handlers/server/pipeline"
	scripthandlers "velox-server/internal/handlers/server/script"
	"velox-server/internal/jobs"
	"velox-server/internal/jobs/enqueue"
	velmetrics "velox-server/internal/metrics"
	"velox-server/internal/store"
	"velox-server/internal/workers"
)

// ── Per-route dependency structs ──────────────────────────────────────────
//
// PR-ROUTER-DEPS: replace the legacy `serverDeps` mega-struct with one
// minimal deps struct per route group. Each route function declares
// exactly the dependencies it consumes; newRouter composes them from a
// single RouterBundle and never reads a "global" deps blob.
//
// Rationale:
//   * Each handler now has a compile-time-documented contract of what it
//     depends on. Adding a non-route dep (e.g. a future watcher of the
//     audit log) becomes a new struct, not a new field on a shared blob.
//   * Tests that exercise a single route group can construct just the
//     corresponding RouterDeps<X> struct without paying for the rest.
//   * Production wiring (runServer) and test wiring (buildTestDeps) no
//     longer share a struct; each side declares its own bundle.

// ScriptRouteDeps carries the deps for /api/v1/script routes (script
// generation endpoint).
type ScriptRouteDeps struct {
	Cfg         *config.Config
	SQLiteStore *store.SQLiteStore
	Enqueuer    *enqueue.Enqueuer
}

// GroupsRouteDeps carries the deps for /api/v1/groups routes (the
// WebDAV-style group manager mounted under admin auth).
type GroupsRouteDeps struct {
	SQLiteStore *store.SQLiteStore
}

// PipelineRouteDeps carries the deps for the /api/script-* and
// /api/remote/pipeline routes (remote-engine fan-out +
// creatorflow forwarder).
type PipelineRouteDeps struct {
	Cfg         *config.Config
	Enqueuer    *enqueue.Enqueuer
	SQLiteStore *store.SQLiteStore
	JobsRepo    jobs.Repository
	CmdMgr      *workers.CommandManager
	// Resolver is the canonical creatorflow.Resolver. The pipeline
	// handler delegates forward-completed routes to Resolver.Resolve
	// so the creator_forwardings row + Job row land in the same write
	// path as the CreatorForwardingRunner. Required as of Blocco 4
	// step #3 — the legacy creatorflow.Service forwarder fallback was
	// removed. Nil here is a wiring bug; registerPipelineRoutes
	// refuses to start if Resolver is nil.
	Resolver *creatorflow.Resolver
}

// DarkeditorRouteDeps carries the deps for the /api/darkeditor routes
// (NVIDIA Runway-backed dark-mode editor).
type DarkeditorRouteDeps struct {
	Cfg         *config.Config
	SQLiteStore *store.SQLiteStore
}

// UploadRouteDeps carries the deps for upload POST routes
// (upload-completed + chunked upload).
type UploadRouteDeps struct {
	Cfg            *config.Config
	ArtifactSvc    *artifacts.Service
	ChunkedHandler *workerhandlersuploads.ChunkedUploadHandler
}

// MetricsRouteDeps carries the deps for the /metrics route (Prometheus
// exporter mounted when EnableMetricsEnpoint is true).
type MetricsRouteDeps struct {
	Registry *velmetrics.Registry
}

// ── RouterBundle ───────────────────────────────────────────────────────────

// RouterBundle is the composition-root input for newRouter. It contains
// ONLY the per-route dep sets the master actually mounts. Tests can
// build a partial bundle (e.g. just ScriptRouteDeps) to exercise a
// single route group in isolation.
//
// Auth is produced inside newRouter (api.AdminAuthMiddleware) so the
// bundle never carries it — production and tests must converge on the
// same auth source.
type RouterBundle struct {
	Script     ScriptRouteDeps
	Groups     GroupsRouteDeps
	Pipeline   PipelineRouteDeps
	Darkeditor DarkeditorRouteDeps
	Upload     UploadRouteDeps
	Metrics    MetricsRouteDeps
}

// corsMiddleware + adminAuth are unchanged from the pre-refactor router.
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

// newRouter assembles the master HTTP router from the supplied
// RouterBundle. The function never reads a mega-struct — every route
// group registers itself with its OWN deps.
func newRouter(cfg *config.Config, bundle RouterBundle, registry interface {
	RegisterRoutes(*gin.Engine)
}) *gin.Engine {
	var r *gin.Engine
	if cfg.Server.GinMode == "release" {
		gin.SetMode(gin.ReleaseMode)
		r = gin.New()
		r.Use(gin.Recovery())
	} else {
		r = gin.Default()
	}

	auth := api.AdminAuthMiddleware(cfg)
	configureTrustedProxies(r)

	r.Use(corsMiddleware())
	r.Use(requestIDMiddleware())
	r.Use(accessLogMiddleware())
	r.Use(addGzipHeaders())

	// ── Module routes (health, workers, youtube, drive, ansible, frontend) ──
	registry.RegisterRoutes(r)

	// ── Remaining (non-module) routes wired per their own deps bundle ───
	registerScriptRoutes(r, bundle.Script)
	registerGroupsRoutes(r, auth, bundle.Groups)
	registerPipelineRoutes(r, auth, bundle.Pipeline)
	registerDarkeditorRoutes(r, bundle.Darkeditor)
	registerUploadRoutes(r, bundle.Upload)
	registerMetricsRoutes(r, bundle.Metrics)

	return r
}

// registerScriptRoutes mounts the /api/v1/script routes. Nil-tolerant:
// it returns silently when its bundle is empty.
func registerScriptRoutes(r *gin.Engine, deps ScriptRouteDeps) {
	if deps.Enqueuer == nil {
		log.Printf("[ROUTES] script routes skipped: enqueuer=false store=%t", deps.SQLiteStore != nil)
		return
	}

	v1Group := r.Group("/api/v1/script")
	v1Group.Use(api.AdminAuthMiddleware(deps.Cfg))
	// PR15.7a: thread *enqueue.Enqueuer through RegisterRoutes so the
	// script endpoint can submit jobs without package-level state.
	scripthandlers.RegisterRoutes(v1Group, deps.Cfg, deps.SQLiteStore, deps.Enqueuer)
}

func logRegisteredRoutesAtBoot(r *gin.Engine) {
	if r == nil || !strings.EqualFold(strings.TrimSpace(os.Getenv("VELOX_LOG_ROUTES_AT_BOOT")), "true") {
		return
	}
	for _, route := range r.Routes() {
		log.Printf("[ROUTE] %s %s", route.Method, route.Path)
	}
}

// registerGroupsRoutes mounts the /api/v1/groups routes under the
// supplied admin-auth middleware. Nil-tolerant on the store.
func registerGroupsRoutes(r *gin.Engine, auth gin.HandlerFunc, deps GroupsRouteDeps) {
	if deps.SQLiteStore == nil {
		return
	}
	groupsGroup := r.Group("/api/v1/groups")
	if auth != nil {
		groupsGroup.Use(auth)
	}
	groups.NewHandlers(deps.SQLiteStore).RegisterRoutes(groupsGroup)
}

// registerPipelineRoutes mounts /api/script-* and /api/remote/pipeline.
// jobsRepo is split into Reader + Writer for the Handlers' JobsDeps,
// but since jobs.Repository (the canonical surface) satisfies BOTH
// interfaces by structural typing, the same value passes for both.
//
// Blocco 4 step #3: the legacy fallback to NewHandlersFull (which
// constructed a forwarder Service shim) is gone. Resolver is the
// SINGLE authoritative forward-completed entry point; the composition
// root (buildAppComponents → appComponents.resolver) wires it
// unconditionally. A nil Resolver at this layer is a wiring bug and
// refuses to start (log.Fatal) — surfacing it at boot instead of
// letting clients see 404s later.
func registerPipelineRoutes(r *gin.Engine, auth gin.HandlerFunc, deps PipelineRouteDeps) {
	if deps.Enqueuer == nil || deps.JobsRepo == nil {
		return
	}
	if deps.Resolver == nil {
		log.Fatalf("[ROUTES] pipeline routes require a wired Resolver (PipelineRouteDeps.Resolver is nil); refusing to start (composition-root bug)")
	}
	pipeline.NewHandlersWithResolver(
		deps.Cfg,
		deps.Enqueuer,
		pipeline.NewRemoteClientFromConfig(deps.Cfg),
		deps.Resolver,
		deps.JobsRepo, deps.JobsRepo, deps.CmdMgr,
	).WithStore(deps.SQLiteStore).RegisterRoutes(r, auth)
}

// registerDarkeditorRoutes mounts the /api/darkeditor routes.
func registerDarkeditorRoutes(r *gin.Engine, deps DarkeditorRouteDeps) {
	if deps.Cfg == nil {
		return
	}
	deCfg := &darkeditor.Config{
		TempDir:      filepath.Join(deps.Cfg.Runtime.DataDir, "dark_editor", "temp"),
		ProjectsDir:  filepath.Join(deps.Cfg.Runtime.DataDir, "dark_editor", "projects"),
		LogDir:       filepath.Join(deps.Cfg.Runtime.DataDir, "dark_editor", "logs"),
		NVIDIAAPIKey: deps.Cfg.NVIDIA.APIKey,
	}
	deHandler := darkeditor.NewHandler(deCfg)
	if deps.SQLiteStore != nil {
		deHandler.SetDBStore(deps.SQLiteStore)
	}
	darkeditor.RegisterAPIRoutes(r, deHandler)
}

// registerUploadRoutes mounts upload-completed + chunked-upload routes.
// Each sub-route tolerates a nil sub-component so partial bundles still
// produce a working router.
func registerUploadRoutes(r *gin.Engine, deps UploadRouteDeps) {
	if deps.ArtifactSvc != nil {
		r.POST("/api/v1/video/upload-completed",
			workerhandlersuploads.UploadCompletedVideo(deps.Cfg, deps.ArtifactSvc))
	}
	if deps.ChunkedHandler != nil {
		r.POST("/api/v1/video/chunked/init", deps.ChunkedHandler.InitChunkedUpload())
		r.POST("/api/v1/video/chunked/:job_id/:chunk_index", deps.ChunkedHandler.UploadChunk())
		r.POST("/api/v1/video/chunked/:job_id/complete", deps.ChunkedHandler.CompleteChunkedUpload())
	}
}

// registerMetricsRoutes mounts the Prometheus /metrics endpoint only
// when the exporter is wired (scorecard v1 / PR-5: tests may disable).
func registerMetricsRoutes(r *gin.Engine, deps MetricsRouteDeps) {
	if deps.Registry == nil {
		return
	}
	r.GET("/metrics", gin.WrapH(deps.Registry.Handler()))
}
