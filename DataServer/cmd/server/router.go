package main

import (
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
	"velox-server/internal/artifacts"
	"velox-server/internal/config"
	"velox-server/internal/creatorflow"
	"velox-server/internal/instaeditauth"
	workerhandlersuploads "velox-server/internal/handlers/remote/workers/uploads"
	"velox-server/internal/handlers/server/api"
	"velox-server/internal/handlers/server/darkeditor"
	instaedithandler "velox-server/internal/handlers/server/instaedit"
	"velox-server/internal/handlers/server/pipeline"
	scripthandlers "velox-server/internal/handlers/server/script"
	"velox-server/internal/jobs"
	"velox-server/internal/jobs/enqueue"
	velmetrics "velox-server/internal/metrics"
	"velox-server/internal/store"
	"velox-server/internal/taskgraph"
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
	DocCreator  scripthandlers.GoogleDocCreator
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
	TaskReader  taskgraph.Reader
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
	ArtifactReader artifacts.ArtifactReader
	BlobStore      store.BlobStore
	ChunkedHandler *workerhandlersuploads.ChunkedUploadHandler
}

// MetricsRouteDeps carries the deps for the /metrics route (Prometheus
// exporter mounted when EnableMetricsEnpoint is true).
type MetricsRouteDeps struct {
	Registry *velmetrics.Registry
}

// InstaEditRouteDeps carries the deps for the /api/v1/instaedit route
// group. The verifier is created from INSTAEDIT_CONTROL_JWT_SECRET at
// boot; when it is nil the whole group is skipped (dev/test mode).
type InstaEditRouteDeps struct {
	Verifier *instaeditauth.Verifier
	Enqueuer *enqueue.Enqueuer
	Store    *store.SQLiteStore
	Jobs     jobs.Repository
	Assets   store.AssetRepository
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
	Pipeline   PipelineRouteDeps
	Darkeditor DarkeditorRouteDeps
	Upload     UploadRouteDeps
	Metrics    MetricsRouteDeps
	InstaEdit  InstaEditRouteDeps
}

// internalSecurityGuard blocks direct browser access and enforces that
// the Velox master HTTP API is reachable only from the private
// InstaEdit/VPN network (and from workers). In release mode it rejects
// public IP addresses unless they are explicitly allow-listed.
//
// It runs before any route handler so that every HTTP surface
// (including routes registered by modules) is protected at the
// network edge. Authentication is layered on top by the per-route
// middlewares (InstaEdit JWT or admin token).
func internalSecurityGuard(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		// A missing RemoteAddr is common in unit tests. In real
		// deployments the underlying listener always provides one.
		if c.Request.RemoteAddr == "" {
			c.Next()
			return
		}

		clientIP := c.ClientIP()
		ip := net.ParseIP(clientIP)

		// Reject any request carrying a browser Origin header. Velox
		// master is not a browser-facing API; the only legitimate HTTP
		// callers are internal services (InstaEdit BFF, metrics
		// scrapers, Ansible runners). Browsers never need direct access.
		if c.GetHeader("Origin") != "" {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "direct browser access forbidden"})
			return
		}

		// Allow loopback unconditionally; side-cars and local tooling
		// run in the same pod / network namespace.
		if ip != nil && ip.IsLoopback() {
			c.Next()
			return
		}

		// In production the master is only reachable from private
		// networks. Public IPs are rejected unless explicitly listed in
		// cfg.Workers.AllowedIPs. Non-production modes keep the network
		// check permissive so dev/test tooling works, while the Origin
		// guard above still blocks cross-origin browsers.
		if isNetworkEnforced(cfg) {
			if ip == nil {
				c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "unparseable client address"})
				return
			}

			// Explicit allowlist takes precedence and can include public IPs.
			if isClientIPAllowed(clientIP, cfg.Workers.AllowedIPs) {
				c.Next()
				return
			}

			// Otherwise only private networks are permitted.
			if !ip.IsPrivate() && !ip.IsLinkLocalUnicast() {
				c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "public access forbidden: master is reachable only from the private InstaEdit/VPN network or allow-listed IPs"})
				return
			}
		}

		c.Next()
	}
}

// isNetworkEnforced reports whether the private-network access
// controls should be active. It is enabled in Gin release mode or
// when the runtime environment is explicitly production.
func isNetworkEnforced(cfg *config.Config) bool {
	if cfg.Server.GinMode == "release" {
		return true
	}
	env := strings.ToLower(strings.TrimSpace(cfg.Runtime.Environment))
	return env == "production" || env == "prod"
}

// isClientIPAllowed reports whether clientIP matches one of the entries
// in allowed. Each entry may be an exact IP or a CIDR (e.g. "10.0.0.0/8").
// Both IPv4 and IPv6 are supported.
func isClientIPAllowed(clientIP string, allowed []string) bool {
	ip := net.ParseIP(clientIP)
	if ip == nil {
		return false
	}
	for _, entry := range allowed {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		if strings.Contains(entry, "/") {
			_, cidr, err := net.ParseCIDR(entry)
			if err == nil && cidr.Contains(ip) {
				return true
			}
		} else {
			a := net.ParseIP(entry)
			if a != nil && a.Equal(ip) {
				return true
			}
		}
	}
	return false
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

	r.Use(internalSecurityGuard(cfg))
	r.Use(requestIDMiddleware())
	r.Use(accessLogMiddleware())
	r.Use(addGzipHeaders())

	// ── Module routes (health, workers, drive, ansible, frontend) ──
	registry.RegisterRoutes(r)

	// ── Remaining (non-module) routes wired per their own deps bundle ───
	registerScriptRoutes(r, bundle.Script)
	registerPipelineRoutes(r, auth, bundle.Pipeline)
	registerDarkeditorRoutes(r, bundle.Darkeditor)
	registerUploadRoutes(r, bundle.Upload)
	registerMetricsRoutes(r, bundle.Metrics)
	registerInstaEditRoutes(r, bundle.InstaEdit)

	return r
}

// registerInstaEditRoutes mounts the InstaEdit BFF route group under
// /api/v1/instaedit. Every route in this group is protected by the
// instaeditauth JWT middleware (signature, iss, aud, exp, scopes). The
// group is omitted entirely when the verifier is nil (dev/test).
func registerInstaEditRoutes(r *gin.Engine, deps InstaEditRouteDeps) {
	if deps.Verifier == nil {
		log.Printf("[ROUTES] InstaEdit BFF routes skipped: verifier=nil (INSTAEDIT_CONTROL_JWT_SECRET not configured)")
		return
	}
	if deps.Enqueuer == nil || deps.Store == nil || deps.Jobs == nil || deps.Assets == nil {
		log.Printf("[ROUTES] InstaEdit BFF routes skipped: incomplete dependencies")
		return
	}
	instaedithandler.NewHandler(instaedithandler.HandlerDeps{
		Verifier: deps.Verifier,
		Enqueuer: deps.Enqueuer,
		Store:    deps.Store,
		Jobs:     deps.Jobs,
		Assets:   deps.Assets,
	}).RegisterRoutes(r)
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
	scripthandlers.RegisterRoutes(v1Group, deps.Cfg, deps.SQLiteStore, deps.Enqueuer, deps.DocCreator)
}

func logRegisteredRoutesAtBoot(r *gin.Engine) {
	if r == nil || !strings.EqualFold(strings.TrimSpace(os.Getenv("VELOX_LOG_ROUTES_AT_BOOT")), "true") {
		return
	}
	for _, route := range r.Routes() {
		log.Printf("[ROUTE] %s %s", route.Method, route.Path)
	}
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
	).WithStore(deps.SQLiteStore).WithTaskReader(deps.TaskReader).RegisterRoutes(r, auth)
}

// registerDarkeditorRoutes mounts the /api/darkeditor routes.
func registerDarkeditorRoutes(r *gin.Engine, deps DarkeditorRouteDeps) {
	if deps.Cfg == nil {
		return
	}
	adminAuth := api.AdminAuthMiddleware(deps.Cfg)
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
	// Wrap darkeditor routes with admin auth. The dark editor SPA is
	// served by the same internal-only master, so it is protected by
	// the same service-token gate as the rest of the HTTP API.
	darkeditor.RegisterAPIRoutes(r.Group("/api/darkeditor", adminAuth), deHandler)
}

// registerUploadRoutes mounts upload-completed + chunked-upload routes.
// Each sub-route tolerates a nil sub-component so partial bundles still
// produce a working router. All upload surfaces are wrapped with the
// admin auth middleware: the Velox master HTTP API is internal-only
// (InstaEdit BFF + workers) and must never be reachable from a browser.
func registerUploadRoutes(r *gin.Engine, deps UploadRouteDeps) {
	adminAuth := api.AdminAuthMiddleware(deps.Cfg)
	if deps.ArtifactReader != nil && deps.BlobStore != nil {
		r.GET("/api/internal/artifacts/:artifact_id/download", adminAuth, artifactDownloadHandler(deps.ArtifactReader, deps.BlobStore))
		r.HEAD("/api/internal/artifacts/:artifact_id/download", adminAuth, artifactDownloadHandler(deps.ArtifactReader, deps.BlobStore))
	}
	if deps.ArtifactSvc != nil {
		r.POST("/api/v1/video/upload-completed",
			adminAuth, workerhandlersuploads.UploadCompletedVideo(deps.Cfg, deps.ArtifactSvc))
	}
	if deps.ChunkedHandler != nil {
		r.POST("/api/v1/video/chunked/init", adminAuth, deps.ChunkedHandler.InitChunkedUpload())
		r.POST("/api/v1/video/chunked/:job_id/:chunk_index", adminAuth, deps.ChunkedHandler.UploadChunk())
		r.POST("/api/v1/video/chunked/:job_id/complete", adminAuth, deps.ChunkedHandler.CompleteChunkedUpload())
	}
}

func artifactDownloadHandler(reader artifacts.ArtifactReader, blobs store.BlobStore) gin.HandlerFunc {
	return func(c *gin.Context) {
		a, err := reader.GetByID(c.Request.Context(), c.Param("artifact_id"))
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "artifact lookup failed"})
			return
		}
		if a == nil || a.Status != "READY" {
			c.AbortWithStatus(http.StatusNotFound)
			return
		}
		f, err := blobs.ReadFinal(a.StorageKey)
		if err != nil {
			c.AbortWithStatus(http.StatusNotFound)
			return
		}
		defer f.Close()
		st, err := f.Stat()
		if err != nil {
			c.AbortWithStatus(http.StatusNotFound)
			return
		}
		mime := "video/mp4"
		if strings.HasPrefix(a.Type, "video/") {
			mime = a.Type
		}
		c.Header("Content-Type", mime)
		c.Header("Content-Disposition", "attachment")
		http.ServeContent(c.Writer, c.Request, a.ID, st.ModTime(), f)
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
