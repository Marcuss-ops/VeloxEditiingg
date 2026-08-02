package main

import (
	"log"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/artifacts"
	voiceoverassets "velox-server/internal/assets"
	"velox-server/internal/config"
	"velox-server/internal/creatorflow"
	workerhandlersuploads "velox-server/internal/handlers/remote/workers/uploads"
	"velox-server/internal/handlers/server/api"
	"velox-server/internal/handlers/server/darkeditor"
	instaedithandler "velox-server/internal/handlers/server/instaedit"
	scripthandlers "velox-server/internal/handlers/server/script"
	"velox-server/internal/instaeditauth"
	"velox-server/internal/jobs"
	"velox-server/internal/jobs/enqueue"
	velmetrics "velox-server/internal/metrics"
	"velox-server/internal/performance"
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
//
// File split: this file keeps the composition-root contract surface
// (per-route deps structs, RouterBundle, the network security guard,
// newRouter). The per-group register* functions live in router_routes.go;
// the M2M middleware factory lives in router_m2m.go.

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
	Resolver     *creatorflow.Resolver
	AssetService *voiceoverassets.AssetService
}

// DarkeditorRouteDeps carries the deps for the /api/darkeditor routes
// (NVIDIA Runway-backed dark-mode editor).
type DarkeditorRouteDeps struct {
	Cfg         *config.Config
	SQLiteStore *store.SQLiteStore
	// Handler is the shared dark editor handler instance. When nil,
	// registerDarkeditorRoutes builds one locally.
	Handler *darkeditor.Handler
}

// UploadRouteDeps carries the deps for upload POST routes
// (upload-completed + chunked upload).
type UploadRouteDeps struct {
	Cfg            *config.Config
	WorkerTokens   *workers.TokenManager
	ArtifactSvc    *artifacts.Service
	ArtifactReader artifacts.ArtifactReader
	BlobStore      store.BlobStore
	ChunkedHandler *workerhandlersuploads.ChunkedUploadHandler
}

// MetricsRouteDeps carries the deps for the /metrics route (Prometheus
// exporter mounted when EnableMetricsEnpoint is true).
type MetricsRouteDeps struct {
	Registry      *velmetrics.Registry
	BenchmarkRuns performance.BenchmarkRunRepository
}

// InstaEditRouteDeps carries the deps for the /api/v1/instaedit route
// group. The verifier is created from INSTAEDIT_CONTROL_JWT_SECRET at
// boot; when it is nil the whole group is skipped (dev/test mode).
type InstaEditRouteDeps struct {
	Verifier      *instaeditauth.Verifier
	Service       *instaedithandler.Service
	DarkHandler   *darkeditor.Handler
	WebhookSecret string
}

// FleetRouteDeps carries the deps for the /api/v1/admin/operations
// audit routes (Step 4/15 fleet-operator rollout, GET-only surface).
// The Handler wraps a ControllerAudit bridge to the live
// FleetController constructed by the composition root; audit reads
// return real fleet_operations ledger rows.
//
// Auth is produced inside newRouter (api.AdminAuthMiddleware) so
// the bundle never carries it — matches the existing convention
// documented above RouterBundle. The FleetController tick goroutine
// is NOT started by this commit's wiring path: the tick lands in a
// follow-up (Step 5+) so the audit endpoints render real QUEUED
// rows NOW, but the transition lifecycle (QUEUED→RUNNING→SUCCEEDED)
// is owned by the supervisor runner registration in a later step.
type FleetRouteDeps struct {
	Handler *api.AdminOperationsHandler
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
	Fleet      FleetRouteDeps
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
}) (*gin.Engine, error) {
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
	// registerPipelineRoutes takes (adminAuth, m2mAuth); the M2M
	// middleware is constructed here from the production SQLite
	// store + M2M defaults. The composition root is the only place
	// that mints the bucket map + middleware closure; tests build
	// their own directly via pipeline.NewM2MJwAuthMiddleware.
	registerPipelineRoutes(r, auth, newM2MJwAuthFromBundle(cfg, bundle.Pipeline), bundle.Pipeline)
	registerDarkeditorRoutes(r, bundle.Darkeditor)
	registerUploadRoutes(r, bundle.Upload)
	registerMetricsRoutes(r, bundle.Metrics)
	registerBenchmarkRoutes(r, bundle.Metrics, cfg)
	if err := registerInstaEditRoutes(r, bundle.InstaEdit); err != nil {
		return nil, err
	}

	// ── Admin CRUD for M2M API keys + audit log (still guarded by
	//    adminAuth so operators — NOT external M2M clients — can
	//    rotate/disable keys). Mounted under /api/v1/admin/m2m so it
	//    follows the existing /api/v1/admin/* convention.
	registerM2MAdminRoutes(r, auth, bundle.Pipeline.SQLiteStore)

	// ── Step 4/15 fleet-operator audit (GET-only). The Handler is
	//    constructed by the composition root from the live
	//    FleetController. nil-tolerant: a misconfigured boot keeps
	//    the routes un-mounted rather than serving a 503 on every
	//    request. AdminAuth path mirrors /api/v1/admin/workers
	//    above.
	registerFleetOperationsRoutes(r, auth, bundle.Fleet)

	return r, nil
}

func logRegisteredRoutesAtBoot(r *gin.Engine) {
	if r == nil || !strings.EqualFold(strings.TrimSpace(os.Getenv("VELOX_LOG_ROUTES_AT_BOOT")), "true") {
		return
	}
	for _, route := range r.Routes() {
		log.Printf("[ROUTE] %s %s", route.Method, route.Path)
	}
}
