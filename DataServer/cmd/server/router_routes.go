package main

import (
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/artifacts"
	workerhandlersuploads "velox-server/internal/handlers/remote/workers/uploads"
	"velox-server/internal/handlers/server/api"
	"velox-server/internal/handlers/server/darkeditor"
	instaedithandler "velox-server/internal/handlers/server/instaedit"
	"velox-server/internal/handlers/server/pipeline"
	scripthandlers "velox-server/internal/handlers/server/script"
	velmetrics "velox-server/internal/metrics"
	"velox-server/internal/store"
)

// ── Per-group route registration ──────────────────────────────────────────
//
// Split out of router.go so the composition root (RouterBundle +
// newRouter) stays readable. Each register* function consumes exactly
// its own deps struct; nil-tolerant where documented.

// registerInstaEditRoutes mounts the InstaEdit BFF route group under
// /api/v1/instaedit. Every route in this group is protected by the
// instaeditauth JWT middleware (signature, iss, aud, exp, scopes).
//
// Failure modes:
//   - Verifier nil: feature is explicitly disabled; routes are not
//     mounted and the function returns nil.
//   - Verifier non-nil (feature configured) but any other required
//     dependency is nil: the function returns an error so the master
//     refuses to start in a misconfigured state.
func registerInstaEditRoutes(r *gin.Engine, deps InstaEditRouteDeps) error {
	if deps.Service != nil && deps.WebhookSecret != "" {
		instaedithandler.NewHandler(instaedithandler.HandlerDeps{
			Service: deps.Service, WebhookSecret: deps.WebhookSecret,
		}).RegisterInternalCallbackRoute(r)
	}
	if deps.Verifier == nil {
		log.Printf("[ROUTES] InstaEdit BFF routes skipped: verifier=nil (INSTAEDIT_CONTROL_JWT_SECRET not configured)")
		return nil
	}
	if deps.Service == nil {
		return fmt.Errorf("InstaEdit BFF routes enabled but service is nil")
	}
	if deps.DarkHandler == nil {
		return fmt.Errorf("InstaEdit BFF routes enabled but dark editor handler is nil")
	}
	instaedithandler.NewHandler(instaedithandler.HandlerDeps{
		Verifier:      deps.Verifier,
		Service:       deps.Service,
		DarkHandler:   deps.DarkHandler,
		WebhookSecret: deps.WebhookSecret,
	}).RegisterRoutes(r)
	return nil
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

// registerPipelineRoutes mounts /api/script-* and /api/remote/pipeline.
// jobsRepo is split into Reader + Writer for the Handlers' JobsDeps,
// but since jobs.Repository (the canonical surface) satisfies BOTH
// interfaces by structural typing, the same value passes for both.
//
// m2mAuth is applied EXCLUSIVELY to /api/v1/jobs (the M2M intake).
// Every other group keeps the legacy adminAuth. nil m2mAuth falls
// back to adminAuth so test mounts retain the legacy shape.
//
// Blocco 4 step #3: the legacy fallback to NewHandlersFull (which
// constructed a forwarder Service shim) is gone. Resolver is the
// SINGLE authoritative forward-completed entry point; the composition
// root (buildAppComponents → appComponents.resolver) wires it
// unconditionally. A nil Resolver at this layer is a wiring bug and
// refuses to start (log.Fatal) — surfacing it at boot instead of
// letting clients see 404s later.
func registerPipelineRoutes(r *gin.Engine, auth, m2mAuth gin.HandlerFunc, deps PipelineRouteDeps) {
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
	).WithStore(deps.SQLiteStore).WithTaskReader(deps.TaskReader).WithAssetService(deps.AssetService).WithIntakeSink(velmetrics.NewCreatorIntakeSink()).RegisterRoutes(r, auth, m2mAuth)
}

// registerDarkeditorRoutes mounts the legacy /api/darkeditor/dark_editor_v2
// routes and reuses the shared handler if one was supplied by the
// composition root. The InstaEdit-protected editor surface is mounted
// separately under /api/v1/instaedit/editor by registerInstaEditRoutes.
func registerDarkeditorRoutes(r *gin.Engine, deps DarkeditorRouteDeps) {
	if deps.Cfg == nil {
		return
	}
	adminAuth := api.AdminAuthMiddleware(deps.Cfg)
	var deHandler *darkeditor.Handler
	if deps.Handler != nil {
		deHandler = deps.Handler
	} else {
		deCfg := &darkeditor.Config{
			TempDir:      filepath.Join(deps.Cfg.Runtime.DataDir, "dark_editor", "temp"),
			ProjectsDir:  filepath.Join(deps.Cfg.Runtime.DataDir, "dark_editor", "projects"),
			LogDir:       filepath.Join(deps.Cfg.Runtime.DataDir, "dark_editor", "logs"),
			NVIDIAAPIKey: deps.Cfg.NVIDIA.APIKey,
		}
		deHandler = darkeditor.NewHandler(deCfg)
		if deps.SQLiteStore != nil {
			deHandler.SetDBStore(deps.SQLiteStore)
		}
	}
	// Wrap darkeditor routes with admin auth. The dark editor SPA is
	// served by the same internal-only master, so it is protected by
	// the same service-token gate as the rest of the HTTP API.
	//
	// NOTE: RegisterAPIRoutes no longer adds its own /dark_editor_v2
	// prefix, so we mount the legacy surface at
	// /api/darkeditor/dark_editor_v2/* for backwards compatibility.
	darkeditor.RegisterAPIRoutes(r.Group("/api/darkeditor/dark_editor_v2", adminAuth), deHandler)
}

// registerUploadRoutes mounts upload-completed + chunked-upload routes.
// Each sub-route tolerates a nil sub-component so partial bundles still
// produce a working router. All upload surfaces are wrapped with the
// admin auth middleware: the Velox master HTTP API is internal-only
// (InstaEdit BFF + workers) and must never be reachable from a browser.
func registerUploadRoutes(r *gin.Engine, deps UploadRouteDeps) {
	adminAuth := api.AdminAuthMiddleware(deps.Cfg)
	workerDataPlaneAuth := api.WorkerOrAdminAuthMiddleware(deps.Cfg, deps.WorkerTokens)
	if deps.ArtifactReader != nil && deps.BlobStore != nil {
		r.GET("/api/internal/artifacts/:artifact_id/download", adminAuth, artifactDownloadHandler(deps.ArtifactReader, deps.BlobStore))
		r.HEAD("/api/internal/artifacts/:artifact_id/download", adminAuth, artifactDownloadHandler(deps.ArtifactReader, deps.BlobStore))
	}
	if deps.ArtifactSvc != nil {
		r.POST("/api/v1/video/upload-completed",
			workerDataPlaneAuth, workerhandlersuploads.UploadCompletedVideo(deps.Cfg, deps.ArtifactSvc))
	}
	if deps.ChunkedHandler != nil {
		deps.ChunkedHandler.SetCommitTokenVerifier(deps.CompletionTokenVerifier)
		r.POST("/api/v1/video/chunked/init", adminAuth, deps.ChunkedHandler.InitChunkedUpload())
		r.POST("/api/v1/video/chunked/:job_id/:chunk_index", adminAuth, deps.ChunkedHandler.UploadChunk())
		r.POST("/api/v1/video/chunked/:job_id/complete", adminAuth, deps.ChunkedHandler.CompleteChunkedUpload())
		// Typed worker artifact publication authenticates with its short-lived
		// HMAC commit token, not the master admin bearer.
		r.POST("/api/v1/video/master-stream/:upload_id/:chunk_index", deps.ChunkedHandler.MasterStreamChunk())
		r.POST("/api/v1/video/master-stream/:upload_id/complete", deps.ChunkedHandler.MasterStreamComplete())
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

// registerM2MAdminRoutes mounts the admin CRUD endpoints for M2M
// API keys + audit log under /api/v1/admin/m2m/* . The endpoints
// are guarded by the OPERATOR'S adminAuth (VELOX_ADMIN_TOKEN) —
// NOT M2M — because a M2M client must NOT be able to mint another
// client's credentials (else the rate-limit + audit-trail model
// collapses). The split:
//   - /api/v1/jobs           → m2mAuth    (per-client credentials)
//   - /api/v1/admin/m2m/keys → adminAuth  (operator manage keys)
//
// is the canonical authorization boundary.
//
// nil SQLiteStore is treated as "feature disabled" — routes
// skipped silently — so dev/test wiring that doesn't include a
// store still boots (the bootstrap log line tells the operator
// what happened).
func registerM2MAdminRoutes(r *gin.Engine, auth gin.HandlerFunc, st *store.SQLiteStore) {
	if st == nil {
		log.Printf("[ROUTES] M2M admin routes skipped: store=nil")
		return
	}
	if auth == nil {
		log.Fatalf("[ROUTES] M2M admin routes require adminAuth (auth=nil); refusing to start")
	}
	admin := r.Group("/api/v1/admin/m2m", auth)
	admin.POST("/keys", api.IssueM2MKey(st))
	admin.GET("/keys", api.ListM2MKeys(st))
	admin.GET("/keys/:client_id", api.GetM2MKey(st))
	admin.DELETE("/keys/:client_id", api.DisableM2MKey(st))
	admin.GET("/audit", api.ListM2MAudit(st))
	log.Printf("[ROUTES] M2M admin routes mounted under /api/v1/admin/m2m")
}

// registerFleetOperationsRoutes mounts the fleet-operator audit
// surface at /api/v1/admin/operations/* . The Handler was
// constructed by the composition root from the live FleetController;
// nil-tolerant: a misconfigured boot (handler=nil) keeps the
// route un-mounted rather than serving a 503-on-every-request
// dead route, matching the nil-guard pattern used for admin/workers
// in internal/app/workers.go.
//
// Auth surface mirrors /api/v1/admin/workers (adminAuth via the
// operator's VELOX_ADMIN_TOKEN).
func registerFleetOperationsRoutes(r *gin.Engine, auth gin.HandlerFunc, deps FleetRouteDeps) {
	if deps.Handler == nil {
		log.Printf("[ROUTES] fleet operations audit routes skipped: handler=nil (FleetController not wired at this boot)")
		return
	}
	if auth == nil {
		log.Fatalf("[ROUTES] fleet operations audit routes require adminAuth (auth=nil); refusing to start")
	}
	adminOps := r.Group("/api/v1/admin/operations", auth)
	adminOps.GET("", deps.Handler.ListAdminOperations())
	adminOps.GET("/:operation_id", deps.Handler.GetAdminOperation())
	log.Printf("[ROUTES] Fleet operations audit routes mounted under /api/v1/admin/operations")
}
