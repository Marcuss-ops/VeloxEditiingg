package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/app"
	"velox-server/internal/config"
	"velox-server/internal/deprecation"
	remoteansible "velox-server/internal/handlers/remote/ansible"
	workersuploads "velox-server/internal/handlers/remote/workers/uploads"
	"velox-server/internal/handlers/server/analytics"
	"velox-server/internal/handlers/server/api"
	"velox-server/internal/handlers/server/groups"
	"velox-server/internal/handlers/server/darkeditor"
	pipelinehandler "velox-server/internal/handlers/server/pipeline"
	scripthandlers "velox-server/internal/handlers/server/script"
	jobservice "velox-server/internal/services/jobs"
	workersreg "velox-server/internal/workers"
	"velox-server/internal/workflow"
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
	// V1 routes are delegated entirely to the dedicated `api` package module; the helpers
	// below isolate V2 jobs, orchestrator admin, chunked upload, and bundle compatibility
	// into dedicated registration calls so concerns stay distinct.
	jrs := buildJobRoutes(cfg, deps)
	dep := buildDeprecationRegistry()
	snap := dep.Snapshot()
	log.Printf("[DEPRECATION] %d legacy endpoints tracked; sunset %s (override via VELOX_LEGACY_SUNSET_DAYS, default 14d, max 30d). Read counters at /api/_internal/deprecation_stats.",
		len(snap.Stats), snap.SunsetAt)
	registerDeprecationStatsRoute(r, cfg, dep)
	registerAPIV1Routes(r, cfg, deps, ansibleHandlers, jrs)
	registerV2JobRoutes(r, cfg, deps, jrs)
	registerOrchestratorAdminRoutes(r, cfg, deps)
	registerUploadAndBundleCompatRoutes(r, cfg, deps)
	registerScriptRoutes(r, cfg, deps)
	registerLegacyJobCompatRoutes(r, deps, jrs, dep)
	registerPipelineRoutes(r, cfg, deps, dep)

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

	// Note: orchestrator /api/v1/group pipeline routes are registered by
	// registerOrchestratorAdminRoutes above (avoids gin double-registration
	// of the same POST /orchestrator/jobs paths from a single gin IRoute).

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

	return r
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

// buildDeprecationRegistry returns the in-memory Registry used to track
// calls to legacy endpoints (post-split), wired with a sunset window.
// PR 2 of the velox-core verdict: keep legacy endpoints alive for 7-14
// days while the operator confirms zero callers, then delete them.
//
// Sunset is configurable via VELOX_LEGACY_SUNSET_DAYS (default 14, max 30).
func buildDeprecationRegistry() *deprecation.Registry {
	now := time.Now().UTC()
	sunsetDays := 14
	if v := strings.TrimSpace(os.Getenv("VELOX_LEGACY_SUNSET_DAYS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 1 && n <= 30 {
			sunsetDays = n
		}
	}
	reg := deprecation.New(now, now.Add(time.Duration(sunsetDays)*24*time.Hour))

	// Legacy /api/jobs/* worker polling endpoints (superceded by /api/v1/jobs/* V2 routes
	// for new workers; these four endpoints carry the historical worker contract).
	reg.Register("POST", "/api/jobs/get", "")
	reg.Register("POST", "/api/jobs/result", "POST /api/v1/jobs/:id/result")
	reg.Register("POST", "/api/jobs/complete", "POST /api/v1/jobs/:id/complete")
	reg.Register("POST", "/api/jobs/lease", "PUT /api/v1/jobs/:id/lease")

	// Legacy /api/remote/pipeline/* + /api/script-{simple,multiple} endpoints.
	reg.Register("POST", "/api/remote/pipeline/generate", "POST /api/v1/pipeline/generate")
	reg.Register("GET", "/api/remote/pipeline/status/:trace_id", "GET /api/v1/pipeline/status/:trace_id")
	reg.Register("DELETE", "/api/remote/pipeline/cancel/:trace_id", "DELETE /api/v1/pipeline/cancel/:trace_id")
	reg.Register("POST", "/api/script-simple", "POST /api/v1/script/generate-with-images")
	reg.Register("POST", "/api/script-multiple", "POST /api/v1/script (batch)")

	return reg
}

// registerDeprecationStatsRoute exposes /api/_internal/deprecation_stats
// behind the admin auth middleware. Operators poll this endpoint to
// confirm caller counts are zero before scheduling the next PR that
// actually removes the legacy handlers.
func registerDeprecationStatsRoute(r *gin.Engine, cfg *config.Config, dep *deprecation.Registry) {
	if dep == nil {
		return
	}
	internal := r.Group("/api/_internal")
	internal.Use(api.AdminAuthMiddleware(cfg))
	internal.GET("/deprecation_stats", func(c *gin.Context) {
		c.JSON(http.StatusOK, dep.Snapshot())
	})
}

func registerLegacyJobCompatRoutes(r *gin.Engine, deps *serverDeps, jrs *jobRouteState, dep *deprecation.Registry) {
	if deps == nil || jrs == nil || jrs.jobSvc == nil {
		return
	}

	// Loadability note: every legacy endpoint is wrapped in dep.Track(...)
	// so hits are counted and Deprecation/Sunset/Link headers are set.
	// The handler bodies are unchanged.
	legacy := r.Group("/api")

	legacy.POST("/jobs/get", dep.Track("POST", "/api/jobs/get"), func(c *gin.Context) {
		var body struct {
			WorkerID    string `json:"worker_id"`
			WorkerName  string `json:"worker_name"`
			Drain       bool   `json:"drain"`
			Schedulable bool   `json:"schedulable"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(200, gin.H{
				"success": false,
				"message": "invalid request",
				"error":   err.Error(),
			})
			return
		}

		result, err := jrs.jobSvc.ClaimNextJob(c.Request.Context(), jobservice.ClaimRequest{
			WorkerID:    strings.TrimSpace(body.WorkerID),
			WorkerName:  strings.TrimSpace(body.WorkerName),
			ClientIP:    c.ClientIP(),
			Drain:       body.Drain,
			Schedulable: body.Schedulable,
		})
		if err != nil {
			c.JSON(200, gin.H{
				"success": false,
				"message": err.Error(),
			})
			return
		}
		if result == nil || strings.TrimSpace(result.JobID) == "" {
			resp := gin.H{"success": false}
			if result != nil && strings.TrimSpace(result.Reason) != "" {
				resp["message"] = result.Reason
			}
			c.JSON(200, resp)
			return
		}

		payload := map[string]interface{}{}
		for k, v := range result.Payload {
			payload[k] = v
		}
		if createdAt, ok := payload["created_at"]; ok {
			payload["created_at"] = normalizeLegacyJobTime(createdAt)
		}
		if updatedAt, ok := payload["updated_at"]; ok {
			payload["updated_at"] = normalizeLegacyJobTime(updatedAt)
		}
		if startedAt, ok := payload["started_at"]; ok {
			payload["started_at"] = normalizeLegacyJobTime(startedAt)
		}
		if leaseExp, ok := payload["lease_expiry"]; ok {
			payload["lease_expiry"] = normalizeLegacyJobTime(leaseExp)
		}

		c.JSON(200, gin.H{
			"success": true,
			"data":    payload,
		})
	})

	legacy.POST("/jobs/result", dep.Track("POST", "/api/jobs/result"), func(c *gin.Context) {
		var body struct {
			JobID           string                 `json:"job_id"`
			JobRunID        string                 `json:"job_run_id"`
			WorkerID        string                 `json:"worker_id"`
			Status          string                 `json:"status"`
			Output          map[string]interface{} `json:"output"`
			Error           string                 `json:"error"`
			StartTime       string                 `json:"start_time"`
			EndTime         string                 `json:"end_time"`
			ContractVersion int                    `json:"contract_version"`
			LeaseID         string                 `json:"lease_id"`
			Attempt         int                    `json:"attempt"`
			ArtifactID      string                 `json:"artifact_id"`
			OutputSHA256    string                 `json:"output_sha256"`
			IdempotencyKey  string                 `json:"idempotency_key"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(200, gin.H{"success": false, "error": "invalid request"})
			return
		}
		ok, err := jrs.jobSvc.SubmitResult(c.Request.Context(), jobservice.SubmitResultRequest{
			JobID:           strings.TrimSpace(body.JobID),
			WorkerID:        strings.TrimSpace(body.WorkerID),
			Status:          strings.TrimSpace(body.Status),
			Error:           body.Error,
			Output:          body.Output,
			EndTime:         strings.TrimSpace(body.EndTime),
			LeaseID:         strings.TrimSpace(body.LeaseID),
			Attempt:         body.Attempt,
			ContractVersion: body.ContractVersion,
			ArtifactID:      strings.TrimSpace(body.ArtifactID),
			OutputSHA256:    strings.TrimSpace(body.OutputSHA256),
			IdempotencyKey:  strings.TrimSpace(body.IdempotencyKey),
		})
		if err != nil {
			c.JSON(200, gin.H{"success": false, "error": err.Error()})
			return
		}
		c.JSON(200, gin.H{"success": ok})
	})

	legacy.POST("/jobs/complete", dep.Track("POST", "/api/jobs/complete"), func(c *gin.Context) {
		var body struct {
			JobID    string `json:"job_id"`
			WorkerID string `json:"worker_id"`
			LeaseID  string `json:"lease_id"`
			Attempt  int    `json:"attempt"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(200, gin.H{"success": false, "error": "invalid request"})
			return
		}
		if err := jrs.jobSvc.ValidateJobLease(c.Request.Context(), strings.TrimSpace(body.JobID), strings.TrimSpace(body.WorkerID), strings.TrimSpace(body.LeaseID)); err != nil {
			c.JSON(200, gin.H{"success": false, "error": err.Error()})
			return
		}
		if err := jrs.jobSvc.CompleteJob(c.Request.Context(), strings.TrimSpace(body.JobID), strings.TrimSpace(body.WorkerID)); err != nil {
			c.JSON(200, gin.H{"success": false, "error": err.Error()})
			return
		}
		c.JSON(200, gin.H{"success": true})
	})

	legacy.POST("/jobs/lease", dep.Track("POST", "/api/jobs/lease"), func(c *gin.Context) {
		var body struct {
			JobID           string `json:"job_id"`
			WorkerID        string `json:"worker_id"`
			LeaseID         string `json:"lease_id"`
			LeaseExpiresAt  string `json:"lease_expires_at"`
			Attempt         int    `json:"attempt"`
			ContractVersion int    `json:"contract_version"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(200, gin.H{"success": false, "error": "invalid request"})
			return
		}
		if err := jrs.jobSvc.ValidateJobLease(c.Request.Context(), strings.TrimSpace(body.JobID), strings.TrimSpace(body.WorkerID), strings.TrimSpace(body.LeaseID)); err != nil {
			c.JSON(200, gin.H{"success": false, "error": err.Error()})
			return
		}
		if deps.fileQ == nil {
			c.JSON(200, gin.H{"success": false, "error": "queue unavailable"})
			return
		}
		if err := deps.fileQ.RenewJobLease(c.Request.Context(), strings.TrimSpace(body.JobID), strings.TrimSpace(body.WorkerID), strings.TrimSpace(body.LeaseID), time.Now().UTC().Add(30*time.Minute)); err != nil {
			c.JSON(200, gin.H{"success": false, "error": err.Error()})
			return
		}
		c.JSON(200, gin.H{"success": true})
	})
}

func normalizeLegacyJobTime(v interface{}) interface{} {
	switch tv := v.(type) {
	case string:
		if strings.TrimSpace(tv) != "" {
			return tv
		}
	case int64:
		if tv > 0 {
			return time.Unix(tv, 0).UTC().Format(time.RFC3339)
		}
	case int:
		if tv > 0 {
			return time.Unix(int64(tv), 0).UTC().Format(time.RFC3339)
		}
	case float64:
		if tv > 0 {
			return time.Unix(int64(tv), 0).UTC().Format(time.RFC3339)
		}
	}
	return v
}

// registerOrchestratorAdminRoutes is a thin wrapper that mounts
// registerOrchestratorRoutes under the /api/v1 admin sub-group. Kept as a
// distinct entry point so caller sites that already hold an *gin.Engine +
// *serverDeps (router.go bootstrap path) don't have to know the
// workflow.Repository indirection.
func registerOrchestratorAdminRoutes(r *gin.Engine, cfg *config.Config, deps *serverDeps) {
	if deps == nil || deps.workflowRepo == nil {
		return
	}
	v1Admin := r.Group("/api/v1")
	v1Admin.Use(api.AdminAuthMiddleware(cfg))
	registerOrchestratorRoutes(v1Admin, deps.workflowRepo)
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
			"job_id":        run.RunID,
			"workflow_type": run.WorkflowType,
			"total_steps":   len(run.Input),
			"steps_count":   len(spec.Steps),
			"status":        string(run.Status),
			"created_at":    run.CreatedAt,
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

func registerPipelineRoutes(r *gin.Engine, cfg *config.Config, deps *serverDeps, dep *deprecation.Registry) {
	// Legacy pipeline endpoints: still alive for the sunset window so existing
	// callers keep working. Each is wrapped by dep.Track(name) to:
	//   - bump per-endpoint hit/error counters
	//   - emit RFC 8594 Deprecation/Sunset + Link: <successor>
	//   - write a one-line [DEPRECATED] entry to server.log
	// Once the 7-14 day window elapses with zero hits, a follow-up PR
	// deletes these routes and the corresponding handlers.
	var cmdMgr *workersreg.CommandManager
	if deps.workerUpdateHandler != nil {
		cmdMgr = deps.workerUpdateHandler.CommandManager()
	}

	r.POST("/api/remote/pipeline/generate",
		dep.Track("POST", "/api/remote/pipeline/generate"),
		pipelinehandler.PipelineGenerate(cfg, deps.fileQ))

	r.GET("/api/remote/pipeline/status/:trace_id",
		dep.Track("GET", "/api/remote/pipeline/status/:trace_id"),
		pipelinehandler.PipelineStatus(cfg))

	r.DELETE("/api/remote/pipeline/cancel/:trace_id",
		dep.Track("DELETE", "/api/remote/pipeline/cancel/:trace_id"),
		pipelinehandler.PipelineCancel(cfg, deps.fileQ, cmdMgr))

	r.POST("/api/script-simple",
		dep.Track("POST", "/api/script-simple"),
		pipelinehandler.ScriptSimple(cfg))

	r.POST("/api/script-multiple",
		dep.Track("POST", "/api/script-multiple"),
		pipelinehandler.ScriptMultiple(cfg))
}

func registerAPIV1Routes(r *gin.Engine, cfg *config.Config, deps *serverDeps, ansibleHandlers *remoteansible.AnsibleHandlers, jrs *jobRouteState) {
}

func registerV2JobRoutes(r *gin.Engine, cfg *config.Config, deps *serverDeps, jrs *jobRouteState) {
}

func registerUploadAndBundleCompatRoutes(r *gin.Engine, cfg *config.Config, deps *serverDeps) {
}
