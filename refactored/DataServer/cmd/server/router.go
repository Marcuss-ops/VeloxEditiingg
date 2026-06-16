package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/app"
	"velox-server/internal/config"
	remoteansible "velox-server/internal/handlers/remote/ansible"
	"velox-server/internal/handlers/remote/workers/uploads"
	"velox-server/internal/handlers/server/analytics"
	"velox-server/internal/handlers/server/api"
	"velox-server/internal/handlers/server/darkeditor"
	"velox-server/internal/handlers/server/groups"
	jobhandlers "velox-server/internal/handlers/server/jobs"
	pipelinehandler "velox-server/internal/handlers/server/pipeline"
	scripthandlers "velox-server/internal/handlers/server/script"
	integrationsDrive "velox-server/internal/integrations/drive"
	ytservice "velox-server/internal/integrations/youtube"
	"velox-server/internal/queue"
	jobservice "velox-server/internal/services/jobs"
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
	// V1 routes are delegated entirely to the dedicated `api` package module; the helpers
	// below isolate V2 jobs, orchestrator admin, chunked upload, and bundle compatibility
	// into dedicated registration calls so concerns stay distinct.
	jrs := buildJobRoutes(cfg, deps)
	registerAPIV1Routes(r, cfg, deps, ansibleHandlers, jrs)
	registerV2JobRoutes(r, cfg, deps, jrs)
	registerOrchestratorAdminRoutes(r, cfg, deps)
	registerUploadAndBundleCompatRoutes(r, cfg, deps)
	registerScriptRoutes(r, cfg, deps)
	registerLegacyJobCompatRoutes(r, deps, jrs)
	registerPipelineRoutes(r, cfg, deps)

	// Initialize groups handlers with SQLite store
	groups.InitGroupsStore(deps.sqliteStore)

	// Analytics cache
	analytics.InitAnalyticsCache(deps.paths.dataDir, deps.sqliteStore)

	// Dark Editor API routes
	deCfg := &darkeditor.Config{
		TempDir:      filepath.Join(cfg.DataDir, "dark_editor", "temp"),
		ProjectsDir:  filepath.Join(cfg.DataDir, "dark_editor", "projects"),
		LogDir:       filepath.Join(cfg.DataDir, "dark_editor", "logs"),
		NVIDIAAPIKey: cfg.NVIDIAAPIKey,
	}
	deHandler := darkeditor.NewHandler(deCfg)
	deHandler.SetDBStore(deps.sqliteStore)
	darkeditor.RegisterAPIRoutes(r, deHandler)

	return r
}

func registerAPIV1Routes(r *gin.Engine, cfg *config.Config, deps *serverDeps, ansibleHandlers *remoteansible.AnsibleHandlers, jrs *jobRouteState) {
	// V1 routes live entirely in the dedicated `api` package module; this helper is kept
	// thin so future V1-only changes have a single, obvious landing spot.
	var youtubeService *ytservice.Service
	if deps.youtubeModule != nil {
		youtubeService = deps.youtubeModule.Service()
	}
	var driveService *integrationsDrive.Service
	if deps.driveModule != nil {
		driveService = deps.driveModule.Service()
	}
	api.RegisterV1Routes(r, cfg, deps.fileQ, deps.reg, jrs.jobAPI, jrs.submitHandler, deps.workersRepo, deps.sqliteStore, deps.workerUpdateHandler, youtubeService, driveService, ansibleHandlers)
}

// jobRouteState bundles shared job-route dependencies so V1 and V2 registration paths
// stay consistent (same service, same job API, same submission handler).
type jobRouteState struct {
	jobSvc        *jobservice.Service
	jobAPI        *jobhandlers.JobAPI
	submitHandler *jobhandlers.JobSubmissionHandler
}

func buildJobRoutes(cfg *config.Config, deps *serverDeps) *jobRouteState {
	jobRepo := store.NewSQLiteJobsRepository(deps.sqliteStore)
	tokenMgr := deps.workerLifecycle.GetTokenManager()
	jobSvc := jobservice.NewService(cfg, deps.fileQ, jobRepo, nil, deps.reg)
	if deps.workerUpdateHandler != nil {
		if hash := deps.workerUpdateHandler.ComputeBundleSHA256(); hash != "" {
			jobSvc.SetMasterBundleHash(hash)
		}
	}
	jobAPI := jobhandlers.NewJobAPI(cfg, deps.fileQ, tokenMgr, jobSvc, deps.sqliteStore)
	submitHandler := jobhandlers.NewJobSubmissionHandler(cfg, deps.fileQ)
	return &jobRouteState{
		jobSvc:        jobSvc,
		jobAPI:        jobAPI,
		submitHandler: submitHandler,
	}
}

func registerV2JobRoutes(r *gin.Engine, cfg *config.Config, deps *serverDeps, jrs *jobRouteState) {
	// V2 job routes: /api/v1/jobs/{id}/lease|complete|fail|progress|attempts|artifacts|events
	v2JobsGroup := r.Group("/api/v1")
	jobhandlers.RegisterV2JobRoutes(v2JobsGroup, cfg, deps.fileQ, deps.sqliteStore, jrs.jobSvc)
}

func registerOrchestratorAdminRoutes(r *gin.Engine, cfg *config.Config, deps *serverDeps) {
	// Orchestrator multi-step pipeline routes (admin-protected, same as v1Admin)
	orchAdmin := r.Group("/api/v1")
	orchAdmin.Use(api.AdminAuthMiddleware(cfg))
	registerOrchestratorRoutes(orchAdmin, deps)
}

func registerUploadAndBundleCompatRoutes(r *gin.Engine, cfg *config.Config, deps *serverDeps) {
	// Chunked upload routes (resumable worker→master video upload)
	r.POST("/api/v1/video/chunked/init", uploads.InitChunkedUpload())
	r.POST("/api/v1/video/chunked/:job_id/:chunk_index", uploads.UploadChunk(cfg))
	r.POST("/api/v1/video/chunked/:job_id/complete", uploads.CompleteChunkedUpload(cfg, deps.fileQ))

	// Bundle compat routes (frontend calls /api/bundle/* without /v1/)
	if deps.workerUpdateHandler != nil {
		r.GET("/api/bundle/info", deps.workerUpdateHandler.GetLatestBundleHandler())
		r.GET("/api/bundle/files", deps.workerUpdateHandler.GetBundleFilesHandler())
	}
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

func registerLegacyJobCompatRoutes(r *gin.Engine, deps *serverDeps, jrs *jobRouteState) {
	if deps == nil || jrs == nil || jrs.jobSvc == nil {
		return
	}

	legacy := r.Group("/api")

	legacy.POST("/jobs/get", func(c *gin.Context) {
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

	legacy.POST("/jobs/result", func(c *gin.Context) {
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

	legacy.POST("/jobs/complete", func(c *gin.Context) {
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

	legacy.POST("/jobs/lease", func(c *gin.Context) {
		var body struct {
			JobID          string `json:"job_id"`
			WorkerID       string `json:"worker_id"`
			LeaseID        string `json:"lease_id"`
			LeaseExpiresAt string `json:"lease_expires_at"`
			Attempt        int    `json:"attempt"`
			ContractVersion int   `json:"contract_version"`
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
