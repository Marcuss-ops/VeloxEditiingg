package main

import (
	"fmt"
	"os"
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
