package api

import (
	"context"
	"crypto/subtle"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/handlers/remote/ansible"
	"velox-server/internal/handlers/remote/workers"
	"velox-server/internal/handlers/remote/workers/management"
	"velox-server/internal/handlers/remote/workers/uploads"
	"velox-server/internal/handlers/server/analytics"
	"velox-server/internal/handlers/server/calendar"
	"velox-server/internal/handlers/server/drive"
	"velox-server/internal/handlers/server/groups"
	"velox-server/internal/handlers/server/jobs"
	"velox-server/internal/handlers/server/master"
	"velox-server/internal/handlers/server/pipeline"
	"velox-server/internal/handlers/server/smoke"
	"velox-server/internal/handlers/web/proxy"
	integrationsDrive "velox-server/internal/integrations/drive"
	ytservice "velox-server/internal/integrations/youtube"
	"velox-server/internal/queue"
	analyticsService "velox-server/internal/services/analytics"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

func AdminAuthMiddleware(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		if workersreg.IsLocalRequestIP(c.ClientIP()) {
			c.Next()
			return
		}

		// Allow read-only dashboard routes without an admin token.
		// The workers/ansible UI is meant to stay live on public instances,
		// but write operations must still remain protected.
		if c.Request.Method == http.MethodGet && IsPublicReadOnlyRoute(c.Request.URL.Path) {
			c.Next()
			return
		}

		expected := strings.TrimSpace(cfg.AdminToken)
		if expected == "" {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"error": "admin token required for remote access",
			})
			return
		}

		token := workersreg.ExtractBearerToken(
			c.GetHeader("Authorization"),
			c.GetHeader("X-Admin-Token"),
			c.Query("token"),
		)
		if token == "" || subtle.ConstantTimeCompare([]byte(token), []byte(expected)) != 1 {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "invalid admin token",
			})
			return
		}

		c.Next()
	}
}

func IsPublicReadOnlyRoute(path string) bool {
	if path == "" {
		return false
	}

	publicPrefixes := []string{
		"/api/v1/jobs",
		"/api/v1/workers",
		"/api/v1/dashboard/summary",
		"/api/v1/dashboard/realtime",
		"/api/v1/dashboard/health",
		"/api/v1/youtube",
		"/api/v1/analytics",
		"/api/v1/groups",
		"/api/v1/channels",
		"/api/v1/drive-links",
		"/api/v1/drive",
		"/api/v1/master",
		"/api/v1/ansible",
		"/api/v1/admin/ansible",
		"/api/v1/endpoints-status",
		"/api/v1/services",
		"/api/v1/bundle",
		"/api/v1/queue",
		"/api/v1/stats",
		"/api/v1/calendar",
		"/api/v1/livestream",
		"/api/bundle",
	}

	for _, prefix := range publicPrefixes {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}

	// Legacy compat paths used by the workers dashboard SPA.
	if path == "/jobs" || path == "/workers" || path == "/workers_status" || path == "/api/workers_status" || path == "/api/v1/workers_status" {
		return true
	}

	return false
}

func workerStatusCounts(ctx context.Context, fileQ *queue.FileQueue) (pending, processing, completed, errorCount, total int64) {
	if fileQ != nil {
		if stats, err := fileQ.Stats(ctx); err == nil {
			pending = stats["pending"]
			processing = stats["processing"]
			completed = stats["completed"]
			errorCount = stats["error"]
			total = stats["total"]
			return
		}
	}
	return
}

// RegisterV1Routes registers all /api/v1/* routes (core API)
func RegisterV1Routes(r *gin.Engine, cfg *config.Config, fileQ *queue.FileQueue, reg *workersreg.Registry, jobAPI *jobs.JobAPI, jobSubmitHandler *jobs.JobSubmissionHandler, workersRepo store.WorkersRepository, db *store.SQLiteStore, workerUpdateHandler *workers.WorkerUpdateHandler, youtubeService *ytservice.Service, driveService *integrationsDrive.Service, ansibleHandlers *ansible.AnsibleHandlers) {
	v1 := r.Group("/api/v1")
	v1Admin := r.Group("/api/v1")
	v1Admin.Use(AdminAuthMiddleware(cfg))
	{
		// Jobs - Core API
		v1Admin.GET("/jobs", jobAPI.GetJobsHandler())
		v1Admin.POST("/jobs", jobSubmitHandler.CreateJobHandler())
		v1Admin.GET("/jobs/summary", jobAPI.GetJobsSummaryHandler())
		v1Admin.GET("/jobs/dashboard", jobSubmitHandler.GetJobsDashboardHandler())
		v1Admin.GET("/jobs/:id", jobAPI.GetJobStatusHandler())
		v1Admin.DELETE("/jobs/:id", jobAPI.DeleteJobHandler())
		v1Admin.POST("/jobs/:id/retry", jobSubmitHandler.RetryJobHandler())
		v1Admin.POST("/jobs/bulk_delete", jobSubmitHandler.BulkDeleteJobsHandler())

		// Workers - Core API
		v1Admin.GET("/workers", workers.WorkersList(reg, workersRepo, workerUpdateHandler))
		v1Admin.GET("/workers/:id/logs", management.WorkerLogsHandler(reg))

		if workerUpdateHandler != nil {
			// Update orchestration endpoints used by the frontend and legacy bundle.
			v1Admin.POST("/workers/update_all", workerUpdateHandler.UpdateAllHandler())
			v1Admin.POST("/workers/update_all_latest_bundle", workerUpdateHandler.UpdateAllLatestBundleHandler())
			v1Admin.POST("/workers/restart_all", workerUpdateHandler.RestartAllHandler())
			v1Admin.POST("/workers/send_command_bulk", workerUpdateHandler.SendCommandBulkHandler())
			v1Admin.POST("/workers/full_update_linux", workerUpdateHandler.FullUpdateLinuxHandler())
			v1Admin.POST("/workers/rollout_update", workerUpdateHandler.RolloutUpdateHandler())
			v1Admin.POST("/worker/send_command", workerUpdateHandler.SendCommandHandler())
			v1Admin.GET("/workers/update_status", workerUpdateHandler.GetUpdateStatusHandler())
		}
		// NOTE: /workers/revoke and /workers/drain are NOT registered here to avoid
		// a Gin radix tree conflict with the wildcard route /workers/:id/logs.
		// They are registered directly in router.go under /worker/revoke and /worker/drain.		// Worker polling — canonical endpoints are registered by the workers module.

		// Bundle compat routes for frontend
		if workerUpdateHandler != nil {
			v1.GET("/bundle/files", workerUpdateHandler.GetBundleFilesHandler())
			v1.GET("/bundle/info", workerUpdateHandler.GetLatestBundleHandler())
			v1.GET("/bundle/manifest", workerUpdateHandler.GetBundleManifestHandler())
		}
		v1.GET("/queue/job", jobAPI.GetJobHandler())
		v1.POST("/queue/start", jobAPI.StartJobHandler())

		// Workers status - requires queue for job counts
		statusHandler := func(c *gin.Context) {
			ctx := c.Request.Context()
			master := workers.WorkerStatusMetadata(workerUpdateHandler)
			registered, live := reg.StatusSnapshot(ctx, time.Duration(cfg.WorkerHeartbeatTimeout)*time.Second)
			stale := reg.GetStaleWorkers(ctx, time.Duration(cfg.WorkerHeartbeatTimeout)*time.Second)
			pending, processing, completed, errorCount, total := workerStatusCounts(ctx, fileQ)

			c.JSON(http.StatusOK, gin.H{
				"workers":             registered,
				"live_workers":        live,
				"stale_workers":       stale,
				"master":              master,
				"active_workers":      len(live),
				"registered_workers":  len(registered),
				"stale_workers_count": len(stale),
				"total_workers":       len(registered),
				"pending_jobs":        pending,
				"processing_jobs":     processing,
				"completed_jobs":      completed,
				"error_jobs":          errorCount,
				"total_jobs":          total,
			})
		}
		v1Admin.GET("/workers/status", statusHandler)

		// Analytics - Core API
		v1Admin.GET("/analytics/summary", analytics.AnalyticsSummaryHandler)
		v1Admin.GET("/analytics/timeline", analytics.AnalyticsTimelineHandler)
		v1Admin.GET("/analytics/top-videos", analytics.AnalyticsTopVideosHandler)
		v1Admin.GET("/analytics/top-channels", analytics.AnalyticsTopChannelsHandler)
		v1Admin.GET("/analytics/top-groups", analytics.AnalyticsTopGroupsHandler)
		v1Admin.GET("/analytics/realtime", analytics.AnalyticsRealtimeV1Handler)

		// Channels - Core API
		v1Admin.GET("/channels", analytics.ChannelsListHandler)
		v1Admin.GET("/channels/simple", analytics.ChannelsSimpleHandler)

		// Groups - Core API
		v1Admin.GET("/groups", groups.GetGroupsHandler)
		v1Admin.GET("/groups/:name", groups.GetGroupHandler)

		// Drive - Core API
		v1Admin.GET("/drive/groups", drive.GetDriveGroupsHandler)
		v1Admin.POST("/drive/folders", drive.GetDriveFoldersHandler)
		v1Admin.POST("/drive/files", drive.DriveFilesHandler)
		v1Admin.POST("/drive/create-folder", drive.CreateDriveFolderHandler)
		v1Admin.POST("/drive/upload-text", drive.UploadTextHandler)
		v1Admin.POST("/drive/group-folders", drive.GroupFoldersHandler)
		v1Admin.POST("/drive/clip-folder-id", drive.ClipFolderIDHandler)

		// Drive Links - Core API (public for SPA access)
		v1.GET("/drive-links", drive.GetDriveLinksHandler)
		v1.GET("/drive-links/by-group/:group_name", drive.GetDriveLinksByGroupHandler)
		v1.GET("/drive-links/masters", drive.GetMasterFoldersHandler)
		v1.POST("/drive-links", drive.SaveDriveLinksHandler)
		v1.POST("/drive-links/add", drive.AddDriveFolderHandler)
		v1.PUT("/drive-links/:folder_id", drive.UpdateDriveFolderHandler)
		v1.DELETE("/drive-links/:folder_id", drive.DeleteDriveFolderHandler)
		v1.GET("/drive/oauth/start", drive.DriveOAuthStartHandlerFunc(driveService))
		v1.GET("/drive/oauth/callback", drive.DriveOAuthCallbackHandlerFunc(driveService))

		// YouTube - Core API
		v1.GET("/youtube/pending-tasks", analytics.YouTubePendingTasksHandler)

		// Master - Core API
		v1Admin.GET("/master/state", master.MasterState())
		v1Admin.POST("/master/pause_new_jobs", master.PauseNewJobs())
		v1Admin.POST("/master/pause_scheduling", master.PauseScheduling())

		// Pipeline - Core API
		v1Admin.POST("/pipeline/generate", pipeline.PipelineGenerate(cfg, fileQ))
		v1Admin.GET("/pipeline/status/:trace_id", pipeline.PipelineStatus(cfg))

		// Script - Core API
		v1Admin.POST("/script/simple", pipeline.ScriptSimple(cfg))
		v1Admin.POST("/script/multiple", pipeline.ScriptMultiple(cfg))

		// Video smoke test - Core API
		v1Admin.POST("/video/smoke-clip-stock", smoke.CreateSmokeClipStock(cfg, fileQ))

		// Compat endpoints
		v1Admin.GET("/endpoints-status", proxy.EndpointsStatus)
		v1Admin.GET("/services/availability", proxy.ServicesAvailability)
		v1Admin.POST("/services/ensure_started", proxy.ServicesEnsureStarted)
		v1Admin.GET("/master/code-version", proxy.MasterCodeVersion(cfg))

		// Video - Core API
		v1Admin.POST("/video/create-master", master.CreateMaster(cfg, fileQ))
		v1.POST("/video/upload-completed", uploads.UploadCompletedVideo(cfg, fileQ, youtubeService, driveService))

		// Ansible - Core API
		// Registered here so both /api/v1 and legacy /ansible paths share the same implementation.
		if ansibleHandlers != nil {
			v1Admin.GET("/ansible/capabilities", ansibleHandlers.GetCapabilitiesHandler)
			v1Admin.GET("/ansible/runs", ansibleHandlers.GetRunsHandler)
			v1Admin.GET("/ansible/runs/:id", ansibleHandlers.GetRunHandler)
			v1Admin.POST("/ansible/computers/run_action", ansibleHandlers.RunActionHandler)
			v1Admin.POST("/ansible/computers/run_shell", ansibleHandlers.RunShellHandler)
			v1Admin.POST("/ansible/computers/test_ssh", ansibleHandlers.TestSSHHandler)
		}

		// Dashboard - BI & Analytics Dashboard (NEW)
		// Uses dependency injection: AnalyticsService wraps the store
		if db != nil && cfg != nil && cfg.DataDir != "" {
			analyticsSvc := analyticsService.NewAnalyticsService(db)
			analytics.RegisterDashboardRoutes(r, cfg.DataDir, analyticsSvc)
		}

		// Calendar - Video Production Calendar (public, no admin auth required)
		// This is a user-facing feature accessible from the SPA
		if db != nil {
			scheduler := calendar.NewCalendarScheduler(db, fileQ)
			calendar.RegisterRoutes(v1, db, fileQ, scheduler)
		}
	}

	log.Printf("[OK] API v1 routes registered at /api/v1/*")
}

// RegisterV2Routes registers all /api/v2/* routes (enterprise API)
// These are registered by enterprise.Handlers if enterprise is enabled
