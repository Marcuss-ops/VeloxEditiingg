package api

import (
	"crypto/subtle"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/config"
	"velox-server/internal/handlers/remote/ansible"
	"velox-server/internal/handlers/remote/workers"
	workersapi "velox-server/internal/handlers/remote/workers"
	"velox-server/internal/handlers/server/analytics"
	"velox-server/internal/handlers/server/calendar"
	"velox-server/internal/handlers/server/drive"
	"velox-server/internal/handlers/server/groups"
	"velox-server/internal/handlers/server/health"
	"velox-server/internal/handlers/server/jobs"
	"velox-server/internal/handlers/server/master"
	"velox-server/internal/handlers/server/pipeline"
	scenevideo "velox-server/internal/handlers/server/video"
	"velox-server/internal/handlers/web/dashboard"
	"velox-server/internal/handlers/web/proxy"
	"velox-server/internal/queue"
	analyticsService "velox-server/internal/services/analytics"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

// APIVersionInfo represents API version information
type APIVersionInfo struct {
	Version     string    `json:"version"`
	ReleasedAt  time.Time `json:"released_at"`
	Description string    `json:"description"`
	Deprecated  bool      `json:"deprecated"`
	SunsetDate  string    `json:"sunset_date,omitempty"`
}

func adminAuthMiddleware(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		if workersreg.IsLocalRequestIP(c.ClientIP()) {
			c.Next()
			return
		}

		// Allow read-only dashboard routes without an admin token.
		// The workers/ansible UI is meant to stay live on public instances,
		// but write operations must still remain protected.
		if c.Request.Method == http.MethodGet && isPublicReadOnlyRoute(c.Request.URL.Path) {
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

func isPublicReadOnlyRoute(path string) bool {
	if path == "" {
		return false
	}

	publicPrefixes := []string{
		"/api/v1/jobs",
		"/api/v1/workers",
		"/api/v1/dashboard/summary",
		"/api/v1/dashboard/realtime",
		"/api/v1/dashboard/health",
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

// RegisterV1Routes registers all /api/v1/* routes (core API)
func RegisterV1Routes(r *gin.Engine, cfg *config.Config, fileQ *queue.FileQueue, redisQ *queue.Queue, reg *workersreg.Registry, jobAPI *jobs.JobAPI, jobSubmitHandler *jobs.JobSubmissionHandler, workersRepo store.WorkersRepository, db *store.SQLiteStore, workerUpdateHandler *workersapi.WorkerUpdateHandler, workerLifecycle *workersapi.WorkerLifecycle, ansibleHandlers *ansible.AnsibleHandlers) {
	v1 := r.Group("/api/v1")
	v1Admin := r.Group("/api/v1")
	v1Admin.Use(adminAuthMiddleware(cfg))
	{
		// Health
		v1.GET("/health", health.Health)

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
		v1Admin.GET("/workers", workers.WorkersList(reg, workersRepo))
		v1Admin.GET("/workers/:id/logs", workers.WorkerLogsHandler(reg))
		v1Admin.POST("/workers/clear_all", dashboard.WorkersClearAll(redisQ, reg))
		if workerUpdateHandler != nil {
			// Update orchestration endpoints used by the frontend and legacy bundle.
			v1Admin.POST("/workers/update_all", workerUpdateHandler.UpdateAllHandler())
			v1Admin.POST("/workers/restart_all", workerUpdateHandler.RestartAllHandler())
			v1Admin.POST("/workers/send_command_bulk", workerUpdateHandler.SendCommandBulkHandler())
			v1Admin.POST("/workers/full_update_linux", workerUpdateHandler.FullUpdateLinuxHandler())
			v1Admin.POST("/workers/rollout_update", workerUpdateHandler.RolloutUpdateHandler())
			v1Admin.POST("/worker/send_command", workerUpdateHandler.SendCommandHandler())
			v1Admin.GET("/workers/update_status", workerUpdateHandler.GetUpdateStatusHandler())
		}
		// NOTE: /workers/revoke and /workers/drain are NOT registered here to avoid
		// a Gin radix tree conflict with the wildcard route /workers/:id/logs.
		// They are registered directly in router.go under /worker/revoke and /worker/drain.

		// Bundle Explorer - List files in bundle
		if workerUpdateHandler != nil {
			v1.GET("/bundle/files", workerUpdateHandler.GetBundleFilesHandler())
			v1.GET("/bundle/info", workerUpdateHandler.GetLatestBundleHandler())
			v1.GET("/bundle/manifest", workerUpdateHandler.GetBundleManifestHandler())
		}

		// Queue - Core API
		v1.GET("/queue/job", jobAPI.GetJobHandler())
		v1.POST("/queue/start", jobAPI.StartJobHandler())
		v1.POST("/queue/complete", jobAPI.CompleteJobHandler())
		v1.POST("/queue/fail", jobAPI.FailJobHandler())

		// Stats - Core API (uses Redis if available)
		v1Admin.GET("/stats", dashboard.Stats(redisQ, reg))

		// Workers status - requires queue for job counts
		statusHandler := func(c *gin.Context) {
			// Use Redis queue if available, otherwise return basic status
			if redisQ != nil {
				workers.WorkersStatus(reg, redisQ)(c)
			} else {
				// Fallback: return basic worker list without job counts
				list := reg.List(c.Request.Context())
				c.JSON(http.StatusOK, gin.H{
					"workers":         list,
					"active_workers":  len(list),
					"total_workers":   len(list),
					"pending_jobs":    0,
					"processing_jobs": 0,
					"completed_jobs":  0,
					"error_jobs":      0,
					"total_jobs":      0,
				})
			}
		}
		v1Admin.GET("/workers/status", statusHandler)
		v1Admin.GET("/workers_status", statusHandler)

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
		v1Admin.POST("/video/smoke-clip-stock", scenevideo.CreateSmokeClipStock(cfg, fileQ))

		// Compat endpoints
		v1Admin.GET("/endpoints-status", proxy.EndpointsStatus)
		v1Admin.GET("/services/availability", proxy.ServicesAvailability)
		v1Admin.POST("/services/ensure_started", proxy.ServicesEnsureStarted)
		v1Admin.GET("/master/code-version", proxy.MasterCodeVersion)

		// Video - Core API
		v1Admin.POST("/video/create-master", master.CreateMaster(cfg, redisQ))
		v1Admin.POST("/video/create-scenes", scenevideo.CreateFromScenes(cfg, fileQ))
		v1.POST("/video/upload-completed", workers.UploadCompletedVideo(cfg, fileQ))

		// Ansible - Core API
		// Registered here so both /api/v1 and legacy /ansible paths share the same implementation.
		if ansibleHandlers != nil {
			v1Admin.GET("/ansible/capabilities", ansibleHandlers.GetCapabilitiesHandler)
			v1Admin.GET("/ansible/runs", ansibleHandlers.GetRunsHandler)
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
