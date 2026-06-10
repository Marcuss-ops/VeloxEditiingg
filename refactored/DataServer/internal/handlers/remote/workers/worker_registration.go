package workers

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/config"
	workersreg "velox-server/internal/workers"
)

// WorkerLifecycle handles worker registration, commands, and lifecycle
type WorkerLifecycle struct {
	cfg           *config.Config
	reg           *workersreg.Registry
	persistedReg  *workersreg.WorkerRegistry
	cmdMgr        *workersreg.CommandManager
	updateMgr     *workersreg.UpdateManager
	tokenMgr      *workersreg.TokenManager
	codeVersion   string
	versionNumber string
}

func (wl *WorkerLifecycle) authorizeWorkerRequest(c *gin.Context, workerID string) bool {
	token := workersreg.ExtractBearerToken(
		c.GetHeader("Authorization"),
		c.GetHeader("X-Admin-Token"),
		c.Query("token"),
	)
	if !workersreg.AuthorizeWorkerToken(wl.tokenMgr, token, workerID, c.ClientIP()) {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid worker token"})
		return false
	}
	return true
}

// NewWorkerLifecycle creates a new WorkerLifecycle handler
func NewWorkerLifecycle(cfg *config.Config, reg *workersreg.Registry, persistedReg *workersreg.WorkerRegistry, dataDir string) *WorkerLifecycle {
	return &WorkerLifecycle{
		cfg:           cfg,
		reg:           reg,
		persistedReg:  persistedReg,
		cmdMgr:        workersreg.NewCommandManager(),
		updateMgr:     workersreg.NewUpdateManager(),
		tokenMgr:      workersreg.NewTokenManager(),
		codeVersion:   cfg.CodeVersion,
		versionNumber: cfg.VersionNumber,
	}
}

// RegisterV2Handler handles POST /register_v2
func (wl *WorkerLifecycle) RegisterV2Handler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID      string                 `json:"worker_id"`
			WorkerName    string                 `json:"worker_name"`
			APIVersion    string                 `json:"api_version"`
			IPAddress     string                 `json:"ip_address"`
			Host          string                 `json:"host"`
			CodeVersion   string                 `json:"code_version"`
			BundleVersion string                 `json:"bundle_version"`
			Extra         map[string]interface{} `json:"extra"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
			return
		}

		// Validate required fields
		if body.WorkerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id required"})
			return
		}

		// Validate API version
		if body.APIVersion != "2.0" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "api_version required: 2.0 (received: " + body.APIVersion + "). Update the worker.",
			})
			return
		}

		// Check if worker is revoked
		if wl.persistedReg != nil && wl.persistedReg.IsRevoked(body.WorkerID) {
			// Silent rejection (204 No Content)
			c.Status(http.StatusNoContent)
			return
		}

		// Get IP address
		ipAddress := body.IPAddress
		if ipAddress == "" {
			ipAddress = body.Host
		}
		if ipAddress == "" {
			ipAddress = c.ClientIP()
		}

		// Get worker name
		workerName := strings.TrimSpace(body.WorkerName)
		if workerName == "" {
			workerName = body.WorkerID
		}

		// Register in memory registry
		extra := body.Extra
		if extra == nil {
			extra = make(map[string]interface{})
		}
		if body.CodeVersion != "" {
			extra["code_version"] = body.CodeVersion
		}
		if body.BundleVersion != "" {
			extra["bundle_version"] = body.BundleVersion
		}

		ctx := c.Request.Context()
		if err := wl.reg.RegisterWorker(ctx, body.WorkerID, workerName, ipAddress, extra); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "registration failed"})
			return
		}

		// Register in persisted registry
		if wl.persistedReg != nil {
			wl.persistedReg.Register(body.WorkerID, workerName, ipAddress)
		}

		// Check for pending update acknowledgment
		pendingUpdate := wl.updateMgr.GetPendingUpdate(body.WorkerID)
		if pendingUpdate != nil && pendingUpdate.Ack {
			log.Printf("🔄 Worker %s reconnected after update (version: %s)", workerName, pendingUpdate.AckVersion)
		}

		log.Printf("✅ Worker registered: %s (%s) ip=%s", workerName, body.WorkerID[:min(16, len(body.WorkerID))]+"...", ipAddress)

		c.JSON(http.StatusOK, gin.H{
			"status":      "success",
			"worker_id":   body.WorkerID,
			"worker_name": workerName,
		})
	}
}

// RegisterCompatHandler handles POST /api/workers/register and /api/v1/workers/register
// using the Go worker-agent payload format.
func (wl *WorkerLifecycle) RegisterCompatHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID     string                 `json:"worker_id"`
			WorkerName   string                 `json:"worker_name"`
			Hostname     string                 `json:"hostname"`
			IP           string                 `json:"ip"`
			Version      string                 `json:"version"`
			Capabilities map[string]bool        `json:"capabilities"`
			Extra        map[string]interface{} `json:"extra"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "invalid JSON body"})
			return
		}

		if body.WorkerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "worker_id required"})
			return
		}

		if wl.persistedReg != nil && wl.persistedReg.IsRevoked(body.WorkerID) {
			c.Status(http.StatusNoContent)
			return
		}

		workerName := strings.TrimSpace(body.WorkerName)
		if workerName == "" {
			workerName = strings.TrimSpace(body.Hostname)
		}
		if workerName == "" {
			workerName = body.WorkerID
		}

		ipAddress := strings.TrimSpace(body.IP)
		if ipAddress == "" {
			ipAddress = c.ClientIP()
		}

		extra := body.Extra
		if extra == nil {
			extra = make(map[string]interface{})
		}
		if body.Version != "" {
			extra["code_version"] = body.Version
		}
		if body.Hostname != "" {
			extra["host"] = body.Hostname
		}
		if body.Capabilities != nil {
			extra["capabilities"] = body.Capabilities
		}

		ctx := c.Request.Context()
		if err := wl.reg.RegisterWorker(ctx, body.WorkerID, workerName, ipAddress, extra); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"success": false, "error": "registration failed"})
			return
		}

		if wl.persistedReg != nil {
			wl.persistedReg.Register(body.WorkerID, workerName, ipAddress)
		}

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"message": "Worker registered",
			"data": gin.H{
				"worker_id":   body.WorkerID,
				"worker_name": workerName,
			},
		})
	}
}

// UnregisterCompatHandler handles POST /api/workers/unregister and /api/v1/workers/unregister.
func (wl *WorkerLifecycle) UnregisterCompatHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID string `json:"worker_id"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "invalid JSON body"})
			return
		}

		if body.WorkerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "worker_id required"})
			return
		}
		if !wl.authorizeWorkerRequest(c, body.WorkerID) {
			return
		}

		_ = wl.reg.UnregisterWorker(c.Request.Context(), body.WorkerID)

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"message": "Worker unregistered",
		})
	}
}

// HeartbeatCompatHandler handles POST /api/workers/heartbeat with Go worker-agent payload.
func (wl *WorkerLifecycle) HeartbeatCompatHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID   string                 `json:"worker_id"`
			WorkerName string                 `json:"worker_name"`
			Status     string                 `json:"status"`
			CurrentJob string                 `json:"current_job"`
			JobID      string                 `json:"job_id"`
			Extra      map[string]interface{} `json:"extra"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "invalid JSON body"})
			return
		}

		if body.WorkerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "missing worker_id"})
			return
		}
		if !wl.authorizeWorkerRequest(c, body.WorkerID) {
			return
		}
		if body.Status == "" {
			body.Status = "online"
		}
		currentJob := body.CurrentJob
		if currentJob == "" {
			currentJob = body.JobID
		}

		if err := wl.reg.Heartbeat(c.Request.Context(), body.WorkerID, body.WorkerName, body.Status, currentJob, body.Extra); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"success": false, "error": "heartbeat failed"})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"message": "heartbeat ok",
		})
	}
}

// GetCommandsCompatHandler handles GET /api/workers/commands and /api/v1/workers/commands.
func (wl *WorkerLifecycle) GetCommandsCompatHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		workerID := c.Query("worker_id")
		if workerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "worker_id required"})
			return
		}
		if !wl.authorizeWorkerRequest(c, workerID) {
			return
		}

		pending := wl.cmdMgr.GetPendingCommands(workerID)
		commands := make([]gin.H, 0, len(pending))
		for _, cmd := range pending {
			commands = append(commands, gin.H{
				"command":   cmd.Command,
				"timestamp": cmd.Timestamp,
				"payload":   cmd.Params,
			})
		}

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"data":    commands,
		})
	}
}

// AckCommandCompatHandler handles POST /api/workers/commands/ack and /api/v1/workers/commands/ack.
func (wl *WorkerLifecycle) AckCommandCompatHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID string `json:"worker_id"`
			Command  string `json:"command"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "invalid JSON body"})
			return
		}
		if body.WorkerID == "" || body.Command == "" {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "worker_id and command required"})
			return
		}
		if !wl.authorizeWorkerRequest(c, body.WorkerID) {
			return
		}

		wl.cmdMgr.AckCommand(body.WorkerID, body.Command)
		c.JSON(http.StatusOK, gin.H{"success": true, "message": "acknowledged"})
	}
}

// UpdateStatusCompatHandler handles POST /api/workers/status and /api/v1/workers/status.
func (wl *WorkerLifecycle) UpdateStatusCompatHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID string                 `json:"worker_id"`
			Status   string                 `json:"status"`
			Details  map[string]interface{} `json:"details"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "invalid JSON body"})
			return
		}
		if body.WorkerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "worker_id required"})
			return
		}
		if !wl.authorizeWorkerRequest(c, body.WorkerID) {
			return
		}

		log.Printf("📡 Worker status update: worker=%s status=%s details=%v", body.WorkerID, body.Status, body.Details)

		// Persist command status into showlog buffers for immediate operator feedback.
		existing := wl.reg.GetWorker(c.Request.Context(), body.WorkerID)
		recentLogs := []string{}
		recentErrors := []string{}
		if existing != nil {
			recentLogs = append(recentLogs, existing.RecentLogs...)
			recentErrors = append(recentErrors, existing.RecentErrors...)
		}
		line := fmt.Sprintf("[%s] status=%s details=%v", time.Now().UTC().Format(time.RFC3339), body.Status, body.Details)
		recentLogs = append(recentLogs, line)
		if len(recentLogs) > 300 {
			recentLogs = recentLogs[len(recentLogs)-300:]
		}
		if body.Status == "command_failed" {
			recentErrors = append(recentErrors, line)
			if len(recentErrors) > 120 {
				recentErrors = recentErrors[len(recentErrors)-120:]
			}
		}
		_ = wl.reg.UpdateWorker(c.Request.Context(), body.WorkerID, map[string]interface{}{
			"recent_logs":   recentLogs,
			"recent_errors": recentErrors,
		})

		c.JSON(http.StatusOK, gin.H{"success": true, "message": "status updated"})
	}
}

// WorkerHelloHandler handles POST /api/worker/hello
func (wl *WorkerLifecycle) WorkerHelloHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID      string                 `json:"worker_id"`
			WorkerName    string                 `json:"worker_name"`
			BundleVersion string                 `json:"bundle_version"`
			BuildHash     string                 `json:"build_hash"`
			PID           int                    `json:"pid"`
			StartTime     string                 `json:"start_time"`
			Capabilities  map[string]interface{} `json:"capabilities"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
			return
		}

		if body.WorkerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id required"})
			return
		}

		// Check if worker is revoked
		if wl.persistedReg != nil && wl.persistedReg.IsRevoked(body.WorkerID) {
			c.JSON(http.StatusForbidden, gin.H{
				"status": "banned",
				"reason": "Worker revoked",
			})
			return
		}

		// Generate token
		token := wl.tokenMgr.GenerateToken(body.WorkerID)

		log.Printf("👋 Handshake worker: %s (%s) bundle=%s",
			body.WorkerName,
			body.WorkerID[:min(16, len(body.WorkerID))]+"...",
			body.BundleVersion)

		c.JSON(http.StatusOK, gin.H{
			"status":              "ok",
			"token":               token,
			"token_expires_in":    3600,
			"bundle_download_url": "/api/worker/bundle?platform=linux&arch=x86_64",
		})
	}
}

// WorkerCommandHandler handles GET /worker/command
func (wl *WorkerLifecycle) WorkerCommandHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		workerID := c.Query("worker_id")
		if workerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id required"})
			return
		}
		if !wl.authorizeWorkerRequest(c, workerID) {
			return
		}

		commands := wl.cmdMgr.GetPendingCommands(workerID)

		c.JSON(http.StatusOK, gin.H{
			"commands": commands,
		})
	}
}

// WorkerCommandAckHandler handles POST /worker/command_ack
func (wl *WorkerLifecycle) WorkerCommandAckHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID string `json:"worker_id"`
			Command  string `json:"command"`
			Error    string `json:"error"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
			return
		}

		if body.WorkerID == "" || body.Command == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id and command required"})
			return
		}
		if !wl.authorizeWorkerRequest(c, body.WorkerID) {
			return
		}

		wl.cmdMgr.AckCommand(body.WorkerID, body.Command)

		// Handle update acknowledgment
		if body.Command == "update_code" && body.Error == "" {
			wl.updateMgr.AckUpdate(body.WorkerID, wl.codeVersion)
		}

		c.JSON(http.StatusOK, gin.H{"ok": true})
	}
}

// HeartbeatHandler handles POST /workers/heartbeat (enhanced)
func (wl *WorkerLifecycle) HeartbeatHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID         string                 `json:"worker_id"`
			WorkerName       string                 `json:"worker_name"`
			Status           string                 `json:"status"`
			CurrentJob       string                 `json:"current_job"`
			CodeVersion      string                 `json:"code_version"`
			BundleVersion    string                 `json:"bundle_version"`
			Metrics          map[string]interface{} `json:"metrics"`
			RecentLogs       []string               `json:"recent_logs"`
			RecentErrors     []string               `json:"recent_errors"`
			Readiness        map[string]interface{} `json:"readiness"`
			ConnectionStatus string                 `json:"connection_status"`
			Extra            map[string]interface{} `json:"extra"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid JSON body"})
			return
		}

		if body.WorkerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "missing worker_id"})
			return
		}
		if !wl.authorizeWorkerRequest(c, body.WorkerID) {
			return
		}

		status := body.Status
		if status == "" {
			status = "online"
		}

		// Build extra map
		extra := body.Extra
		if extra == nil {
			extra = make(map[string]interface{})
		}
		if body.CodeVersion != "" {
			extra["code_version"] = body.CodeVersion
		}
		if body.BundleVersion != "" {
			extra["bundle_version"] = body.BundleVersion
		}
		if len(body.RecentLogs) > 0 {
			extra["recent_logs"] = body.RecentLogs
		}
		if len(body.RecentErrors) > 0 {
			extra["recent_errors"] = body.RecentErrors
		}
		if body.Readiness != nil {
			extra["readiness"] = body.Readiness
		}
		if body.Metrics != nil {
			extra["metrics"] = body.Metrics
		}

		ctx := c.Request.Context()
		if err := wl.reg.Heartbeat(ctx, body.WorkerID, body.WorkerName, status, body.CurrentJob, extra); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "heartbeat failed"})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"ok":        true,
			"message":   "success",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		})
	}
}

// RequestUpdateHandler handles POST /worker/request_update
func (wl *WorkerLifecycle) RequestUpdateHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID string `json:"worker_id"`
			Version  string `json:"version"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
			return
		}

		if body.WorkerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id required"})
			return
		}
		if !wl.authorizeWorkerRequest(c, body.WorkerID) {
			return
		}

		version := body.Version
		if version == "" {
			version = wl.codeVersion
		}

		// Schedule update command
		wl.cmdMgr.PushCommand(body.WorkerID, "update_code", map[string]interface{}{
			"version": version,
		})
		wl.updateMgr.RequestUpdate(body.WorkerID, version)

		log.Printf("📤 Update requested for worker %s (version: %s)", body.WorkerID[:min(16, len(body.WorkerID))]+"...", version)

		c.JSON(http.StatusOK, gin.H{
			"ok":      true,
			"message": "Update scheduled",
			"version": version,
		})
	}
}

// RestartWorkerHandler handles POST /worker/restart
func (wl *WorkerLifecycle) RestartWorkerHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID string `json:"worker_id"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
			return
		}

		if body.WorkerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id required"})
			return
		}

		// Push restart command
		wl.cmdMgr.PushCommand(body.WorkerID, "restart_worker", nil)

		log.Printf("🔄 Restart requested for worker %s", body.WorkerID[:min(16, len(body.WorkerID))]+"...")

		c.JSON(http.StatusOK, gin.H{
			"ok":      true,
			"message": "Restart scheduled",
		})
	}
}

// RevokeWorkerHandler handles POST /worker/revoke
func (wl *WorkerLifecycle) RevokeWorkerHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID string `json:"worker_id"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
			return
		}

		if body.WorkerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id required"})
			return
		}

		// Revoke worker
		if wl.persistedReg != nil {
			wl.persistedReg.RevokeWorker(body.WorkerID)
		}

		// Also unregister from memory
		ctx := c.Request.Context()
		wl.reg.UnregisterWorker(ctx, body.WorkerID)

		log.Printf("🚫 Worker revoked: %s", body.WorkerID[:min(16, len(body.WorkerID))]+"...")

		c.JSON(http.StatusOK, gin.H{
			"ok":      true,
			"message": "Worker revoked",
		})
	}
}

// UnrevokeWorkerHandler handles POST /worker/unrevoke
func (wl *WorkerLifecycle) UnrevokeWorkerHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID string `json:"worker_id"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
			return
		}

		if body.WorkerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id required"})
			return
		}

		// Unrevoke worker
		if wl.persistedReg != nil {
			wl.persistedReg.UnrevokeWorker(body.WorkerID)
		}

		log.Printf("✅ Worker unrevoked: %s", body.WorkerID[:min(16, len(body.WorkerID))]+"...")

		c.JSON(http.StatusOK, gin.H{
			"ok":      true,
			"message": "Worker unrevoked",
		})
	}
}

// DrainWorkerHandler handles POST /worker/drain
func (wl *WorkerLifecycle) DrainWorkerHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID string `json:"worker_id"`
			Drain    bool   `json:"drain"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
			return
		}

		if body.WorkerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id required"})
			return
		}

		ctx := c.Request.Context()
		if err := wl.reg.SetWorkerDrain(ctx, body.WorkerID, body.Drain); err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "worker not found"})
			return
		}

		action := "drain"
		if !body.Drain {
			action = "undrain"
		}
		log.Printf("🔧 Worker %s set to %s", body.WorkerID[:min(16, len(body.WorkerID))]+"...", action)

		c.JSON(http.StatusOK, gin.H{
			"ok":      true,
			"drain":   body.Drain,
			"message": "Worker drain status updated",
		})
	}
}

// GetWorkerDetailsHandler handles GET /worker/:id
func (wl *WorkerLifecycle) GetWorkerDetailsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		workerID := c.Param("id")
		if workerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id required"})
			return
		}

		ctx := c.Request.Context()
		worker := wl.reg.GetWorker(ctx, workerID)
		if worker == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "worker not found"})
			return
		}

		c.JSON(http.StatusOK, worker)
	}
}

// CleanupStaleWorkersHandler handles POST /workers/cleanup
func (wl *WorkerLifecycle) CleanupStaleWorkersHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			MaxAgeMinutes int `json:"max_age_minutes"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			body.MaxAgeMinutes = 30 // Default 30 minutes
		}

		maxAge := time.Duration(body.MaxAgeMinutes) * time.Minute
		if maxAge <= 0 {
			maxAge = 30 * time.Minute
		}

		ctx := c.Request.Context()
		count := wl.reg.CleanupStaleWorkers(ctx, maxAge)

		c.JSON(http.StatusOK, gin.H{
			"ok":      true,
			"removed": count,
			"message": "Stale workers cleaned up",
		})
	}
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// GetCommandManager returns the command manager for worker updates
func (wl *WorkerLifecycle) GetCommandManager() *workersreg.CommandManager {
	return wl.cmdMgr
}

// GetUpdateManager returns the update manager for worker updates
func (wl *WorkerLifecycle) GetUpdateManager() *workersreg.UpdateManager {
	return wl.updateMgr
}

// GetTokenManager returns the worker token manager used for worker auth.
func (wl *WorkerLifecycle) GetTokenManager() *workersreg.TokenManager {
	return wl.tokenMgr
}

// Config returns the runtime config used by the worker lifecycle handlers.
func (wl *WorkerLifecycle) Config() *config.Config {
	return wl.cfg
}
