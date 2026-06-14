package workers

import (
	"log"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"velox-server/internal/config"
	workersreg "velox-server/internal/workers"
)

type WorkerLifecycle struct {
	cfg           *config.Config
	reg           *workersreg.Registry
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

func NewWorkerLifecycle(cfg *config.Config, reg *workersreg.Registry, dataDir string) *WorkerLifecycle {
	return &WorkerLifecycle{
		cfg:           cfg,
		reg:           reg,
		cmdMgr:        workersreg.NewCommandManager(),
		updateMgr:     workersreg.NewUpdateManager(),
		tokenMgr:      workersreg.NewTokenManager(),
		codeVersion:   cfg.CodeVersion,
		versionNumber: cfg.VersionNumber,
	}
}

func (wl *WorkerLifecycle) RegisterV2Handler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID        string                 `json:"worker_id"`
			WorkerName      string                 `json:"worker_name"`
			APIVersion      string                 `json:"api_version"`
			IPAddress       string                 `json:"ip_address"`
			Host            string                 `json:"host"`
			CodeVersion     string                 `json:"code_version"`
			BundleVersion   string                 `json:"bundle_version"`
			BundleHash      string                 `json:"bundle_hash"`
			ProtocolVersion string                 `json:"protocol_version"`
			EngineVersion   string                 `json:"engine_version"`
			Capabilities    map[string]interface{} `json:"capabilities"`
			Extra           map[string]interface{} `json:"extra"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
			return
		}

		if body.WorkerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id required"})
			return
		}

		if body.APIVersion != "2.0" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "api_version required: 2.0 (received: " + body.APIVersion + "). Update the worker.",
			})
			return
		}

		if wl.reg.IsRevoked(body.WorkerID) {
			c.Status(http.StatusNoContent)
			return
		}

		ipAddress := body.IPAddress
		if ipAddress == "" {
			ipAddress = body.Host
		}
		if ipAddress == "" {
			ipAddress = c.ClientIP()
		}

		workerName := strings.TrimSpace(body.WorkerName)
		if workerName == "" {
			workerName = body.WorkerID
		}

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
		if body.BundleHash != "" {
			extra["bundle_hash"] = body.BundleHash
		}
		if body.ProtocolVersion != "" {
			extra["protocol_version"] = body.ProtocolVersion
		}
		if body.EngineVersion != "" {
			extra["engine_version"] = body.EngineVersion
		}
		if body.Capabilities != nil {
			extra["capabilities"] = body.Capabilities
		}

		ctx := c.Request.Context()
		if err := wl.reg.RegisterWorker(ctx, body.WorkerID, workerName, ipAddress, extra); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "registration failed"})
			return
		}

		pendingUpdate := wl.updateMgr.GetPendingUpdate(body.WorkerID)
		if pendingUpdate != nil && pendingUpdate.Ack {
			log.Printf("[REGISTER] Worker %s reconnected after update (version: %s)", workerName, pendingUpdate.AckVersion)
		}

		log.Printf("[REGISTER] Worker registered: %s (%s) ip=%s", workerName, body.WorkerID[:min(16, len(body.WorkerID))]+"...", ipAddress)

		c.JSON(http.StatusOK, gin.H{
			"status":      "success",
			"worker_id":   body.WorkerID,
			"worker_name": workerName,
		})
	}
}

func (wl *WorkerLifecycle) RegisterCompatHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID        string                 `json:"worker_id"`
			WorkerName      string                 `json:"worker_name"`
			Hostname        string                 `json:"hostname"`
			IP              string                 `json:"ip"`
			Version         string                 `json:"version"`
			CodeVersion     string                 `json:"code_version"`
			BundleVersion   string                 `json:"bundle_version"`
			BundleHash      string                 `json:"bundle_hash"`
			ProtocolVersion string                 `json:"protocol_version"`
			EngineVersion   string                 `json:"engine_version"`
			Capabilities    map[string]interface{} `json:"capabilities"`
			Extra           map[string]interface{} `json:"extra"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "invalid JSON body"})
			return
		}

		if body.WorkerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "worker_id required"})
			return
		}

		if wl.reg.IsRevoked(body.WorkerID) {
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
		if body.CodeVersion != "" {
			extra["code_version"] = body.CodeVersion
		}
		if body.BundleVersion != "" {
			extra["bundle_version"] = body.BundleVersion
		}
		if body.BundleHash != "" {
			extra["bundle_hash"] = body.BundleHash
		}
		if body.ProtocolVersion != "" {
			extra["protocol_version"] = body.ProtocolVersion
		}
		if body.EngineVersion != "" {
			extra["engine_version"] = body.EngineVersion
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

		// Generate a token for the worker for subsequent authenticated requests
		token := wl.tokenMgr.GenerateToken(body.WorkerID)

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"message": "Worker registered",
			"token":   token,
			"data": gin.H{
				"worker_id":   body.WorkerID,
				"worker_name": workerName,
			},
		})
	}
}

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

		if wl.reg.IsRevoked(body.WorkerID) {
			c.JSON(http.StatusForbidden, gin.H{
				"status": "banned",
				"reason": "Worker revoked",
			})
			return
		}

		token := wl.tokenMgr.GenerateToken(body.WorkerID)

		log.Printf("[REGISTER] Handshake worker: %s (%s) bundle=%s",
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

func (wl *WorkerLifecycle) GetCommandManager() *workersreg.CommandManager {
	return wl.cmdMgr
}

func (wl *WorkerLifecycle) GetUpdateManager() *workersreg.UpdateManager {
	return wl.updateMgr
}

func (wl *WorkerLifecycle) GetTokenManager() *workersreg.TokenManager {
	return wl.tokenMgr
}

func (wl *WorkerLifecycle) Config() *config.Config {
	return wl.cfg
}

