package install

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"velox-server/internal/config"

	"github.com/gin-gonic/gin"
)

// Runtime mode constants (RUNTIME_CONTRACT_MINIMALE.md)
// Legacy runtime modes are NOT allowed. Only 'docker' is supported.
const (
	installAllowedRuntimeMode    = "docker"
	installErrRuntimeModeBlocked = "RUNTIME_MODE_NOT_ALLOWED"
)

// validateRuntimeModeQuery checks query parameters for legacy runtime modes.
// Per RUNTIME_CONTRACT_MINIMALE.md, only 'docker' runtime is allowed.
func validateRuntimeModeQuery(c *gin.Context) (blocked bool, requestedMode string) {
	// Check runtime_mode query parameter
	runtimeMode := c.Query("runtime_mode")
	if runtimeMode != "" && runtimeMode != installAllowedRuntimeMode {
		return true, runtimeMode
	}

	// Check runtime query parameter
	runtime := c.Query("runtime")
	if runtime != "" && runtime != installAllowedRuntimeMode {
		return true, runtime
	}

	return false, ""
}

// runtimeModeBlockedResponse sends a standardized error response for blocked runtime modes
func runtimeModeBlockedResponse(c *gin.Context, requestedMode string) {
	log.Printf("[RUNTIME_BLOCKED] endpoint=%s requested_mode=%s client_ip=%s",
		c.Request.URL.Path, requestedMode, c.ClientIP())
	c.JSON(http.StatusBadRequest, gin.H{
		"error":          fmt.Sprintf("Runtime mode '%s' is not allowed. Only 'docker' runtime is supported.", requestedMode),
		"code":           installErrRuntimeModeBlocked,
		"requested_mode": requestedMode,
		"allowed_mode":   installAllowedRuntimeMode,
	})
}

// InstallHandler handles worker installation endpoints.
type InstallHandler struct {
	scriptDir    string         // Directory containing scripts (typically RemoteCodex)
	bundleRoot   string         // Root directory for bundles
	masterURL    string         // Master URL for generated scripts (fallback)
	allowlistIPs []string       // Optional IP allowlist
	cfg          *config.Config // Application config for master URL resolution
}

// Config returns the app config associated with the install handler.
func (h *InstallHandler) Config() *config.Config {
	return h.cfg
}

// Config holds configuration for InstallHandler
type Config struct {
	ScriptDir    string
	BundleRoot   string
	MasterURL    string
	AllowlistIPs []string
}

// NewInstallHandler creates a new install handler
// The master URL is resolved using the centralized resolver in ansible package.
// Priority: MASTER_PUBLIC_URL env > VELOX_MASTER_URL env > MASTER_URL env > hardcoded fallback
func NewInstallHandler(cfg *Config) *InstallHandler {
	// MasterURL will be resolved dynamically using the centralized resolver
	// Keep a fallback for backward compatibility
	masterURL := cfg.MasterURL
	if masterURL == "" {
		masterURL = os.Getenv("MASTER_PUBLIC_URL")
	}
	if masterURL == "" {
		masterURL = os.Getenv("VELOX_MASTER_URL")
	}
	if masterURL == "" {
		masterURL = os.Getenv("MASTER_URL")
	}
	// Only use hardcoded fallback in development mode
	if masterURL == "" {
		masterURL = "http://127.0.0.1:8000" // Dev fallback
		log.Printf("[WARN] [INSTALL] No MASTER_PUBLIC_URL set, using dev fallback: %s", masterURL)
	}

	// If BundleRoot not set, derive from ScriptDir
	if cfg.BundleRoot == "" && cfg.ScriptDir != "" {
		cfg.BundleRoot = cfg.ScriptDir
	}

	return &InstallHandler{
		scriptDir:    cfg.ScriptDir,
		bundleRoot:   cfg.BundleRoot,
		masterURL:    masterURL,
		allowlistIPs: cfg.AllowlistIPs,
		cfg:          nil, // Will be set via SetConfig if needed
	}
}

// SetConfig sets the application config for dynamic master URL resolution
func (h *InstallHandler) SetConfig(appCfg *config.Config) {
	h.cfg = appCfg
}

// checkAllowlist verifies if client IP is allowed
func (h *InstallHandler) checkAllowlist(c *gin.Context) bool {
	if len(h.allowlistIPs) == 0 {
		return true // No allowlist = all allowed
	}

	clientIP := c.ClientIP()
	for _, allowed := range h.allowlistIPs {
		if clientIP == allowed {
			return true
		}
	}

	log.Printf("[SECURITY] Access denied for IP %s (not in allowlist)", clientIP)
	return false
}

// DownloadInstallScript handles GET /install_worker/download_install_script
// DownloadInstallScript handles GET /install_worker/download_install_script
// Sends the compiled Go installer (velox-installer)
func (h *InstallHandler) DownloadInstallScript() gin.HandlerFunc {
	return func(c *gin.Context) {
		log.Printf("[DOWNLOAD] Richiesta download install script from %s", c.ClientIP())

		// RUNTIME_CONTRACT_MINIMALE.md: Validate runtime mode from query params
		if blocked, mode := validateRuntimeModeQuery(c); blocked {
			runtimeModeBlockedResponse(c, mode)
			return
		}

		// Check allowlist
		if !h.checkAllowlist(c) {
			c.JSON(http.StatusForbidden, gin.H{"error": "Access denied: Worker IP not allowlisted"})
			return
		}

		// Look for velox-installer in candidate locations
		candidates := []string{
			filepath.Join(h.scriptDir, "velox-installer"),
			filepath.Join(h.bundleRoot, "velox-installer"),
		}

		var scriptPath string
		for _, p := range candidates {
			if _, err := os.Stat(p); err == nil {
				scriptPath = p
				break
			}
		}

		if scriptPath == "" {
			log.Printf("[ERROR] velox-installer non trovato (cercato in %v)", candidates)
			c.JSON(http.StatusNotFound, gin.H{
				"error":      "velox-installer non trovato sul server",
				"candidates": candidates,
			})
			return
		}

		log.Printf("[OK] Serving install script from: %s", scriptPath)
		c.FileAttachment(scriptPath, "velox-installer")
	}
}

// DownloadSetupScript handles GET /install_worker/download_setup_script
// Generates and returns a bash/bat script for automatic setup
func (h *InstallHandler) DownloadSetupScript() gin.HandlerFunc {
	return func(c *gin.Context) {
		// RUNTIME_CONTRACT_MINIMALE.md: Validate runtime mode from query params
		if blocked, mode := validateRuntimeModeQuery(c); blocked {
			runtimeModeBlockedResponse(c, mode)
			return
		}

		// Accept both ?platform= and ?os= (alias used in logs)
		platform := c.Query("platform")
		if platform == "" {
			platform = c.Query("os")
		}
		if platform == "" {
			platform = "linux"
		}

		log.Printf("[DOWNLOAD] Richiesta download setup script (platform=%s) from %s", platform, c.ClientIP())

		masterURL := h.masterURL

		if strings.ToLower(platform) == "windows" {
			script := fmt.Sprintf(`@echo off
set MASTER_URL=%s
set SCRIPT_NAME=velox-installer.exe
echo ========================================
echo INSTALLAZIONE AUTOMATICA WORKER
echo ========================================
echo [INFO] Master URL: %%MASTER_URL%%
echo [INFO] Download script di installazione...
curl -s %%MASTER_URL%%/install_worker/download_install_script -o %%SCRIPT_NAME%%
echo [OK] Script scaricato
echo [INFO] Esecuzione installazione automatica...
%%SCRIPT_NAME%% --master-url %%MASTER_URL%%
echo [OK] Installazione completata!
`, masterURL)

			c.Data(http.StatusOK, "text/plain; charset=utf-8", []byte(script))
			c.Header("Content-Disposition", "attachment; filename=INSTALLA_WORKER.bat")
			return
		}

		// Default: Linux/Mac
		script := fmt.Sprintf(`#!/bin/bash
set -e
MASTER_URL="%s"
SCRIPT_NAME="velox-installer"
echo "========================================"
echo "INSTALLAZIONE AUTOMATICA WORKER"
echo "========================================"
echo "[INFO] Master URL: $MASTER_URL"
echo "[INFO] Download installer in corso..."
curl -s "$MASTER_URL/install_worker/download_install_script" -o "$SCRIPT_NAME"
chmod +x "$SCRIPT_NAME"
echo "[OK] Script scaricato"
echo "[INFO] Esecuzione installazione automatica..."
./"$SCRIPT_NAME" --master-url "$MASTER_URL"
echo "[OK] Installazione completata!"
`, masterURL)

		c.Data(http.StatusOK, "text/x-shellscript; charset=utf-8", []byte(script))
		c.Header("Content-Disposition", "attachment; filename=INSTALLA_WORKER.sh")
	}
}
