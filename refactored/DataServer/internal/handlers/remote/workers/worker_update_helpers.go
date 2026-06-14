package workers

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type updateAllRequest struct {
	ExcludeLocal *bool `json:"exclude_local"`
	DryRun       *bool `json:"dry_run"`
}

type bundleTargetInfo struct {
	Version   string
	Hash      string
	Filename  string
	UpdatedAt string
	Available bool
}

func (h *WorkerUpdateHandler) readUpdateAllOptions(c *gin.Context) (excludeLocal bool, dryRun bool) {
	excludeLocal = c.Query("exclude_local") != "false"
	dryRun = c.Query("dry_run") == "true"

	if c.Request != nil && c.Request.ContentLength != 0 {
		var body updateAllRequest
		if err := c.ShouldBindJSON(&body); err == nil {
			if body.ExcludeLocal != nil {
				excludeLocal = *body.ExcludeLocal
			}
			if body.DryRun != nil {
				dryRun = *body.DryRun
			}
		}
	}

	return excludeLocal, dryRun
}

func (h *WorkerUpdateHandler) latestBundleTarget() bundleTargetInfo {
	info := bundleTargetInfo{
		Version:   h.codeVersion,
		Available: false,
	}
	if h == nil {
		return info
	}

	if bundlePath, stat, err := resolveBundlePath(h.bundleDir, "linux", "x86_64"); err == nil {
		info.Hash = computeFileSHA256(bundlePath)
		info.Filename = filepath.Base(bundlePath)
		info.UpdatedAt = stat.ModTime().UTC().Format(time.RFC3339)
		info.Available = true
	}

	manifestPaths := []string{
		filepath.Join(h.bundleDir, "manifest_v2.json"),
		filepath.Join(h.bundleDir, "release.json"),
		filepath.Join(h.bundleDir, "VERSION.txt"),
	}
	for _, manifestPath := range manifestPaths {
		raw, err := os.ReadFile(manifestPath)
		if err != nil {
			continue
		}
		trimmed := strings.TrimSpace(string(raw))
		if trimmed == "" {
			continue
		}
		if strings.HasSuffix(manifestPath, "VERSION.txt") {
			info.Version = trimmed
			break
		}
		var payload map[string]interface{}
		if err := json.Unmarshal(raw, &payload); err != nil {
			continue
		}
		if v, ok := payload["version"].(string); ok && strings.TrimSpace(v) != "" {
			info.Version = strings.TrimSpace(v)
		}
		if info.Version == "" {
			if v, ok := payload["code_version"].(string); ok && strings.TrimSpace(v) != "" {
				info.Version = strings.TrimSpace(v)
			}
		}
		if info.Hash == "" {
			if v, ok := payload["build_hash"].(string); ok && strings.TrimSpace(v) != "" {
				info.Hash = strings.TrimSpace(v)
			}
		}
		break
	}

	if info.Version == "" {
		info.Version = h.cfg.VersionNumber
	}
	if info.Version == "" {
		info.Version = h.codeVersion
	}
	return info
}

func (h *WorkerUpdateHandler) queueBundleUpdateForWorkers(workerIDs []string, target bundleTargetInfo, dryRun bool, maintenanceID string) int {
	commandsQueued := 0
	for _, wid := range workerIDs {
		h.cmdMgr.PushCommand(wid, "maintenance_full_update_linux", map[string]interface{}{
			"id":        maintenanceID,
			"dry_run":   dryRun,
			"requested": time.Now().Unix(),
		})
		commandsQueued++

		h.cmdMgr.PushCommand(wid, "update_code", map[string]interface{}{
			"version":                target.Version,
			"bundle_version":         target.Version,
			"bundle_hash":            target.Hash,
			"target_artifact_sha256": target.Hash,
		})
		h.updateMgr.RequestUpdate(wid, target.Version)
		commandsQueued++

		h.cmdMgr.PushCommand(wid, "restart_worker", nil)
		commandsQueued++

		h.cmdMgr.PushCommand(wid, "run_smoke_job", buildSmokeJobPayload(wid))
		commandsQueued++
	}
	return commandsQueued
}
