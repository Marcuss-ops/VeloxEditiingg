package workers

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gin-gonic/gin"
)

// GenerateManifestV2 regenerates manifest_v2.json programmatically (non-HTTP).
// Called at server startup to ensure manifest is always fresh.
func (h *WorkerUpdateHandler) GenerateManifestV2() error {
	bundlePath, _, err := resolveBundlePath(h.bundleDir, "linux", "x86_64")
	if err != nil {
		bundlePath = filepath.Join(h.bundleDir, "worker_code.zip")
		if _, statErr := os.Stat(bundlePath); statErr != nil {
			return fmt.Errorf("bundle not found in %s", h.bundleDir)
		}
	}

	actualSHA := computeFileSHA256(bundlePath)
	now := time.Now().UTC().Format(time.RFC3339)

	version := h.cfg.Workers.VersionNumber
	if version == "" {
		version = h.codeVersion
	}
	if version == "" {
		version = "unknown"
	}

	manifest := map[string]interface{}{
		"version":          version,
		"code_version":     h.codeVersion,
		"bundle_version":   version,
		"build_hash":       actualSHA,
		"bundle_hash":      actualSHA,
		"protocol_version": "v3",
		"engine_version":   version,
		"platform":         "linux",
		"arch":             "x86_64",
		"timestamp":        now,
		"generated_at":     now,
	}

	manifestPath := filepath.Join(h.bundleDir, "manifest_v2.json")
	raw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}

	tmpPath := manifestPath + ".tmp"
	if err := os.WriteFile(tmpPath, raw, 0644); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	if err := os.Rename(tmpPath, manifestPath); err != nil {
		return fmt.Errorf("rename manifest: %w", err)
	}

	log.Printf("[MANIFEST] Auto-generated manifest_v2.json: version=%s build_hash=%s",
		version, actualSHA[:min(16, len(actualSHA))]+"...")
	return nil
}

// GenerateManifestV2Handler handles POST /bundle/manifest/generate
// It regenerates manifest_v2.json in the bundle directory from the current
// config values (version, code_version) and the actual bundle SHA256.
// This is useful after a config update or bundle rebuild to keep the
// manifest consistent with the running master.
func (h *WorkerUpdateHandler) GenerateManifestV2Handler() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Locate bundle file to compute actual SHA256
		bundlePath, _, err := resolveBundlePath(h.bundleDir, "linux", "x86_64")
		if err != nil {
			// Try finding any bundle as fallback
			bundlePath = filepath.Join(h.bundleDir, "worker_code.zip")
			if _, statErr := os.Stat(bundlePath); statErr != nil {
				c.JSON(http.StatusNotFound, gin.H{
					"error": "Bundle not found, cannot generate manifest",
					"path":  h.bundleDir,
				})
				return
			}
		}

		actualSHA := computeFileSHA256(bundlePath)
		now := time.Now().UTC().Format(time.RFC3339)

		// Determine version from config
		version := h.cfg.Workers.VersionNumber
		if version == "" {
			version = h.codeVersion
		}
		if version == "" {
			version = "unknown"
		}

		// Build manifest payload
		manifest := map[string]interface{}{
			"version":          version,
			"code_version":     h.codeVersion,
			"bundle_version":   version,
			"build_hash":       actualSHA,
			"bundle_hash":      actualSHA,
			"protocol_version": "v3",
			"engine_version":   version,
			"platform":         "linux",
			"arch":             "x86_64",
			"timestamp":        now,
			"generated_at":     now,
		}

		// Write manifest_v2.json atomically
		manifestPath := filepath.Join(h.bundleDir, "manifest_v2.json")
		raw, err := json.MarshalIndent(manifest, "", "  ")
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "failed to marshal manifest",
			})
			return
		}

		tmpPath := manifestPath + ".tmp"
		if err := os.WriteFile(tmpPath, raw, 0644); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "failed to write manifest: " + err.Error(),
			})
			return
		}
		if err := os.Rename(tmpPath, manifestPath); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "failed to rename manifest: " + err.Error(),
			})
			return
		}

		log.Printf("[MANIFEST] Regenerated manifest_v2.json: version=%s build_hash=%s",
			version, actualSHA[:min(16, len(actualSHA))]+"...")

		c.JSON(http.StatusOK, gin.H{
			"ok":              true,
			"message":         "Manifest regenerated",
			"path":            manifestPath,
			"version":         version,
			"code_version":    h.codeVersion,
			"build_hash":      actualSHA,
			"bundle_path":     bundlePath,
			"bundle_basename": filepath.Base(bundlePath),
		})
	}
}
