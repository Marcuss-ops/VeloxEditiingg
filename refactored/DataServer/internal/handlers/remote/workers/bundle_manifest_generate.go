package workers

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gin-gonic/gin"
)

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
		version := h.cfg.VersionNumber
		if version == "" {
			version = h.codeVersion
		}
		if version == "" {
			version = "unknown"
		}

		// Build manifest payload
		manifest := map[string]interface{}{
			"version":      version,
			"code_version": h.codeVersion,
			"build_hash":   actualSHA,
			"timestamp":    now,
			"generated_at": now,
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
			"ok":             true,
			"message":        "Manifest regenerated",
			"path":           manifestPath,
			"version":        version,
			"code_version":   h.codeVersion,
			"build_hash":     actualSHA,
			"bundle_path":    bundlePath,
			"bundle_basename": filepath.Base(bundlePath),
		})
	}
}
