package workers

import (
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
)

// ForceRegenerateZipHandler handles POST /install_worker/force_regenerate_zip
func (h *WorkerUpdateHandler) ForceRegenerateZipHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		wait := c.DefaultQuery("wait", "0") == "1"
		log.Printf("🔍 DEBUG: h.bundleDir = %q", h.bundleDir)
		repoRoot := findRepoRootFrom(h.bundleDir)
		log.Printf("🔍 DEBUG: repoRoot = %q", repoRoot)
		if repoRoot == "" {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "repo root not found for rebuild tool", "bundleDir": h.bundleDir})
			return
		}
		scriptPath := filepath.Join(repoRoot, "DataServer", "cmd", "velox-bundler", "main.go")
		if _, err := os.Stat(scriptPath); err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "velox-bundler entrypoint not found", "path": scriptPath})
			return
		}

		run := func() (string, error) {
			outputDir := h.bundleDir
			cmd := exec.Command("go", "run", "./cmd/velox-bundler", "--source", repoRoot, "--output", outputDir)
			cmd.Dir = filepath.Join(repoRoot, "DataServer")

			out, err := cmd.CombinedOutput()
			if err != nil {
				log.Printf("❌ rebuild bundle failed: %v | %s", err, strings.TrimSpace(string(out)))
				return "", err
			}
			log.Printf("✅ rebuild bundle V2 completed: %s", strings.TrimSpace(string(out)))
			bundlePath, _, err := resolveBundlePath(h.bundleDir, "linux", "x86_64")
			if err != nil {
				return "", err
			}
			return computeFileSHA256(bundlePath), nil
		}

		if wait {
			newHash, err := run()
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "rebuild failed"})
				return
			}
			c.JSON(http.StatusOK, gin.H{
				"ok":              true,
				"message":         "bundle rebuild completed",
				"new_bundle_hash": newHash,
				"script":          scriptPath,
			})
			return
		}

		go func() { _, _ = run() }()
		c.JSON(http.StatusAccepted, gin.H{
			"ok":      true,
			"message": "bundle rebuild started",
			"script":  scriptPath,
		})
	}
}
