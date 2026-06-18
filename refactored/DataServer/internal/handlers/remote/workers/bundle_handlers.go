package workers

import (
	"archive/zip"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// GetBundleFilesHandler handles GET /api/bundle/files
func (h *WorkerUpdateHandler) GetBundleFilesHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		platform := c.DefaultQuery("platform", "linux")
		arch := c.DefaultQuery("arch", "x86_64")
		searchPath := c.Query("path")
		if searchPath == "" {
			searchPath = c.Query("prefix")
		}

		// Look for bundle file
		bundleName := fmt.Sprintf("worker_code_%s_%s.zip", platform, arch)
		bundlePath := filepath.Join(h.bundleDir, bundleName)
		if _, err := os.Stat(bundlePath); os.IsNotExist(err) {
			bundlePath = filepath.Join(h.bundleDir, "worker_code.zip")
		}

		r, err := zip.OpenReader(bundlePath)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "Bundle not found or invalid"})
			return
		}
		defer r.Close()

		var results []gin.H
		for _, f := range r.File {
			// If path is provided, only show files in that directory
			if searchPath != "" {
				rel, err := filepath.Rel(searchPath, f.Name)
				if err != nil || rel == ".." || filepath.IsAbs(rel) {
					continue
				}
			}

			if searchPath != "" && !filepath.HasPrefix(f.Name, searchPath) {
				continue
			}

			results = append(results, gin.H{
				"name":           f.Name,
				"size":           f.UncompressedSize64,
				"size_formatted": formatSize(int64(f.UncompressedSize64)),
				"compressed":     f.CompressedSize64,
			})

			if len(results) >= 1000 {
				break
			}
		}

		c.JSON(http.StatusOK, gin.H{
			"files":       results,
			"total_count": len(results),
		})
	}
}

// GetLatestBundleHandler handles GET /install_worker/latest
func (h *WorkerUpdateHandler) GetLatestBundleHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		platform := c.DefaultQuery("platform", "linux")
		arch := c.DefaultQuery("arch", "x86_64")
		bundlePath, info, err := resolveBundlePath(h.bundleDir, platform, arch)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "Bundle not found"})
			return
		}
		bundleHash := computeFileSHA256(bundlePath)
		c.JSON(http.StatusOK, gin.H{
			"bundle_hash":  bundleHash,
			"manifest_url": fmt.Sprintf("/install_worker/manifest/%s?platform=%s&arch=%s", bundleHash, platform, arch),
			"updated_at":   info.ModTime().UTC().Format(time.RFC3339),
			"filename":     filepath.Base(bundlePath),
		})
	}
}

// GetBundleManifestByHashHandler handles GET /install_worker/manifest/{bundle_hash}
func (h *WorkerUpdateHandler) GetBundleManifestByHashHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		platform := c.DefaultQuery("platform", "linux")
		arch := c.DefaultQuery("arch", "x86_64")
		bundleHash := strings.TrimSpace(c.Param("bundle_hash"))
		if bundleHash == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "bundle_hash required"})
			return
		}
		bundlePath, info, err := resolveBundlePath(h.bundleDir, platform, arch)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "Bundle not found"})
			return
		}
		currentHash := computeFileSHA256(bundlePath)
		if currentHash != bundleHash {
			c.JSON(http.StatusNotFound, gin.H{"error": "bundle hash mismatch", "current": currentHash})
			return
		}

		files, dirHashes, err := listZipFilesWithHashes(bundlePath)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read bundle"})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"bundle_hash": bundleHash,
			"platform":    platform,
			"arch":        arch,
			"file_count":  len(files),
			"files":       files,
			"dir_hash":    dirHashes,
			"updated_at":  info.ModTime().UTC().Format(time.RFC3339),
		})
	}
}


