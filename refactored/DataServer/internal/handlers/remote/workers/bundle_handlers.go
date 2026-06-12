package workers

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// GetBundleDownloadHandler handles GET /api/worker/bundle
func (h *WorkerUpdateHandler) GetBundleDownloadHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		platform := c.Query("platform")
		arch := c.Query("arch")

		if platform == "" {
			platform = "linux"
		}
		if arch == "" {
			arch = "x86_64"
		}

		// Look for bundle file
		bundleName := fmt.Sprintf("worker_code_%s_%s.zip", platform, arch)
		bundlePath := filepath.Join(h.bundleDir, bundleName)

		// Fallback to generic bundle
		if _, err := os.Stat(bundlePath); os.IsNotExist(err) {
			bundlePath = filepath.Join(h.bundleDir, "worker_code.zip")
		}

		if _, err := os.Stat(bundlePath); os.IsNotExist(err) {
			c.JSON(http.StatusNotFound, gin.H{"error": "Bundle not found"})
			return
		}

		// Include SHA256 checksum in response header for integrity verification
		bundleHash := computeFileSHA256(bundlePath)
		if bundleHash != "" {
			c.Header("X-Bundle-SHA256", bundleHash)
		}

		c.FileAttachment(bundlePath, filepath.Base(bundlePath))
	}
}

// GetBundleFilesHandler handles GET /api/worker/bundle/files
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

// GetBundleManifestHandler handles GET /bundle_manifest.json
func (h *WorkerUpdateHandler) GetBundleManifestHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		platform := c.DefaultQuery("platform", "linux")
		arch := c.DefaultQuery("arch", "x86_64")

		bundleName := fmt.Sprintf("worker_code_%s_%s.zip", platform, arch)
		bundlePath := filepath.Join(h.bundleDir, bundleName)
		if _, err := os.Stat(bundlePath); os.IsNotExist(err) {
			bundlePath = filepath.Join(h.bundleDir, "worker_code.zip")
		}
		info, err := os.Stat(bundlePath)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "Bundle not found"})
			return
		}
		actualSHA := computeFileSHA256(bundlePath)

		manifest := gin.H{
			"version":        h.cfg.VersionNumber,
			"code_version":   h.codeVersion,
			"protocol_version": "2026-06-worker-v1",
			"platform":       platform,
			"arch":           arch,
			"filename":       filepath.Base(bundlePath),
			"url":            fmt.Sprintf("/api/worker/bundle?platform=%s&arch=%s", platform, arch),
			"sha256":         actualSHA,
			"size":           info.Size(),
			"size_formatted": formatSize(info.Size()),
			"updated_at":     info.ModTime().UTC().Format(time.RFC3339),
			"created_at":     info.ModTime().UTC().Format(time.RFC3339),
		}

		if insp, err := inspectBundleZip(bundlePath); err == nil {
			manifest["file_count"] = insp.FileCount
			manifest["top_dirs"] = insp.TopDirs
			manifest["runtime"] = insp.Runtime
		}

		manifestV2Path := filepath.Join(h.bundleDir, "manifest_v2.json")
		if raw, err := os.ReadFile(manifestV2Path); err == nil {
			var v2 map[string]interface{}
			if err := json.Unmarshal(raw, &v2); err == nil {
				if v, ok := v2["version"]; ok {
					manifest["version"] = v
				}
				if v, ok := v2["build_hash"]; ok {
					manifest["build_id"] = v
					manifest["bundle_sha256"] = v
				}
				if v, ok := v2["timestamp"]; ok {
					manifest["created_at"] = v
					manifest["updated_at"] = v
				}
			}
		} else {
			releasePath := filepath.Join(h.bundleDir, "release.json")
			if raw, err := os.ReadFile(releasePath); err == nil {
				var rel map[string]interface{}
				if err := json.Unmarshal(raw, &rel); err == nil {
					if v, ok := rel["version"]; ok {
						manifest["version"] = v
					}
					if v, ok := rel["build_id"]; ok {
						manifest["build_id"] = v
					}
					if v, ok := rel["created_at"]; ok {
						manifest["created_at"] = v
					}
				}
			}
		}

		if currentHash, ok := readTextFileTrim(filepath.Join(h.bundleDir, "source_hash.txt")); ok {
			manifest["source_hash_current"] = currentHash
			if bundleHash, ok2 := manifest["source_hash_bundle"].(string); ok2 && bundleHash != "" && bundleHash != currentHash {
				manifest["bundle_stale"] = true
				manifest["bundle_sha256_actual"] = actualSHA
				manifest["sha256"] = computeStringSHA256Hex("force-mismatch:" + currentHash)
			}
		}

		c.JSON(http.StatusOK, manifest)
	}
}
