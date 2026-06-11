package workers

import (
	"archive/zip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
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

		c.FileAttachment(bundlePath, filepath.Base(bundlePath))
	}
}

// formatSize formats bytes to human readable string
func formatSize(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
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

// computeBundleSHA256 computes SHA256 of the worker bundle
func (h *WorkerUpdateHandler) computeBundleSHA256() string {
	bundlePath := filepath.Join(h.bundleDir, "worker_code.zip")

	file, err := os.Open(bundlePath)
	if err != nil {
		return ""
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return ""
	}

	return hex.EncodeToString(hash.Sum(nil))
}

// computeFileSHA256 computes SHA256 of any file path
func computeFileSHA256(path string) string {
	file, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return ""
	}
	return hex.EncodeToString(hash.Sum(nil))
}

// bundleInspection holds result of inspecting zip contents
type bundleInspection struct {
	FileCount int                    `json:"file_count"`
	TopDirs   []gin.H                `json:"top_dirs"`
	Runtime   map[string]interface{} `json:"runtime"`
}

// inspectBundleZip opens the zip and returns file count, top-level dirs, and runtime presence flags.
func inspectBundleZip(bundlePath string) (bundleInspection, error) {
	out := bundleInspection{
		TopDirs: []gin.H{},
		Runtime: map[string]interface{}{
			"node": false, "npm": false,
		},
	}
	r, err := zip.OpenReader(bundlePath)
	if err != nil {
		return out, err
	}
	defer r.Close()

	dirSizes := make(map[string]int64)
	dirCounts := make(map[string]int)

	for _, f := range r.File {
		out.FileCount++
		name := strings.TrimPrefix(filepath.ToSlash(f.Name), "./")
		if name == "" || strings.HasSuffix(name, "/") {
			continue
		}
		parts := strings.SplitN(name, "/", 2)
		top := parts[0]
		dirSizes[top] += int64(f.UncompressedSize64)
		dirCounts[top]++

		lower := strings.ToLower(name)
		if strings.Contains(lower, "runtime/node") || strings.HasPrefix(lower, "node/") || top == "node" {
			out.Runtime["node"] = true
		}
		if strings.Contains(lower, "node_modules") || strings.Contains(lower, "package.json") || top == "npm" {
			out.Runtime["npm"] = true
		}
		if strings.Contains(lower, "voiceover") || strings.Contains(lower, "voices") {
			out.Runtime["voiceover_deps"] = true
		}
		if strings.HasPrefix(lower, "refactored/") || top == "refactored" {
			out.Runtime["refactored_root"] = true
		}
	}

	for name, size := range dirSizes {
		out.TopDirs = append(out.TopDirs, gin.H{
			"name":           name,
			"type":           "folder",
			"size":           size,
			"size_formatted": formatSize(size),
			"file_count":     dirCounts[name],
		})
	}

	return out, nil
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

// GetManifestV2Handler handles GET /api/worker/v2/manifest
func (h *WorkerUpdateHandler) GetManifestV2Handler() gin.HandlerFunc {
	return func(c *gin.Context) {
		manifestPath := filepath.Join(h.bundleDir, "manifest_v2.json")
		if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
			c.JSON(http.StatusNotFound, gin.H{"error": "Manifest V2 not found"})
			return
		}
		c.File(manifestPath)
	}
}

// GetChunkV2Handler handles GET /api/worker/v2/chunk/:chunkName
func (h *WorkerUpdateHandler) GetChunkV2Handler() gin.HandlerFunc {
	return func(c *gin.Context) {
		chunkName := c.Param("chunkName")
		if chunkName == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Chunk name required"})
			return
		}

		if strings.Contains(chunkName, "/") || strings.Contains(chunkName, "\\") {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid chunk name"})
			return
		}

		chunkPath := filepath.Join(h.bundleDir, chunkName)
		stat, err := os.Stat(chunkPath)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "Chunk not found"})
			return
		}

		if stat.IsDir() {
			c.JSON(http.StatusForbidden, gin.H{"error": "Cannot serve directory"})
			return
		}

		file, err := os.Open(chunkPath)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to read chunk"})
			return
		}
		defer file.Close()

		etag := fmt.Sprintf(`"%x-%x"`, stat.Size(), stat.ModTime().UnixNano())
		c.Header("ETag", etag)

		if match := c.GetHeader("If-None-Match"); match == etag {
			c.Status(http.StatusNotModified)
			return
		}

		http.ServeContent(c.Writer, c.Request, filepath.Base(chunkPath), stat.ModTime(), file)
	}
}

func listZipFilesWithHashes(bundlePath string) ([]gin.H, map[string]string, error) {
	r, err := zip.OpenReader(bundlePath)
	if err != nil {
		return nil, nil, err
	}
	defer r.Close()

	type fileEntry struct {
		Name string
		Size int64
		Hash string
		Top  string
	}
	entries := make([]fileEntry, 0, len(r.File))
	for _, f := range r.File {
		if f.FileInfo().IsDir() {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			continue
		}
		h := sha256.New()
		_, _ = io.Copy(h, rc)
		_ = rc.Close()
		sum := hex.EncodeToString(h.Sum(nil))
		name := f.Name
		top := strings.SplitN(strings.TrimLeft(name, "/"), "/", 2)[0]
		entries = append(entries, fileEntry{
			Name: name,
			Size: int64(f.UncompressedSize64),
			Hash: sum,
			Top:  top,
		})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name < entries[j].Name
	})

	dirHash := make(map[string]hash.Hash)
	for _, e := range entries {
		if _, ok := dirHash[e.Top]; !ok {
			dirHash[e.Top] = sha256.New()
		}
		dirHash[e.Top].Write([]byte(e.Name))
		dirHash[e.Top].Write([]byte(e.Hash))
	}

	files := make([]gin.H, 0, len(entries))
	for _, e := range entries {
		files = append(files, gin.H{
			"path":   e.Name,
			"size":   e.Size,
			"sha256": e.Hash,
		})
	}

	dirHashOut := make(map[string]string)
	for k, h := range dirHash {
		dirHashOut[k] = hex.EncodeToString(h.Sum(nil))
	}
	return files, dirHashOut, nil
}
