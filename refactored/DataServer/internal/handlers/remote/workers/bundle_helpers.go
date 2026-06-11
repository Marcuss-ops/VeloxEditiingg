package workers

import (
	"archive/zip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"
)

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
