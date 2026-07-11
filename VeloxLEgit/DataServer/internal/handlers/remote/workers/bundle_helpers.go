package workers

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// computeBundleSHA256 computes SHA256 of the worker bundle
func (h *WorkerUpdateHandler) computeBundleSHA256() string {
	return h.ComputeBundleSHA256()
}

// ComputeBundleSHA256 computes SHA256 of the worker bundle (exported).
func (h *WorkerUpdateHandler) ComputeBundleSHA256() string {
	if h == nil {
		return ""
	}
	if bundlePath, _, err := resolveBundlePath(h.bundleDir, "linux", "x86_64"); err == nil {
		return computeFileSHA256(bundlePath)
	}
	if hash := computeBundleHashFromManifest(h.bundleDir); hash != "" {
		return hash
	}
	return ""
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

func computeBundleHashFromManifest(bundleDir string) string {
	manifestPaths := []string{
		filepath.Join(bundleDir, "manifest_v2.json"),
		filepath.Join(bundleDir, "release.json"),
		filepath.Join(bundleDir, "source_hash.txt"),
		filepath.Join(bundleDir, "VERSION.txt"),
	}
	for _, path := range manifestPaths {
		raw, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		trimmed := strings.TrimSpace(string(raw))
		if trimmed == "" {
			continue
		}
		if strings.HasSuffix(path, ".txt") {
			return trimmed
		}
		var payload map[string]interface{}
		if err := json.Unmarshal(raw, &payload); err != nil {
			continue
		}
		for _, key := range []string{"build_hash", "bundle_hash", "sha256", "source_hash"} {
			if v, ok := payload[key].(string); ok && strings.TrimSpace(v) != "" {
				return strings.TrimSpace(v)
			}
		}
	}
	return ""
}
