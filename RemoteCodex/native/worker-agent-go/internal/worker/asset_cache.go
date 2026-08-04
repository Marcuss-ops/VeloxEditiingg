package worker

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"velox-worker-agent/internal/telemetry"
)

// assetCacheDir returns the directory where downloaded audio assets are
// cached. Returns the canonical assets/audio subdirectory.
func (w *Worker) assetCacheDir() string {
	if w != nil && w.config != nil {
		if trimmed := strings.TrimSpace(w.config.AssetCacheDir); trimmed != "" {
			return filepath.Join(trimmed, "assets", "audio")
		}
		if trimmed := strings.TrimSpace(w.config.StateDir); trimmed != "" {
			return filepath.Join(trimmed, "asset-cache", "assets", "audio")
		}
		if trimmed := strings.TrimSpace(w.config.WorkDir); trimmed != "" {
			return filepath.Join(trimmed, "asset-cache", "assets", "audio")
		}
	}
	return filepath.Join(os.TempDir(), "velox-worker", "assets", "audio")
}

// assetImageCacheDir returns the directory where downloaded image assets are
// cached. Returns the canonical assets/image subdirectory.
func (w *Worker) assetImageCacheDir() string {
	if w != nil && w.config != nil {
		if trimmed := strings.TrimSpace(w.config.AssetCacheDir); trimmed != "" {
			return filepath.Join(trimmed, "assets", "image")
		}
		if trimmed := strings.TrimSpace(w.config.StateDir); trimmed != "" {
			return filepath.Join(trimmed, "asset-cache", "assets", "image")
		}
		if trimmed := strings.TrimSpace(w.config.WorkDir); trimmed != "" {
			return filepath.Join(trimmed, "asset-cache", "assets", "image")
		}
	}
	return filepath.Join(os.TempDir(), "velox-worker", "assets", "image")
}

// cacheKeyPrefix builds the filesystem-safe cache key from assetID and an
// optional SHA-256 prefix. When sha256Prefix is non-empty, the first 12
// characters are embedded in the filename so different versions of the same
// asset do not collide. Format: <assetID>_<sha12> (when sha256 is set) or
// just <assetID> (legacy, no integrity check possible).
func cacheKeyPrefix(assetID string, sha256Prefix string) string {
	safe := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, assetID)
	if sha256Prefix != "" {
		short := sha256Prefix
		if len(short) > 12 {
			short = short[:12]
		}
		return safe + "_" + short
	}
	return safe
}

// cachedAssetPath returns a previously cached asset path when present.
// A hit is valid only after every supplied integrity constraint passes:
// expectedSizeBytes (when positive) and expectedSHA256 (when non-empty).
// Any invalid entry is removed individually and reported as a miss so the
// caller re-downloads it from the Master.
func cachedAssetPath(cacheDir, assetID string, expectedSHA256 string, expectedSizeBytes int64) (string, error) {
	path, _, err := cachedAssetPathTimed(cacheDir, assetID, expectedSHA256, expectedSizeBytes)
	return path, err
}

func cachedAssetPathTimed(cacheDir, assetID string, expectedSHA256 string, expectedSizeBytes int64) (string, time.Duration, error) {
	// Folder-backed assets may have no expected hash/size in the job payload.
	// In that mode reuse the asset-ID cache entry after a basic regular-file
	// check; the downloader computed the SHA while creating the file.
	if expectedSHA256 == "" || expectedSizeBytes <= 0 {
		prefix := cacheKeyPrefix(assetID, "")
		matches, err := filepath.Glob(filepath.Join(cacheDir, prefix+".*"))
		if err != nil || len(matches) == 0 {
			return "", 0, err
		}
		for _, candidate := range matches {
			info, statErr := os.Stat(candidate)
			if statErr == nil && info.Mode().IsRegular() && info.Size() > 0 {
				return candidate, 0, nil
			}
		}
		return "", 0, nil
	}
	prefix := cacheKeyPrefix(assetID, expectedSHA256)
	matches, err := filepath.Glob(filepath.Join(cacheDir, prefix+".*"))
	if err != nil || len(matches) == 0 {
		// Fall back to legacy cache key (assetID without SHA-256 suffix)
		// when the new key yields no results.
		if expectedSHA256 != "" {
			legacyPrefix := cacheKeyPrefix(assetID, "")
			legacyMatches, legacyErr := filepath.Glob(filepath.Join(cacheDir, legacyPrefix+".*"))
			if legacyErr == nil && len(legacyMatches) > 0 {
				// Legacy cache entry exists but has no SHA-256 guarantee.
				// Treat as cache miss so we re-download with integrity.
				return "", 0, nil
			}
		}
		return "", 0, err
	}
	cachedPath := matches[0]

	info, err := os.Stat(cachedPath)
	if err != nil || !info.Mode().IsRegular() {
		// Remove only this invalid entry; the rest of the cache remains
		// untouched and the caller re-downloads the asset.
		telemetry.GetPrometheusMetrics().RecordCacheEviction("invalid")
		_ = os.Remove(cachedPath)
		return "", 0, nil
	}
	verifyDuration := time.Duration(0)
	if expectedSizeBytes > 0 && info.Size() != expectedSizeBytes {
		telemetry.GetPrometheusMetrics().RecordCacheEviction("invalid")
		_ = os.Remove(cachedPath)
		return "", 0, nil // size mismatch → re-download
	}
	if expectedSHA256 != "" {
		verifyStarted := time.Now()
		actual, err := sha256File(cachedPath)
		verifyDuration = time.Since(verifyStarted)
		telemetry.GetPrometheusMetrics().RecordCacheVerify(verifyDuration)
		if err != nil || actual != expectedSHA256 {
			// A cache hit is valid only after the digest matches. Remove
			// this corrupt entry atomically from the cache namespace before
			// reacquiring it; never clear the entire cache.
			telemetry.GetPrometheusMetrics().RecordCacheEviction("invalid")
			_ = os.Remove(cachedPath)
			return "", verifyDuration, nil // hash mismatch → re-download
		}
	}
	return cachedPath, verifyDuration, nil
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

// sha256File computes the lowercase hex SHA-256 of a file.
func sha256File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// writeVeloxAssetToCache streams a successful response body to a temp file
// inside the cache directory, then atomically renames to the final path.
// Sniffs for HTML on both the Content-Type header and the first 512 bytes
// of the payload to refuse HTML responses from misconfigured upstreams.
// When expectedSHA256 is non-empty, the cached filename embeds the SHA-256
// prefix so different asset versions don't collide.
func writeVeloxAssetToCache(cacheDir, assetID string, expectedSHA256 string, expectedSizeBytes int64, resp *http.Response) (string, int64, time.Duration, error) {
	mediaType := strings.TrimSpace(resp.Header.Get("Content-Type"))
	if idx := strings.Index(mediaType, ";"); idx >= 0 {
		mediaType = strings.TrimSpace(mediaType[:idx])
	}
	if isHTMLMediaType(mediaType) {
		return "", 0, 0, fmt.Errorf("unexpected HTML response while downloading asset")
	}

	reader := bufio.NewReader(resp.Body)
	peek, _ := reader.Peek(512)
	if isHTMLPayload(peek) {
		return "", 0, 0, fmt.Errorf("unexpected HTML response while downloading asset")
	}
	if mediaType == "" {
		mediaType = http.DetectContentType(peek)
	}

	ext := extensionForMediaType(mediaType)
	if ext == "" {
		ext = ".audio"
	}

	prefix := cacheKeyPrefix(assetID, expectedSHA256)
	tmp, err := os.CreateTemp(cacheDir, prefix+"-*")
	if err != nil {
		return "", 0, 0, err
	}
	tmpPath := tmp.Name()
	defer tmp.Close()

	// Tee the download into a SHA-256 hasher so the bytes are still
	// fingerprinted while streaming. The authoritative verification
	// timing below reads the completed file, so it measures the actual
	// integrity check rather than the network transfer.
	hasher := sha256.New()
	teeReader := io.TeeReader(reader, hasher)
	written, err := io.Copy(tmp, teeReader)
	if err != nil {
		_ = os.Remove(tmpPath)
		return "", 0, 0, err
	}
	if written <= 0 {
		_ = os.Remove(tmpPath)
		return "", 0, 0, fmt.Errorf("downloaded asset is empty")
	}
	if err := tmp.Sync(); err != nil {
		_ = os.Remove(tmpPath)
		return "", 0, 0, err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return "", 0, 0, err
	}

	// Verify all supplied integrity metadata before promoting the file.
	verifyStarted := time.Now()
	if expectedSizeBytes > 0 && written != expectedSizeBytes {
		_ = os.Remove(tmpPath)
		return "", 0, time.Since(verifyStarted), fmt.Errorf("downloaded asset size mismatch (got %d, want %d)", written, expectedSizeBytes)
	}
	actualSHA256 := fmt.Sprintf("%x", hasher.Sum(nil))
	if expectedSHA256 != "" {
		actualSHA256, err = sha256File(tmpPath)
		if err != nil {
			_ = os.Remove(tmpPath)
			return "", 0, time.Since(verifyStarted), err
		}
	}
	if expectedSHA256 != "" && actualSHA256 != expectedSHA256 {
		_ = os.Remove(tmpPath)
		return "", 0, time.Since(verifyStarted), fmt.Errorf("downloaded asset SHA-256 mismatch (got %s, want %s)", actualSHA256[:16], expectedSHA256[:16])
	}

	finalPath := filepath.Join(cacheDir, prefix+ext)
	// Rename replaces an existing destination atomically. This is important
	// after a verified cache miss: a corrupt entry must be repaired, not
	// silently retained because the filename already exists.
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return "", 0, time.Since(verifyStarted), err
	}
	return finalPath, written, time.Since(verifyStarted), nil
}

// isHTMLMediaType reports whether a Content-Type looks like HTML.
func isHTMLMediaType(mediaType string) bool {
	normalized := strings.ToLower(strings.TrimSpace(mediaType))
	return normalized == "text/html" || strings.HasPrefix(normalized, "text/html;")
}

// isHTMLPayload inspects the very first bytes of a response for HTML markers
// even when the upstream lies about Content-Type.
func isHTMLPayload(data []byte) bool {
	trimmed := strings.ToLower(strings.TrimSpace(string(data)))
	return strings.HasPrefix(trimmed, "<!doctype html") ||
		strings.HasPrefix(trimmed, "<html") ||
		strings.Contains(trimmed, "<body") ||
		strings.Contains(trimmed, "login")
}

// extensionForMediaType maps a Content-Type to the file extension used on
// disk. Falls back to .mp3/.mp4 for audio/video MIME families.
func extensionForMediaType(mediaType string) string {
	mediaType = strings.TrimSpace(mediaType)
	if mediaType == "" {
		return ""
	}
	if exts, err := mime.ExtensionsByType(mediaType); err == nil && len(exts) > 0 {
		return exts[0]
	}
	switch {
	case strings.HasPrefix(strings.ToLower(mediaType), "audio/"):
		return ".mp3"
	case strings.HasPrefix(strings.ToLower(mediaType), "video/"):
		return ".mp4"
	default:
		return ""
	}
}
