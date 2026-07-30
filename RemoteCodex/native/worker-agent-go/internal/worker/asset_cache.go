package worker

import (
	"bufio"
	"crypto/sha256"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// assetCacheDir returns the directory where downloaded audio assets are
// cached. Returns the canonical assets/audio subdirectory.
func (w *Worker) assetCacheDir() string {
	if w != nil && w.config != nil {
		if trimmed := strings.TrimSpace(w.config.AssetCacheDir); trimmed != "" {
			return filepath.Join(trimmed, "assets", "audio")
		}
		if trimmed := strings.TrimSpace(w.config.WorkDir); trimmed != "" {
			return filepath.Join(trimmed, "worker_downloads", "assets", "audio")
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
		if trimmed := strings.TrimSpace(w.config.WorkDir); trimmed != "" {
			return filepath.Join(trimmed, "worker_downloads", "assets", "image")
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
// When expectedSHA256 is non-empty, the file's SHA-256 is verified on cache
// hit; a mismatch returns ("", nil) so the caller re-downloads.
func cachedAssetPath(cacheDir, assetID string, expectedSHA256 string) (string, error) {
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
				return "", nil
			}
		}
		return "", err
	}
	cachedPath := matches[0]

	// Verify SHA-256 when expected.
	if expectedSHA256 != "" {
		actual, err := sha256File(cachedPath)
		if err != nil {
			return "", nil // corrupted file → re-download
		}
		if actual != expectedSHA256 {
			return "", nil // hash mismatch → re-download
		}
	}
	return cachedPath, nil
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
func writeVeloxAssetToCache(cacheDir, assetID string, expectedSHA256 string, resp *http.Response) (string, error) {
	mediaType := strings.TrimSpace(resp.Header.Get("Content-Type"))
	if idx := strings.Index(mediaType, ";"); idx >= 0 {
		mediaType = strings.TrimSpace(mediaType[:idx])
	}
	if isHTMLMediaType(mediaType) {
		return "", fmt.Errorf("unexpected HTML response while downloading asset")
	}

	reader := bufio.NewReader(resp.Body)
	peek, _ := reader.Peek(512)
	if isHTMLPayload(peek) {
		return "", fmt.Errorf("unexpected HTML response while downloading asset")
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
		return "", err
	}
	tmpPath := tmp.Name()
	defer tmp.Close()

	// Tee the download into a SHA-256 hasher so we can verify integrity
	// before promoting to cache. Even when no expectedSHA256 is supplied,
	// we compute the hash for future caller-side verification.
	hasher := sha256.New()
	teeReader := io.TeeReader(reader, hasher)
	written, err := io.Copy(tmp, teeReader)
	if err != nil {
		_ = os.Remove(tmpPath)
		return "", err
	}
	if written <= 0 {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("downloaded asset is empty")
	}
	if err := tmp.Sync(); err != nil {
		_ = os.Remove(tmpPath)
		return "", err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return "", err
	}

	// Verify against expected SHA-256 when supplied.
	actualSHA256 := fmt.Sprintf("%x", hasher.Sum(nil))
	if expectedSHA256 != "" && actualSHA256 != expectedSHA256 {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("downloaded asset SHA-256 mismatch (got %s, want %s)", actualSHA256[:16], expectedSHA256[:16])
	}

	finalPath := filepath.Join(cacheDir, prefix+ext)
	if info, err := os.Stat(finalPath); err == nil && !info.IsDir() {
		_ = os.Remove(tmpPath)
		return finalPath, nil
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return "", err
	}
	return finalPath, nil
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
