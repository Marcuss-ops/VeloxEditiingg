package worker

import (
	"bufio"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// assetCacheDir returns the directory where downloaded worker-assets
// (audio + image) are stored. Same resolver path reused for both media
// types — cache key prefixes keep them separated when needed.
//
// Renamed from voiceoverCacheDir when the asset bridge was split
// (commit d8b0131): the directory is now consumed by both the audio
// resolver (resolveAudioPayload → resolveVoiceoverAudioPath) and the
// scene-image resolver (resolveSceneImagePayload), so the "voiceover"
// name no longer describes the full scope of consumers. Cache key
// prefixes (asset_id from each velox-asset:// URI) keep the two media
// streams separated when needed.
func (w *Worker) assetCacheDir() string {
	if w != nil && w.config != nil {
		if trimmed := strings.TrimSpace(w.config.AssetCacheDir); trimmed != "" {
			return filepath.Join(trimmed, "voiceover")
		}
		if trimmed := strings.TrimSpace(w.config.WorkDir); trimmed != "" {
			return filepath.Join(trimmed, "worker_downloads", "assets", "audio")
		}
	}
	return filepath.Join(os.TempDir(), "velox-worker", "assets", "audio")
}

// cachedVoiceoverPath returns a previously cached asset path when present.
func cachedVoiceoverPath(cacheDir, assetID string) (string, error) {
	matches, err := filepath.Glob(filepath.Join(cacheDir, assetID+".*"))
	if err != nil || len(matches) == 0 {
		return "", err
	}
	return matches[0], nil
}

// writeVeloxAssetToCache streams a successful response body to a temp file
// inside the cache directory, then atomically renames to the final path.
// Sniffs for HTML on both the Content-Type header and the first 512 bytes
// of the payload to refuse HTML responses from misconfigured upstreams.
func writeVeloxAssetToCache(cacheDir, assetID string, resp *http.Response) (string, error) {
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

	tmp, err := os.CreateTemp(cacheDir, assetID+"-*")
	if err != nil {
		return "", err
	}
	tmpPath := tmp.Name()
	defer tmp.Close()

	written, err := io.Copy(tmp, reader)
	if err != nil {
		_ = os.Remove(tmpPath)
		return "", err
	}
	if written <= 0 {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("downloaded voiceover is empty")
	}
	if err := tmp.Sync(); err != nil {
		_ = os.Remove(tmpPath)
		return "", err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return "", err
	}

	finalPath := filepath.Join(cacheDir, assetID+ext)
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
