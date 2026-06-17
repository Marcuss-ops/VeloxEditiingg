package worker

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"mime"
	"net/http"
	neturl "net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func firstVoiceoverReference(params map[string]interface{}) string {
	if params == nil {
		return ""
	}
	for _, key := range []string{"audio_path", "voiceover_path", "voiceover"} {
		if v, ok := params[key].(string); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	if v, ok := params["voiceover_paths"]; ok {
		switch items := v.(type) {
		case []string:
			for _, item := range items {
				if trimmed := strings.TrimSpace(item); trimmed != "" {
					return trimmed
				}
			}
		case []interface{}:
			for _, item := range items {
				if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
					return strings.TrimSpace(s)
				}
			}
		}
	}
	return ""
}

func (w *Worker) resolveVoiceoverAudioPath(ctx context.Context, ref string, params map[string]interface{}) (string, error) {
	reference := strings.TrimSpace(ref)
	if reference == "" {
		reference = firstVoiceoverReference(params)
	}
	if reference == "" {
		return "", fmt.Errorf("missing voiceover audio path")
	}

	switch {
	case strings.HasPrefix(reference, "velox-asset://"):
		assetID := strings.TrimSpace(strings.TrimPrefix(reference, "velox-asset://"))
		if assetID == "" || strings.ContainsAny(assetID, `/\`) {
			return "", fmt.Errorf("invalid velox asset reference")
		}
		return w.downloadVeloxAsset(ctx, assetID)
	case strings.HasPrefix(strings.ToLower(reference), "http://"), strings.HasPrefix(strings.ToLower(reference), "https://"), strings.Contains(strings.ToLower(reference), "drive.google.com"):
		return "", fmt.Errorf("unsupported voiceover reference: raw URL must be bridged as velox-asset://")
	default:
		if info, err := os.Stat(reference); err == nil && !info.IsDir() {
			return reference, nil
		}
		return "", fmt.Errorf("voiceover file not found locally: %s", reference)
	}
}

func (w *Worker) downloadVeloxAsset(ctx context.Context, assetID string) (string, error) {
	cacheDir := w.voiceoverCacheDir()
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return "", err
	}

	if existing, err := cachedVoiceoverPath(cacheDir, assetID); err == nil && existing != "" {
		return existing, nil
	}

	downloadURL := strings.TrimRight(strings.TrimSpace(w.config.MasterURL), "/") + "/api/v1/worker-assets/" + neturl.PathEscape(assetID)
	authToken := ""
	if w.apiClient != nil {
		authToken = strings.TrimSpace(w.apiClient.AuthToken())
	}
	baseHost := ""
	if parsed, err := neturl.Parse(strings.TrimSpace(w.config.MasterURL)); err == nil {
		baseHost = strings.ToLower(strings.TrimSpace(parsed.Host))
	}

	client := &http.Client{
		Timeout: 2 * time.Minute,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return fmt.Errorf("too many redirects")
			}
			if baseHost != "" && strings.ToLower(strings.TrimSpace(req.URL.Host)) != baseHost {
				return fmt.Errorf("redirected to unexpected host")
			}
			return nil
		},
	}

	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(1<<uint(attempt-1)) * 500 * time.Millisecond
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(backoff):
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
		if err != nil {
			return "", err
		}
		if authToken != "" {
			req.Header.Set("Authorization", "Bearer "+authToken)
		}

		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode == http.StatusNotFound {
			resp.Body.Close()
			return "", fmt.Errorf("voiceover asset not found")
		}
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			return "", fmt.Errorf("voiceover asset download failed: %s", strings.TrimSpace(string(body)))
		}
		if resp.StatusCode >= 500 {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			lastErr = fmt.Errorf("master returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
			continue
		}

		localPath, err := writeVeloxAssetToCache(cacheDir, assetID, resp)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		return localPath, nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("download failed")
	}
	return "", fmt.Errorf("failed to download velox asset %s: %w", assetID, lastErr)
}

func (w *Worker) voiceoverCacheDir() string {
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

func cachedVoiceoverPath(cacheDir, assetID string) (string, error) {
	matches, err := filepath.Glob(filepath.Join(cacheDir, assetID+".*"))
	if err != nil || len(matches) == 0 {
		return "", err
	}
	return matches[0], nil
}

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

func isHTMLMediaType(mediaType string) bool {
	normalized := strings.ToLower(strings.TrimSpace(mediaType))
	return normalized == "text/html" || strings.HasPrefix(normalized, "text/html;")
}

func isHTMLPayload(data []byte) bool {
	trimmed := strings.ToLower(strings.TrimSpace(string(data)))
	return strings.HasPrefix(trimmed, "<!doctype html") ||
		strings.HasPrefix(trimmed, "<html") ||
		strings.Contains(trimmed, "<body") ||
		strings.Contains(trimmed, "login")
}

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
