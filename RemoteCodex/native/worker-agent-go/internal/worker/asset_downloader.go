package worker

import (
	"context"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"os"
	"strings"
	"time"
)

// downloadVeloxAsset downloads a single velox-asset by ID through the
// configured master's worker-assets endpoint. The shared instance is
// reused by both the audio resolver and the scene-image resolver —
// the asset bridge never builds per-domain downloaders.
//
// Behaviour preserved verbatim from the original asset_bridge.go:
//   - up to 4 attempts with exponential backoff (500ms, 1s, 2s)
//   - redirects constrained to the master's base host (max 5 hops)
//   - 404 fails fast, 5xx is retried, 4xx other than 404 fails fast
//   - authenticated via worker's Bearer token
//   - HTML responses are rejected after both header and pre-fetch sniff
func (w *Worker) downloadVeloxAsset(ctx context.Context, assetID string) (string, error) {
	cacheDir := w.assetCacheDir()
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
