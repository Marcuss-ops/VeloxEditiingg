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
// Backward-compat: delegates to the metadata-aware downloader without
// expected metadata. Such legacy calls may download the asset, but an
// existing cache entry is never reused as a verified hit.
func (w *Worker) downloadVeloxAsset(ctx context.Context, assetID string) (string, error) {
	return w.downloadVeloxAssetWithMetadata(ctx, assetID, "", 0)
}

// downloadVeloxAssetWithSHA downloads a single velox-asset by ID with
// optional SHA-256 integrity verification. When expectedSHA256 is non-empty:
//   - Cache hit: the cached file's SHA-256 is verified; mismatch triggers
//     a re-download (fail-closed, godlike/07).
//   - Cache miss: the downloaded file's SHA-256 is verified before
//     promoting to cache; mismatch discards the download and returns error.
//   - Cache key includes the first 12 chars of expectedSHA256 so different
//     versions of the same asset never collide.
//
// When expectedSHA256 is empty (legacy path):
//   - Cache hit: is reused only when another expected size is supplied;
//     otherwise the path is treated as a miss and refreshed from Master.
//   - Cache miss: downloads and caches, computing SHA-256 for future callers.
//
// Behaviour preserved verbatim from the original asset_bridge.go:
//   - up to 4 attempts with exponential backoff (500ms, 1s, 2s)
//   - redirects constrained to the master's base host (max 5 hops)
//   - 404 fails fast, 5xx is retried, 4xx other than 404 fails fast
//   - authenticated via worker's Bearer token
//   - HTML responses are rejected after both header and pre-fetch sniff
func (w *Worker) downloadVeloxAssetWithSHA(ctx context.Context, assetID string, expectedSHA256 string) (string, error) {
	return w.downloadVeloxAssetWithMetadata(ctx, assetID, expectedSHA256, 0)
}

// downloadVeloxAssetWithMetadata is the integrity-aware asset path. A cache
// hit is reused only after the supplied size and SHA-256 checks pass. Invalid
// entries are removed individually by cachedAssetPath, then the asset is
// downloaded and verified before atomic promotion.
func (w *Worker) downloadVeloxAssetWithMetadata(ctx context.Context, assetID, expectedSHA256 string, expectedSizeBytes int64) (string, error) {
	cacheDir := w.assetCacheDir()
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return "", err
	}

	operationStarted := time.Now().UTC()

	// Cache hit fast path: reuse an entry only when both integrity metadata
	// values are available. Bare or partially-described legacy URIs are
	// intentionally treated as misses, because an incompletely verified file
	// must never be reused as a valid hit.
	if expectedSHA256 != "" && expectedSizeBytes > 0 {
		if existing, err := cachedAssetPath(cacheDir, assetID, expectedSHA256, expectedSizeBytes); err == nil && existing != "" {
			completed := time.Now().UTC()
			recordAssetOperation(ctx, AssetOperationRecord{
				AssetID:             assetID,
				CacheStatus:         "hit",
				DownloadStartedAt:   operationStarted,
				DownloadCompletedAt: completed,
				DownloadMS:          0,
				DownloadedBytes:     0,
				SHA256Verified:      true,
				IntegrityCheck:      "size_bytes+sha256",
				IntegrityValid:      true,
				LocalPath:           existing,
				Source:              "master_asset_bridge",
			})
			return existing, nil
		}
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
			return "", fmt.Errorf("asset not found")
		}
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			return "", fmt.Errorf("asset download failed: %s", strings.TrimSpace(string(body)))
		}
		if resp.StatusCode >= 500 {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			lastErr = fmt.Errorf("master returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
			continue
		}

		localPath, downloadedBytes, err := writeVeloxAssetToCache(cacheDir, assetID, expectedSHA256, expectedSizeBytes, resp)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		completed := time.Now().UTC()
		recordAssetOperation(ctx, AssetOperationRecord{
			AssetID:             assetID,
			CacheStatus:         "miss",
			DownloadStartedAt:   operationStarted,
			DownloadCompletedAt: completed,
			DownloadMS:          completed.Sub(operationStarted).Milliseconds(),
			DownloadedBytes:     downloadedBytes, SHA256Verified: expectedSHA256 != "",
			IntegrityCheck: integrityCheck(expectedSHA256, expectedSizeBytes),
			IntegrityValid: expectedSHA256 != "" && expectedSizeBytes > 0,
			LocalPath:      localPath,
			Source:         "master_asset_bridge",
		})
		return localPath, nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("download failed")
	}
	return "", fmt.Errorf("failed to download velox asset %s: %w", assetID, lastErr)
}
