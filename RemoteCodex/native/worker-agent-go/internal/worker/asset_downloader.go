package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"os"
	"strings"
	"time"

	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/internal/workercache"
)

// downloadVeloxAssetWithMetadata is the integrity-aware asset path. A cache
// hit is reused only after the supplied size and SHA-256 checks pass. Invalid
// entries are removed individually by cachedAssetPath, then the asset is
// downloaded and verified before atomic promotion.
func (w *Worker) downloadVeloxAssetWithMetadata(ctx context.Context, assetID, expectedSHA256 string, expectedSizeBytes int64) (string, error) {
	key := assetID + ":" + expectedSHA256 + ":" + fmt.Sprint(expectedSizeBytes)
	value, err, _ := w.assetDownloads.Do(key, func() (interface{}, error) {
		return w.downloadVeloxAssetWithMetadataSingle(ctx, assetID, expectedSHA256, expectedSizeBytes)
	})
	if err != nil {
		return "", err
	}
	return value.(string), nil
}

func (w *Worker) downloadVeloxAssetWithMetadataSingle(ctx context.Context, assetID, expectedSHA256 string, expectedSizeBytes int64) (string, error) {
	cacheDir := w.assetCacheDir()
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return "", err
	}

	operationStarted := time.Now().UTC()
	accessStarted := time.Now()

	// Cache hit fast path: reuse an entry only when both integrity metadata
	// values are available. Bare or partially-described legacy URIs are
	// intentionally treated as misses, because an incompletely verified file
	// must never be reused as a valid hit.
	if expectedSHA256 != "" && expectedSizeBytes > 0 {
		if existing, verifyDuration, err := cachedAssetPathTimed(cacheDir, assetID, expectedSHA256, expectedSizeBytes); err == nil && existing != "" {
			if rec := telemetry.RecorderFromContext(ctx); rec != nil {
				h := rec.Begin(telemetry.EventSpec{Origin: telemetry.OriginWorker, Scope: telemetry.ScopeArtifact, Component: "worker.cache", Action: "hit_read"})
				h.SetMetadata("asset_id", assetID)
				h.Complete()
			}
			completed := time.Now().UTC()
			telemetry.GetPrometheusMetrics().RecordAssetCacheHit("asset")
			telemetry.GetPrometheusMetrics().RecordCacheRequest("hit")
			logAssetCacheAccess(ctx, w.config.WorkerID, cacheAssetKey(assetID, expectedSHA256), "hit", 0, time.Since(accessStarted).Milliseconds(), verifyDuration.Milliseconds())
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
			if err := w.syncClipCache(ctx, assetID, existing, expectedSizeBytes); err != nil {
				return "", fmt.Errorf("record cached asset %s: %w", assetID, err)
			}
			return existing, nil
		}
	}

	telemetry.GetPrometheusMetrics().RecordAssetCacheMiss("asset")
	telemetry.GetPrometheusMetrics().RecordCacheRequest("miss")
	if rec := telemetry.RecorderFromContext(ctx); rec != nil {
		rec.Emit(telemetry.EventSpec{Origin: telemetry.OriginWorker, Scope: telemetry.ScopeArtifact, Component: "worker.cache", Action: "miss"}, telemetry.StatusOK, "", "")
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

		transfer := telemetry.RecorderFromContext(ctx)
		transferHandle := (*telemetry.EventHandle)(nil)
		if transfer != nil {
			transferHandle = transfer.Begin(telemetry.EventSpec{Origin: telemetry.OriginWorker, Scope: telemetry.ScopeTask, Component: "worker.asset", Action: "transfer"})
			transferHandle.SetMetadata("asset_id", assetID)
		}
		localPath, downloadedBytes, verifyDuration, err := writeVeloxAssetToCache(cacheDir, assetID, expectedSHA256, expectedSizeBytes, resp)
		resp.Body.Close()
		if err != nil {
			telemetry.GetPrometheusMetrics().RecordCacheVerify(verifyDuration)
			if transferHandle != nil {
				transferHandle.Abort("asset_transfer", err.Error())
			}
			lastErr = err
			continue
		}
		telemetry.GetPrometheusMetrics().RecordCacheVerify(verifyDuration)
		if transferHandle != nil {
			transferHandle.CompleteWith(downloadedBytes, downloadedBytes, 0, telemetry.StatusOK, "", "")
		}
		completed := time.Now().UTC()
		logAssetCacheAccess(ctx, w.config.WorkerID, cacheAssetKey(assetID, expectedSHA256), "miss", downloadedBytes, time.Since(accessStarted).Milliseconds(), verifyDuration.Milliseconds())
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
		telemetry.GetPrometheusMetrics().RecordCacheDownload(downloadedBytes, completed.Sub(operationStarted))
		if err := w.syncClipCache(ctx, assetID, localPath, downloadedBytes); err != nil {
			return "", fmt.Errorf("record downloaded asset %s: %w", assetID, err)
		}
		return localPath, nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("download failed")
	}
	return "", fmt.Errorf("failed to download velox asset %s: %w", assetID, lastErr)
}

// syncClipCache records a verified asset in the durable remote-worker index.
// The content-addressed byte cache remains the data source; this index owns
// leases, protected snapshots and eviction decisions.
func (w *Worker) syncClipCache(ctx context.Context, assetID, localPath string, sizeBytes int64) error {
	if w.clipCache == nil {
		return nil
	}
	entry := workercache.Entry{DriveFileID: assetID, LocalPath: localPath, SizeBytes: sizeBytes, DownloadComplete: true}
	if err := w.clipCache.Store(ctx, entry); err != nil {
		if !errors.Is(err, workercache.ErrDuplicate) {
			return err
		}
		if err := w.clipCache.MarkDownloadComplete(ctx, assetID, localPath, sizeBytes); err != nil {
			return err
		}
	}
	return w.clipCache.MarkUsed(ctx, assetID)
}
