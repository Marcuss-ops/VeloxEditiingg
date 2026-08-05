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

	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/internal/workercache"
)

// downloadVeloxAssetWithMetadata is the integrity-aware asset path. It is now
// a THIN ADAPTER over the canonical AssetDownloadManager: build the explicit
// DownloadRequest, Resolve it, then record the caller-scoped operation
// telemetry. All byte transport, dedup, state tracking, pooling and
// verification live in internal/downloader; this function never touches the
// network itself.
func (w *Worker) downloadVeloxAssetWithMetadata(ctx context.Context, assetID, expectedSHA256 string, expectedSizeBytes int64) (string, error) {
	operationStarted := time.Now().UTC()
	accessStarted := time.Now()
	cacheDir := w.assetCacheDir()
	loggedAccess := false
	defer func() {
		if !loggedAccess {
			logAssetCacheAccess(ctx, w.config.WorkerID, cacheAssetKey(assetID, expectedSHA256), "error", 0, time.Since(accessStarted).Milliseconds(), 0)
		}
	}()
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return "", err
	}

	jobID, role := telemetry.CacheAccessContextFromContext(ctx)
	asset, err := w.assetDownloadManager().Resolve(ctx, downloader.DownloadRequest{
		JobID:     jobID,
		AssetKey:  assetID,
		AssetID:   assetID,
		Role:      downloader.RoleFromString(role),
		SHA256:    expectedSHA256,
		SizeBytes: expectedSizeBytes,
		Source:    "master_asset_bridge",
		Priority:  downloader.DefaultPriority,
	})
	if err != nil {
		return "", fmt.Errorf("failed to download velox asset %s: %w", assetID, err)
	}

	completed := time.Now().UTC()
	status := "miss"
	downloadedBytes := asset.SizeBytes
	downloadMS := completed.Sub(operationStarted).Milliseconds()
	if asset.CacheHit {
		status = "hit"
		downloadedBytes = 0
		downloadMS = 0
	}
	recordAssetOperation(ctx, AssetOperationRecord{
		AssetID:             assetID,
		CacheStatus:         status,
		DownloadStartedAt:   operationStarted,
		DownloadCompletedAt: completed,
		DownloadMS:          downloadMS,
		DownloadedBytes:     downloadedBytes,
		SHA256Verified:      expectedSHA256 != "",
		IntegrityCheck:      integrityCheck(expectedSHA256, expectedSizeBytes),
		IntegrityValid:      expectedSHA256 != "" && expectedSizeBytes > 0,
		LocalPath:           asset.LocalPath,
		Source:              "master_asset_bridge",
	})
	syncSize := asset.SizeBytes
	if asset.CacheHit {
		// A cache hit reports zero downloaded bytes; the durable index should
		// still record the expected file size.
		syncSize = expectedSizeBytes
		if syncSize <= 0 {
			// Legacy hit: the payload carried no size, use the remembered
			// self-verified size so the durable index stays honest.
			if remembered, ok := w.rememberedAssetIntegrity(assetID); ok {
				syncSize = remembered.SizeBytes
			}
		}
	}
	if err := w.syncClipCache(ctx, assetID, asset.LocalPath, syncSize); err != nil {
		return "", fmt.Errorf("record downloaded asset %s: %w", assetID, err)
	}
	logAssetCacheAccess(ctx, w.config.WorkerID, cacheAssetKey(assetID, expectedSHA256), status, downloadedBytes, time.Since(accessStarted).Milliseconds(), 0)
	loggedAccess = true
	return asset.LocalPath, nil
}

// assetDownloadManager returns the worker's canonical download manager,
// constructing it lazily on first use so bare test Workers keep working
// without explicit wiring. The manager's pool size comes from
// VELOX_ASSET_DOWNLOAD_CONCURRENCY (default 4).
func (w *Worker) assetDownloadManager() downloader.AssetDownloadManager {
	w.assetManagerMu.Lock()
	defer w.assetManagerMu.Unlock()
	if w.assetManager == nil {
		w.assetManager = downloader.NewManager(downloader.Config{
			Concurrency: w.assetDownloadConcurrency(),
			// Durable progress checkpoints (throttled ~2s / 16MB by the
			// manager): emitted as worker-side telemetry events that feed the
			// master's asset-download read model. Non-blocking by contract.
			OnCheckpoint: func(snap downloader.DownloadSnapshot, reportCtx context.Context) {
				emitAssetProgressCheckpoint(reportCtx, snap)
			},
		}, &masterAssetTransferer{w: w})
	}
	return w.assetManager
}

// emitAssetProgressCheckpoint records one durable asset-download checkpoint
// event (origin: worker, scope: task). The manager throttles these to ~2s or
// 16MB per transfer; terminal transitions always checkpoint. The first
// waiter's telemetry context supplies the recorder — value-reads only, per
// the Transferer context contract.
func emitAssetProgressCheckpoint(ctx context.Context, snap downloader.DownloadSnapshot) {
	rec := telemetry.RecorderFromContext(ctx)
	if rec == nil {
		return
	}
	h := rec.Begin(telemetry.EventSpec{
		Origin: telemetry.OriginWorker, Scope: telemetry.ScopeTask,
		Component: "worker.asset", Action: "progress_checkpoint",
	})
	h.SetMetadata("asset_id", snap.AssetID)
	h.SetMetadata("transfer_id", snap.TransferID)
	h.SetMetadata("state", string(snap.State))
	h.SetMetadata("progress_percent", fmt.Sprintf("%.1f", snap.ProgressPercent))
	h.SetMetadata("bytes_downloaded", snap.BytesDownloaded)
	h.SetMetadata("bytes_total", snap.BytesTotal)
	h.SetMetadata("throughput_bps", int64(snap.ThroughputBytesPerSecond))
	h.SetMetadata("eta_seconds", snap.ETASeconds)
	h.Complete()
}

func (w *Worker) assetDownloadConcurrency() int {
	if w.config != nil && w.config.AssetDownloadConcurrency > 0 {
		return w.config.AssetDownloadConcurrency
	}
	return downloader.DefaultAssetConcurrency
}

// masterAssetTransferer implements downloader.Transferer with the worker's
// existing integrity-aware byte pipeline: cache probe on the canonical
// assets directory, then an authenticated HTTP GET on the master asset bridge
// with atomic .part-style promotion (writeVeloxAssetToCache) and size+SHA-256
// verification.
//
// Context contract (see downloader/source.go): ctx is the TRANSFER context
// (cancellation only); reportCtx is the first waiter's caller context and is
// used purely for non-blocking telemetry value reads.
type masterAssetTransferer struct{ w *Worker }

// Check probes the local cache. A hit is valid only when the supplied size
// and SHA-256 pass against the on-disk file; corrupt entries are evicted
// individually and reported as a miss.
//
// Requests that arrive with partial (or no) integrity metadata are upgraded
// using the remembered self-verified digest of the last successful download
// of the same asset, so a fresh cache access for a folder-backed asset can
// still be served as a verified hit without a master round-trip.
func (t *masterAssetTransferer) Check(ctx context.Context, reportCtx context.Context, req downloader.DownloadRequest) (downloader.CacheCheckResult, error) {
	w := t.w
	probeSHA, probeSize := req.SHA256, req.SizeBytes
	if probeSHA == "" || probeSize <= 0 {
		if remembered, ok := w.rememberedAssetIntegrity(req.AssetID); ok {
			probeSHA, probeSize = remembered.SHA256, remembered.SizeBytes
		}
	}
	if probeSHA != "" && probeSize > 0 {
		if existing, _, err := cachedAssetPathTimed(w.assetCacheDir(), req.AssetID, probeSHA, probeSize); err == nil && existing != "" {
			telemetry.GetPrometheusMetrics().RecordAssetCacheHit("asset")
			telemetry.GetPrometheusMetrics().RecordCacheRequest("hit")
			if rec := telemetry.RecorderFromContext(reportCtx); rec != nil {
				h := rec.Begin(telemetry.EventSpec{Origin: telemetry.OriginWorker, Scope: telemetry.ScopeTask, Component: "worker.cache", Action: "hit_read"})
				h.SetMetadata("asset_id", req.AssetID)
				h.Complete()
			}
			return downloader.CacheCheckResult{CacheHit: true, LocalPath: existing}, nil
		}
	}
	return downloader.CacheCheckResult{}, nil
}

// Transfer streams the asset from the master asset bridge into the local
// cache, verifying size and SHA-256 before the file becomes visible. The
// retry loop is classification-aware: 401/403/404 and other permanent 4xx are
// terminal; 408/429/5xx and transport errors are retried (1s/2s/4s + jitter).
func (t *masterAssetTransferer) Transfer(ctx context.Context, reportCtx context.Context, req downloader.DownloadRequest, onProgress func(downloadedBytes int64)) (downloader.TransferResult, error) {
	w := t.w
	assetID := req.AssetID
	cacheDir := w.assetCacheDir()
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return downloader.TransferResult{}, err
	}

	transferStarted := time.Now()

	telemetry.GetPrometheusMetrics().RecordAssetCacheMiss("asset")
	telemetry.GetPrometheusMetrics().RecordCacheRequest("miss")
	if rec := telemetry.RecorderFromContext(reportCtx); rec != nil {
		rec.Emit(telemetry.EventSpec{Origin: telemetry.OriginWorker, Scope: telemetry.ScopeTask, Component: "worker.cache", Action: "miss"}, telemetry.StatusOK, "", "")
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
		CheckRedirect: func(r *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return fmt.Errorf("too many redirects")
			}
			if baseHost != "" && strings.ToLower(strings.TrimSpace(r.URL.Host)) != baseHost {
				return fmt.Errorf("redirected to unexpected host")
			}
			return nil
		},
	}
	backoffs := downloader.BackoffSchedule(downloader.DefaultMaxAttempts, downloader.DefaultBaseBackoff, downloader.DefaultJitter)

	var lastErr error
	for attempt := 0; attempt < downloader.DefaultMaxAttempts; attempt++ {
		if attempt > 0 {
			wait := backoffs[attempt-1]
			select {
			case <-ctx.Done():
				return downloader.TransferResult{}, ctx.Err()
			case <-time.After(wait):
			}
		}

		reqHTTP, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
		if err != nil {
			return downloader.TransferResult{}, err
		}
		if authToken != "" {
			reqHTTP.Header.Set("Authorization", "Bearer "+authToken)
		}

		resp, err := client.Do(reqHTTP)
		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode == http.StatusNotFound {
			resp.Body.Close()
			return downloader.TransferResult{}, fmt.Errorf("asset not found")
		}
		if downloader.IsPermanentStatus(resp.StatusCode) {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			return downloader.TransferResult{}, fmt.Errorf("asset download failed: %s", strings.TrimSpace(string(body)))
		}
		if downloader.IsRetryableStatus(resp.StatusCode) {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			lastErr = fmt.Errorf("master returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
			continue
		}

		transfer := telemetry.RecorderFromContext(reportCtx)
		transferHandle := (*telemetry.EventHandle)(nil)
		if transfer != nil {
			transferHandle = transfer.Begin(telemetry.EventSpec{Origin: telemetry.OriginWorker, Scope: telemetry.ScopeTask, Component: "worker.asset", Action: "transfer"})
			transferHandle.SetMetadata("asset_id", assetID)
		}
		// Report streamed bytes to the download manager so throttled
		// progress snapshots (percent, throughput, ETA) stay live during the
		// transfer.
		if onProgress != nil {
			// Each retry is a fresh logical attempt. Reset the manager's
			// logical byte progress before the new body starts; network bytes
			// from failed attempts must not make a later attempt look READY.
			if attempt > 0 {
				onProgress(0)
			}
			resp.Body = &assetProgressBody{src: resp.Body, onProgress: onProgress}
		}
		localPath, downloadedBytes, actualSHA, verifyDuration, err := writeVeloxAssetToCache(cacheDir, assetID, req.SHA256, req.SizeBytes, resp)
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
		telemetry.GetPrometheusMetrics().RecordCacheDownload(downloadedBytes, time.Since(transferStarted))
		if transferHandle != nil {
			transferHandle.CompleteWith(downloadedBytes, downloadedBytes, 0, telemetry.StatusOK, "", "")
		}
		// Remember the self-verified digest so later partial-metadata accesses
		// for the same asset become verified cache hits (no re-download).
		w.rememberAssetIntegrity(assetID, actualSHA, downloadedBytes)
		return downloader.TransferResult{LocalPath: localPath, Bytes: downloadedBytes, SHA256: actualSHA}, nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("download failed")
	}
	return downloader.TransferResult{}, fmt.Errorf("failed to download velox asset %s: %w", assetID, lastErr)
}

// assetProgressBody wraps an http response body to report streamed bytes to
// the download manager's progress hook (one call per read chunk; the manager
// throttles its own publishes). Counts bytes actually read, so a partial or
// aborted stream reports exactly the bytes received.
type assetProgressBody struct {
	src        io.ReadCloser
	onProgress func(downloadedBytes int64)
	done       int64
}

func (p *assetProgressBody) Read(b []byte) (int, error) {
	n, err := p.src.Read(b)
	if n > 0 {
		p.done += int64(n)
		p.onProgress(p.done)
	}
	return n, err
}

func (p *assetProgressBody) Close() error { return p.src.Close() }

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
