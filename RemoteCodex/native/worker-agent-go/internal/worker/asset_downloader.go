package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"velox-shared/assetref"
	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/internal/prefetch"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/internal/workercache"
)

// downloadVeloxAssetWithMetadata is the integrity-aware asset path. It is now
// a THIN ADAPTER over the canonical CacheResolver: build the explicit
// DownloadRequest, Resolve it through the structured resolution surface (the
// single point where cache telemetry is emitted), then record the
// caller-scoped operation detail. All byte transport, dedup, state tracking,
// pooling, outcome classification and verification live in internal/downloader;
// this function never touches the network itself.
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
	// Operational lifecycle: when the asset resolver blocks on a cache miss,
	// surface the WaitingRuntimeAssets phase so operators see the worker
	// is waiting for a download rather than stuck.
	taskID := TaskIDFromContext(ctx)
	if taskID != "" {
		w.UpdateOperationalPhase(taskID, PhaseWaitingRuntimeAssets)
	}
	resolution, err := w.assetCacheResolver().Resolve(ctx, downloader.DownloadRequest{
		JobID:     jobID,
		TaskID:    taskID,
		WorkerID:  w.config.WorkerID,
		AssetKey:  assetref.AssetKey(assetID),
		AssetID:   assetID,
		Role:      downloader.RoleFromString(role),
		SHA256:    assetref.ContentHash(expectedSHA256),
		SizeBytes: expectedSizeBytes,
		Source:    "master_asset_bridge",
		Priority:  downloader.PriorityForeground,
	})
	if err != nil {
		return "", fmt.Errorf("failed to download velox asset %s: %w", assetID, err)
	}
	// Restore prefetching phase after the asset is resolved.
	if taskID != "" {
		w.UpdateOperationalPhase(taskID, PhasePrefetching)
	}
	if w.prefetchScheduler != nil {
		w.prefetchScheduler.MarkForegroundUse(assetref.AssetKey(assetID))
	}

	completed := time.Now().UTC()
	status := "miss"
	downloadedBytes := resolution.DownloadBytes
	downloadMS := completed.Sub(operationStarted).Milliseconds()
	if resolution.CacheHit {
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
		LocalPath:           resolution.LocalPath,
		Source:              "master_asset_bridge",
	})
	syncSize := resolution.DownloadBytes
	if resolution.CacheHit {
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
	if err := w.syncClipCache(ctx, assetID, resolution.LocalPath, syncSize, assetref.ContentHash(resolution.SHA256)); err != nil {
		return "", fmt.Errorf("record downloaded asset %s: %w", assetID, err)
	}
	logAssetCacheAccess(ctx, w.config.WorkerID, cacheAssetKey(assetID, expectedSHA256), status, downloadedBytes, time.Since(accessStarted).Milliseconds(), 0)
	loggedAccess = true
	return resolution.LocalPath, nil
}

// assetCacheResolver returns the canonical structured-resolution adapter over
// the shared download manager, building it lazily on first use (mirrors the
// manager's lazy construction). The wired sink is the single emission point
// for cache telemetry: every resolution feeds the attempt-scoped tracker
// (per-attempt counters starting at zero) AND the worker-lifetime Prometheus
// view. Rebuilt after Stop() nils the manager.
func (w *Worker) assetCacheResolver() *downloader.CacheResolver {
	w.cacheResolverMu.Lock()
	defer w.cacheResolverMu.Unlock()
	if w.cacheResolver == nil {
		w.cacheResolver = downloader.NewCacheResolver(w.assetDownloadManager(), cacheResolutionSink{
			preparedJobs: func() []prefetch.PreparedJob {
				if w.prefetchScheduler == nil {
					return nil
				}
				return w.prefetchScheduler.PreparedJobs()
			},
			invalidatePreparedAsset: func(jobID, assetKey string) {
				if w.prefetchScheduler != nil {
					w.prefetchScheduler.InvalidatePreparedAsset(jobID, assetKey)
				}
			},
			latestPreparedAtMs: func() int64 {
				if w.prefetchScheduler == nil {
					return 0
				}
				var latestMs int64
				for _, job := range w.prefetchScheduler.PreparedJobs() {
					if !job.PreparedAt.IsZero() {
						ms := job.PreparedAt.UnixMilli()
						if ms > latestMs {
							latestMs = ms
						}
					}
				}
				return latestMs
			},
		})
	}
	return w.cacheResolver
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
				progressCtx := context.WithValue(reportCtx, assetProgressWorkerContextKey{}, w)
				emitAssetProgressCheckpoint(progressCtx, snap)
			},
			OnOperationalSnapshot: func(snapshot downloader.OperationalSnapshot) {
				telemetry.GetPrometheusMetrics().SetAssetDownloadOperational(
					snapshot.ActiveTransfers, snapshot.QueuedTransfers, snapshot.ReadyTransfers,
					snapshot.FailedTransfers, snapshot.CacheHitTransfers, snapshot.BytesDownloaded,
					snapshot.BytesTotal, snapshot.ThroughputBPS, float64(snapshot.ETASeconds),
					snapshot.CoalescedRequestsTotal,
				)
			},
			OnCoalescedRequest: func(sizeBytes int64) {
				telemetry.GetPrometheusMetrics().RecordCacheDuplicateDownload(sizeBytes)
			},
		}, &masterAssetTransferer{w: w})
	}
	return w.assetManager
}

func (w *Worker) assetDownloadConcurrency() int {
	if w.config != nil && w.config.AssetDownloadConcurrency > 0 {
		return w.config.AssetDownloadConcurrency
	}
	return downloader.DefaultAssetConcurrency
}

// syncClipCache records a verified asset in the durable remote-worker index.
// The content-addressed byte cache remains the data source; this index owns
// leases, protected snapshots and eviction decisions.
func (w *Worker) syncClipCache(ctx context.Context, assetID, localPath string, sizeBytes int64, hash assetref.ContentHash) error {
	if w.canonicalAssetCache == nil {
		return nil
	}
	key := assetref.AssetKey(assetID)
	entry := workercache.Entry{AssetKey: key, ContentHash: hash, LocalPath: localPath, SizeBytes: sizeBytes, DownloadComplete: true}
	if err := w.canonicalAssetCache.Store(ctx, entry); err != nil {
		if !errors.Is(err, workercache.ErrDuplicate) {
			return err
		}
		if err := w.canonicalAssetCache.PreserveContentHash(ctx, key, localPath, sizeBytes, hash); err != nil {
			return err
		}
	}
	return w.canonicalAssetCache.MarkUsed(ctx, key)
}
