package worker

import (
	"context"
	"strings"
	"sync"
	"time"

	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/internal/telemetry"
)

// AssetOperationRecord is the report shape for one asset materialization.
// It intentionally contains only JSON-compatible scalar fields so it can be
// embedded in TaskExecutionReport.Metrics without a new wire schema.
type AssetOperationRecord struct {
	AssetID             string    `json:"asset_id"`
	CacheStatus         string    `json:"cache_status"`
	DownloadStartedAt   time.Time `json:"download_started_at"`
	DownloadCompletedAt time.Time `json:"download_completed_at"`
	DownloadMS          int64     `json:"download_ms"`
	DownloadedBytes     int64     `json:"downloaded_bytes"`
	SHA256Verified      bool      `json:"sha256_verified"`
	IntegrityCheck      string    `json:"integrity_check"`
	IntegrityValid      bool      `json:"integrity_valid"`
	LocalPath           string    `json:"local_path"`
	Source              string    `json:"source"`
}

// AttemptCacheMetrics is the per-attempt, zero-based cache accounting
// projection (Phase A1). It is fed ONLY by the canonical resolver sink
// (cacheResolutionSink via CacheResolver.Resolve) and starts at zero for
// every attempt: restarts, retries and previous jobs can never contaminate
// it. The worker-lifetime totals live in the Prometheus exporter as a
// SEPARATE view (WorkerCacheMetrics).
type AttemptCacheMetrics struct {
	CacheLookups       int64
	CacheHits          int64
	CacheMisses        int64
	CacheDownloadCount int64
	CacheDownloadBytes int64
	// Byte-level attribution: the single cacheResolutionSink is the ONLY
	// authority for these counters. They are derived from CacheResolution
	// fields at the single resolution point — never re-derived by report
	// builders or metric adapters.
	CacheHitBytes  int64 // total bytes served from verified local cache
	CacheMissBytes int64 // total bytes downloaded from remote
	// PrefetchHitBytes is the subset of CacheHitBytes where Origin == prefetch.
	// The remainder (CacheHitBytes - PrefetchHitBytes) is warm_cache bytes.
	PrefetchHitBytes int64
	// PrefetchHitCount is the number of resolutions served from prefetch
	// (Origin == prefetch). Paired with PrefetchHitBytes for count+byte
	// attribution from the single cacheResolutionSink authority.
	PrefetchHitCount int64
	// Origin counters: exactly one of these is incremented per resolution.
	OriginPrefetchCount  int64
	OriginWarmCacheCount int64
	OriginDownloadCount  int64
}

// AssetPreparationSummary is the per-attempt asset materialization drill-down.
// Wall-vs-work is kept distinct: WallMS spans a transfer's own window (summed
// across parallel downloads is NOT the attempt wall), WorkSumMS is the sum of
// byte-moving work. Both derive from the canonical per-transfer Timing carried
// on each CacheResolution.
type AssetPreparationSummary struct {
	AssetsTotal   int
	AssetsUnique  int
	CacheHits     int
	CacheMisses   int
	ReadyBefore   int // served from a verified local file (no bytes moved)
	DownloadedNow int // bytes transferred during this attempt

	// Origin counts: how many assets were resolved via each path.
	PrefetchHits     int // assets resolved from prefetch (PreparedJob match)
	WarmCacheHits    int // assets resolved from warm cache (no PreparedJob)
	RuntimeDownloads int // bytes downloaded during this attempt (count)

	// Byte-level attribution: derived from the single cacheResolutionSink.
	CacheHitBytes        int64 // total bytes served from verified local cache
	CacheMissBytes       int64 // total bytes downloaded from remote
	PrefetchHitBytes     int64 // bytes served from prefetch (subset of CacheHitBytes)
	WarmCacheBytes       int64 // bytes served from warm cache (CacheHitBytes - PrefetchHitBytes)
	RuntimeDownloadBytes int64 // bytes downloaded at runtime (same as CacheMissBytes)
	RequiredAssetBytes   int64 // total SizeBytes across all resolutions
	LatestPreparedAtMs   int64 // epoch ms of the most recent prefetch preparation

	CacheLookupMS      int64
	RemoteWaitMS       int64
	RemoteWaitCount    int64
	DownloadWallMS     int64
	DownloadWorkSum    int64
	HashVerifyMS       int64
	MetadataProbeMS    int64
	MaterializeLocalMS int64
}

type assetOperationTracker struct {
	mu           sync.Mutex
	records      []AssetOperationRecord
	cacheEnabled bool
	cache        AttemptCacheMetrics
	prep         AssetPreparationSummary
	// attemptStartedAtMs is set once when the attempt begins. It is the
	// epoch-millisecond wall-clock timestamp used to derive
	// prefetch_ready_lead_ms = attempt_started_at − latest_prepared_at.
	attemptStartedAtMs int64
	// uniqueAssets is the sink-fed unique-asset identity set. It is populated
	// by recordResolution (the canonical single emission point) so the
	// preparation summary never depends on the per-asset record list, which is
	// a separate detail producer and can be empty in resolver-only paths.
	uniqueAssets map[string]struct{}
}

// recordResolution accumulates one canonical cache resolution into the
// per-attempt counters. It is invoked exactly once per resolution by the
// CacheResolver sink — never by handlers, adapters or report builders.
func (t *assetOperationTracker) recordResolution(resolution downloader.CacheResolution) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.cache.CacheLookups++
	if resolution.CacheHit {
		t.cache.CacheHits++
	} else {
		t.cache.CacheMisses++
	}
	if resolution.Downloaded {
		t.cache.CacheDownloadCount++
		t.cache.CacheDownloadBytes += resolution.DownloadBytes
	}
	switch resolution.Origin {
	case downloader.OriginPrefetch:
		t.cache.OriginPrefetchCount++
		t.cache.PrefetchHitCount++
		t.cache.CacheHitBytes += resolution.SizeBytes
		t.cache.PrefetchHitBytes += resolution.SizeBytes
	case downloader.OriginWarmCache:
		t.cache.OriginWarmCacheCount++
		t.cache.CacheHitBytes += resolution.SizeBytes
	case downloader.OriginRuntimeDownload:
		t.cache.OriginDownloadCount++
		t.cache.CacheMissBytes += resolution.DownloadBytes
	}
	// Track required asset bytes (all resolutions contribute).
	if resolution.SizeBytes > 0 {
		t.prep.RequiredAssetBytes += resolution.SizeBytes
	}
	t.prep.AssetsTotal++
	if assetID := strings.TrimSpace(resolution.AssetID); assetID != "" {
		if t.uniqueAssets == nil {
			t.uniqueAssets = make(map[string]struct{})
		}
		t.uniqueAssets[assetID] = struct{}{}
	}
	if resolution.CacheHit {
		t.prep.ReadyBefore++
	} else if resolution.Downloaded {
		t.prep.DownloadedNow++
	}
	// Aggregate the observable per-transfer sub-phases onto the attempt-scoped
	// drill-down. Zero ReviewTiming on hits/legacy paths is safe to add.
	t.prep.CacheLookupMS += resolution.Timing.CacheLookupMS
	t.prep.RemoteWaitMS += resolution.Timing.RemoteWaitMS
	t.prep.DownloadWallMS += resolution.Timing.DownloadWallMS
	t.prep.DownloadWorkSum += resolution.Timing.DownloadWorkMS
	t.prep.HashVerifyMS += resolution.Timing.HashVerifyMS
	t.prep.MetadataProbeMS += resolution.Timing.MetadataProbeMS
	t.prep.MaterializeLocalMS += resolution.Timing.MaterializeLocalMS
	if resolution.Timing.RemoteWaitMS > 0 {
		t.prep.RemoteWaitCount++
	}
}

// cacheSnapshot returns a copy of the per-attempt cache counters.
func (t *assetOperationTracker) cacheSnapshot() AttemptCacheMetrics {
	if t == nil {
		return AttemptCacheMetrics{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.cache
}

type assetOperationTrackerKey struct{}

func withAssetOperationTracker(ctx context.Context, tracker *assetOperationTracker) context.Context {
	return context.WithValue(ctx, assetOperationTrackerKey{}, tracker)
}

func withCacheAccessContext(ctx context.Context, jobID, role string) context.Context {
	return telemetry.WithCacheAccessContext(ctx, jobID, role)
}

func logAssetCacheAccess(ctx context.Context, workerID, assetKey, result string, downloadedBytes, lookupMS, shaVerifyMS int64) {
	telemetry.LogAssetCacheAccess(ctx, workerID, assetKey, result, downloadedBytes, lookupMS, shaVerifyMS)
}

func assetOperationTrackerFromContext(ctx context.Context) *assetOperationTracker {
	if ctx == nil {
		return nil
	}
	tracker, _ := ctx.Value(assetOperationTrackerKey{}).(*assetOperationTracker)
	return tracker
}

func (t *assetOperationTracker) add(record AssetOperationRecord) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.records = append(t.records, record)
	t.mu.Unlock()
}

// prepSnapshot returns a copy of the per-attempt asset-preparation summary
// with the unique-asset count computed from the records.
func (t *assetOperationTracker) prepSnapshot() AssetPreparationSummary {
	if t == nil {
		return AssetPreparationSummary{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	out := t.prep
	out.AssetsUnique = len(t.uniqueAssets)
	out.CacheHits = int(t.cache.CacheHits)
	out.CacheMisses = int(t.cache.CacheMisses)
	out.PrefetchHits = int(t.cache.OriginPrefetchCount)
	out.WarmCacheHits = int(t.cache.OriginWarmCacheCount)
	out.RuntimeDownloads = int(t.cache.OriginDownloadCount)
	out.CacheHitBytes = t.cache.CacheHitBytes
	out.CacheMissBytes = t.cache.CacheMissBytes
	out.PrefetchHitBytes = t.cache.PrefetchHitBytes
	out.WarmCacheBytes = t.cache.CacheHitBytes - t.cache.PrefetchHitBytes
	out.RuntimeDownloadBytes = t.cache.CacheMissBytes
	out.RequiredAssetBytes = t.prep.RequiredAssetBytes
	out.LatestPreparedAtMs = t.prep.LatestPreparedAtMs
	return out
}

func (t *assetOperationTracker) snapshot() []AssetOperationRecord {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]AssetOperationRecord(nil), t.records...)
}

// setLatestPreparedAtMs updates the latest prefetch preparation timestamp
// when a higher value is observed. The max ensures we track the most
// recently prepared asset across all resolutions in this attempt.
func (t *assetOperationTracker) setLatestPreparedAtMs(ms int64) {
	if t == nil || ms <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if ms > t.prep.LatestPreparedAtMs {
		t.prep.LatestPreparedAtMs = ms
	}
}

// setAttemptStartedAtMs records the epoch-millisecond timestamp when the
// attempt began. This is set exactly once at attempt start and is used to
// derive prefetch_ready_lead_ms = attempt_started_at − latest_prepared_at.
func (t *assetOperationTracker) setAttemptStartedAtMs(ms int64) {
	if t == nil || ms <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.attemptStartedAtMs = ms
}

func recordAssetOperation(ctx context.Context, record AssetOperationRecord) {
	assetOperationTrackerFromContext(ctx).add(record)
}

// cacheResolutionSink is the canonical single-emission cache telemetry
// point (Phase A1). It is invoked exactly once per resolution by
// CacheResolver.Resolve and feeds BOTH views from the same structured
// outcome:
//
//  1. AttemptCacheMetrics — the attempt-scoped tracker, starting at zero
//     per attempt (certification view);
//  2. WorkerCacheMetrics — the worker-lifetime Prometheus counters
//     (host observability view).
//
// It also emits the structured per-attempt cache event (hit_read/miss),
// replacing the previous transfer-scoped emissions inside the transferer.
//
// The sink classifies ResolutionOrigin by consulting the PreparedJob
// read model: a cache hit with a matching PreparedJob entry is
// OriginPrefetch; a cache hit without one is OriginWarmCache; a cache
