package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	sharedtelemetry "velox-shared/telemetry"
	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/internal/prefetch"
	"velox-worker-agent/internal/taskrunner"
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
	CacheHitBytes    int64 // total bytes served from verified local cache
	CacheMissBytes   int64 // total bytes downloaded from remote
	PrefetchHitBytes int64 // bytes served from prefetch (subset of CacheHitBytes)

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
// miss is always OriginRuntimeDownload.
type cacheResolutionSink struct {
	// preparedJobs returns the current PreparedJob read model from the
	// prefetch scheduler. The callback is invoked once per cache hit to
	// classify the origin; it must be non-blocking.
	preparedJobs func() []prefetch.PreparedJob

	// invalidatePreparedAsset removes a prepared asset entry when its
	// integrity check fails at runtime (SHA/size mismatch after prefetch).
	// This prevents stale PreparedJob metadata from misclassifying future
	// resolutions. The callback must be non-blocking.
	invalidatePreparedAsset func(jobID, assetKey string)
}

func (s cacheResolutionSink) RecordResolution(ctx context.Context, resolution downloader.CacheResolution) {
	// Classify origin for cache hits.
	if resolution.CacheHit && resolution.Origin == "" {
		resolution.Origin = s.classifyOrigin(resolution)
	} else if !resolution.CacheHit && resolution.Origin == "" {
		resolution.Origin = downloader.OriginRuntimeDownload
	}
	// Invalidate corrupt prepared assets: when a cache miss is classified as
	// MISS_INVALID or MISS_HASH_MISMATCH and there's a matching PreparedJob
	// entry, the prefetch's preparation evidence is stale. Remove it so
	// future resolutions don't misclassify the origin.
	if !resolution.CacheHit && s.invalidatePreparedAsset != nil {
		switch resolution.Outcome {
		case downloader.CacheOutcomeMissInvalid, downloader.CacheOutcomeMissHashMismatch:
			s.invalidateCorruptPreparedAsset(resolution)
		}
	}
	// Attempt view: zero-based per-attempt counters.
	if tracker := assetOperationTrackerFromContext(ctx); tracker != nil {
		tracker.recordResolution(resolution)
	}
	// Worker view is projected from the canonical AttemptSnapshot at
	// attempt Stop. This producer records only the typed cache fact and
	// journal event; it never writes Prometheus directly.
	// Structured attempt event, recorded once per resolution with the
	// canonical outcome attached.
	if rec := telemetry.RecorderFromContext(ctx); rec != nil {
		action := "miss"
		if resolution.CacheHit {
			action = "hit_read"
		}
		h := rec.Begin(telemetry.EventSpec{Origin: telemetry.OriginWorker, Scope: telemetry.ScopeTask, Component: "worker.cache", Action: action})
		h.SetMetadata("asset_id", resolution.AssetID)
		h.SetMetadata("outcome", string(resolution.Outcome))
		h.SetMetadata("origin", string(resolution.Origin))
		if resolution.Downloaded {
			h.SetMetadata("downloaded_bytes", resolution.DownloadBytes)
		}
		h.Complete()
	}
}

// invalidateCorruptPreparedAsset checks if a failed resolution corresponds
// to a prepared asset and invalidates the stale entry. This is called when
// the transferer classifies a cache entry as invalid (SHA/size mismatch),
// meaning the prefetch's preparation evidence is no longer trustworthy.
// The match is job-scoped: when the resolution carries identity fields,
// only the matching PreparedJob entry is invalidated, preventing
// cross-job corruption of unrelated prefetch evidence.
func (s cacheResolutionSink) invalidateCorruptPreparedAsset(resolution downloader.CacheResolution) {
	if s.preparedJobs == nil || s.invalidatePreparedAsset == nil {
		return
	}
	jobs := s.preparedJobs()
	for _, job := range jobs {
		for assetKey, asset := range job.Assets {
			if !resolutionCorruptionMatch(resolution, job, asset) {
				continue
			}
			s.invalidatePreparedAsset(job.JobID, assetKey)
			// Emit corruption metric.
			switch resolution.Outcome {
			case downloader.CacheOutcomeMissInvalid:
				telemetry.GetPrometheusMetrics().RecordPrefetchCorrupted("size_mismatch")
			case downloader.CacheOutcomeMissHashMismatch:
				telemetry.GetPrometheusMetrics().RecordPrefetchCorrupted("hash_mismatch")
			}
			return
		}
	}
}

// resolutionCorruptionMatch checks if a corrupt cache resolution corresponds
// to a prepared asset. Unlike resolutionPrefetchMatch, this does NOT require
// SizeBytes to match (corrupt entries may have zero/incorrect sizes) and does
// NOT require the SHA to match the file (that's exactly why it's corrupt).
// It matches on SHA256 (the EXPECTED hash from the request) and optionally
// scopes to the job/task/asset identity when available.
func resolutionCorruptionMatch(resolution downloader.CacheResolution, job prefetch.PreparedJob, asset prefetch.PreparedAssetMetadata) bool {
	if resolution.SHA256 == "" || asset.SHA256 == "" {
		return false
	}
	if string(resolution.SHA256) != asset.SHA256 {
		return false
	}
	// Job-scoped match: when the resolution carries identity fields,
	// require them to align with the PreparedJob entry.
	if resolution.JobID != "" && job.JobID != "" && resolution.JobID != job.JobID {
		return false
	}
	if resolution.TaskID != "" && job.TaskID != "" && resolution.TaskID != job.TaskID {
		return false
	}
	if resolution.AssetKey != "" && asset.AssetKey != "" && string(resolution.AssetKey) != asset.AssetKey {
		return false
	}
	return true
}

// classifyOrigin determines the ResolutionOrigin for a cache hit by checking
// whether the asset has a matching PreparedJob entry. A PreparedJob with
// matching JobID, TaskID, AssetKey, SHA256, and SizeBytes proves the asset
// was materialized by a FutureAssetPlan for the specific job — this is
// OriginPrefetch. A cache hit without a matching PreparedJob entry is
// OriginWarmCache (the asset was already local from a prior job or session).
//
// The multi-field match prevents cross-job SHA collisions: if Job C has the
// same SHA as an asset prefetched for Job B, the resolution for Job C is
// correctly classified as OriginWarmCache because the JobID/TaskID/AssetKey
// don't match the PreparedJob entry for Job B.
func (s cacheResolutionSink) classifyOrigin(resolution downloader.CacheResolution) downloader.ResolutionOrigin {
	if s.preparedJobs == nil {
		return downloader.OriginWarmCache
	}
	jobs := s.preparedJobs()
	for _, job := range jobs {
		for _, asset := range job.Assets {
			if !resolutionPrefetchMatch(resolution, job, asset) {
				continue
			}
			// Prefer the origin carried on the PreparedAssetMetadata when
			// available. The scheduler tags each prepared asset at prefetch
			// time; this avoids re-deriving the origin from scratch.
			if asset.Origin != "" {
				return asset.Origin
			}
			return downloader.OriginPrefetch
		}
	}
	return downloader.OriginWarmCache
}

// resolutionPrefetchMatch reports whether the cache resolution matches a
// prepared asset entry. The match requires all five identity fields to align:
// JobID, TaskID, AssetKey, SHA256, and SizeBytes. This prevents cross-job
// SHA collisions where two different jobs happen to share the same content
// hash.
func resolutionPrefetchMatch(resolution downloader.CacheResolution, job prefetch.PreparedJob, asset prefetch.PreparedAssetMetadata) bool {
	if resolution.SHA256 == "" || asset.SHA256 == "" {
		return false
	}
	if string(resolution.SHA256) != asset.SHA256 {
		return false
	}
	if asset.SizeBytes <= 0 {
		return false
	}
	// Size match: validate when the resolution carries a known size.
	// A zero SizeBytes on the resolution means the caller did not supply
	// a size contract (legacy/test path); only reject mismatches when both
	// sides have a positive value.
	if resolution.SizeBytes > 0 && resolution.SizeBytes != asset.SizeBytes {
		return false
	}
	// Job-scoped match: when the resolution carries identity fields,
	// require them to align with the PreparedJob entry. This prevents
	// a cache hit for Job C from being classified as prefetch based on
	// a PreparedJob entry belonging to Job B.
	if resolution.JobID != "" && job.JobID != "" && resolution.JobID != job.JobID {
		return false
	}
	if resolution.TaskID != "" && job.TaskID != "" && resolution.TaskID != job.TaskID {
		return false
	}
	if resolution.AssetKey != "" && asset.AssetKey != "" && string(resolution.AssetKey) != asset.AssetKey {
		return false
	}
	return true
}

func cacheAssetKey(assetID, expectedSHA256 string) string {
	if expectedSHA256 != "" {
		return "sha256:" + expectedSHA256
	}
	return assetID
}

func cacheRole(field string) string {
	field = strings.ToLower(field)
	switch {
	case strings.Contains(field, "voiceover"):
		return "voiceover"
	case strings.Contains(field, "stock"), strings.Contains(field, "clip"):
		return "stock"
	case strings.Contains(field, "music"):
		return "music"
	case strings.Contains(field, "effect"), strings.Contains(field, "sfx"):
		return "effect"
	case strings.Contains(field, "image"):
		return "image"
	case strings.Contains(field, "subtitle"), strings.Contains(field, "caption"):
		return "subtitle"
	default:
		return "asset"
	}
}

func expectedAssetSHA256(fields map[string]interface{}) string {
	if fields == nil {
		return ""
	}
	for _, key := range []string{"sha256", "sha_256", "expected_sha256"} {
		if value, ok := fields[key].(string); ok {
			return value
		}
	}
	return ""
}

// expectedAssetSize returns the expected byte count from the asset envelope.
// JSON decoding commonly represents numbers as float64, while typed callers
// may provide int64 or a decimal string, so accept all transport forms.
func integrityCheck(expectedSHA256 string, expectedSizeBytes int64) string {
	if expectedSHA256 != "" && expectedSizeBytes > 0 {
		return "size_bytes+sha256"
	}
	if expectedSHA256 != "" {
		return "sha256"
	}
	if expectedSizeBytes > 0 {
		return "size_bytes"
	}
	return "none"
}

func expectedAssetSize(fields map[string]interface{}) int64 {
	if fields == nil {
		return 0
	}
	for _, key := range []string{"size_bytes", "sizeBytes", "expected_size_bytes", "size"} {
		value, ok := fields[key]
		if !ok {
			continue
		}
		var size int64
		switch typed := value.(type) {
		case int:
			size = int64(typed)
		case int32:
			size = int64(typed)
		case int64:
			size = typed
		case uint:
			size = int64(typed)
		case uint32:
			size = int64(typed)
		case uint64:
			if typed > uint64(^uint64(0)>>1) {
				continue
			}
			size = int64(typed)
		case float64:
			if typed != float64(int64(typed)) {
				continue
			}
			size = int64(typed)
		case json.Number:
			parsed, err := typed.Int64()
			if err != nil {
				continue
			}
			size = parsed
		case string:
			parsed, err := strconv.ParseInt(typed, 10, 64)
			if err != nil {
				continue
			}
			size = parsed
		default:
			continue
		}
		if size > 0 {
			return size
		}
	}
	return 0
}

func attachAssetOperations(report *taskrunner.TaskExecutionReport, tracker *assetOperationTracker) {
	if report == nil || tracker == nil {
		return
	}
	records := tracker.snapshot()
	cache := tracker.cacheSnapshot()
	projectAttemptCacheFacts(report, cache, records)
	if report.Metrics == nil {
		report.Metrics = make(map[string]interface{})
	}
	legacy := report.Metrics

	// The per-attempt counters are accumulated by the canonical resolver
	// sink (single emission point inside CacheResolver.Resolve). They are
	// NEVER re-derived from the record list here: the records are per-asset
	// detail only. Counters intentionally remain zero when no lookup
	// occurred — zero is not a fabricated hit or miss.
	uniqueAssets := make(map[string]struct{}, len(records))
	for _, record := range records {
		if assetID := strings.TrimSpace(record.AssetID); assetID != "" {
			uniqueAssets[assetID] = struct{}{}
		}
	}
	prep := tracker.prepSnapshot()
	// STEP D drill-down: only attach when the resolver observed at least one
	// resolution. An attempt with no lookups carries NO breakdown rather than
	// a zero-filled fake — absence is the honest signal on the wire too.
	if cache.CacheLookups > 0 {
		report.AssetPreparation = &sharedtelemetry.AssetPreparationBreakdown{
			AssetsRequired:          int64(prep.AssetsTotal),
			AssetsUnique:            int64(prep.AssetsUnique),
			CacheHits:               int64(prep.CacheHits),
			CacheMisses:             int64(prep.CacheMisses),
			ReadyBeforeAttempt:      int64(prep.ReadyBefore),
			DownloadedDuringAttempt: int64(prep.DownloadedNow),
			CacheLookupMS:           prep.CacheLookupMS,
			RemoteWaitMS:            prep.RemoteWaitMS,
			RemoteWaitCount:         prep.RemoteWaitCount,
			DownloadWallMS:          prep.DownloadWallMS,
			DownloadWorkMS:          prep.DownloadWorkSum,
			HashVerifyMS:            prep.HashVerifyMS,
			MetadataProbeMS:         prep.MetadataProbeMS,
			MaterializeLocalMS:      prep.MaterializeLocalMS,
			CacheHitBytes:           prep.CacheHitBytes,
			CacheMissBytes:          prep.CacheMissBytes,
			PrefetchHitBytes:        prep.PrefetchHitBytes,
			PrefetchHits:            int64(prep.PrefetchHits),
			WarmCacheHits:           int64(prep.WarmCacheHits),
			RuntimeDownloads:        int64(prep.RuntimeDownloads),
		}
	}
	legacy["cache.enabled"] = tracker.cacheEnabled || len(records) > 0
	legacy["asset.cache.lookups"] = cache.CacheLookups
	legacy["cache.lookups"] = cache.CacheLookups
	legacy["unique.assets.requested"] = int64(len(uniqueAssets))
	legacy["asset.cache.hit.count"] = cache.CacheHits
	legacy["asset.cache.miss.count"] = cache.CacheMisses
	legacy["asset.cache.download.count"] = cache.CacheDownloadCount
	legacy["asset.cache.download.bytes"] = cache.CacheDownloadBytes
	legacy["asset.cache.hit.bytes"] = cache.CacheHitBytes
	legacy["asset.cache.miss.bytes"] = cache.CacheMissBytes
	legacy["asset.cache.prefetch.hit.bytes"] = cache.PrefetchHitBytes
	// Per-attempt asset-preparation drill-down. nested under the requested
	// field names; wall vs work are kept distinct so parallel downloads do not
	// inflate the attempt wall.
	legacy["assets_required"] = int64(prep.AssetsTotal)
	legacy["assets_unique"] = int64(prep.AssetsUnique)
	legacy["assets_cache_hits"] = int64(prep.CacheHits)
	legacy["assets_cache_misses"] = int64(prep.CacheMisses)
	legacy["assets_ready_before_attempt"] = int64(prep.ReadyBefore)
	legacy["assets_downloaded_during_attempt"] = int64(prep.DownloadedNow)
	legacy["asset_preparation"] = map[string]int64{
		"cache_lookup_ms":              prep.CacheLookupMS,
		"remote_wait_ms":               prep.RemoteWaitMS,
		"remote_wait_count":            prep.RemoteWaitCount,
		"network_download_wall_ms":     prep.DownloadWallMS,
		"network_download_work_sum_ms": prep.DownloadWorkSum,
		"hash_verify_ms":               prep.HashVerifyMS,
		"metadata_probe_ms":            prep.MetadataProbeMS,
		"materialize_local_ms":         prep.MaterializeLocalMS,
	}
	if len(records) > 0 {
		// Detailed per-asset records remain a legacy compatibility detail:
		// RawExecutionMetrics carries the canonical aggregate counters, while
		// the existing TaskResult/phase-note path carries this richer record
		// until a typed repeated wire field is introduced.
		legacy["asset_operations"] = records
	}
}

// projectAttemptCacheFacts writes the resolver-owned, attempt-scoped cache
// facts directly into the canonical raw envelope. A tracker with no resolver
// observations carries no cache fact: preserving an existing value is safer
// than manufacturing zeroes from an idle attempt. Once at least one lookup is
// observed, every cache counter is authoritative, including explicit zeroes
// for misses and downloads on a warm attempt.
func projectAttemptCacheFacts(report *taskrunner.TaskExecutionReport, cache AttemptCacheMetrics, records []AssetOperationRecord) {
	if report == nil {
		return
	}
	if cache.CacheLookups == 0 && len(records) == 0 {
		return
	}
	if report.RawMetrics == nil {
		if report.TypedMetrics != nil {
			report.RawMetrics = report.TypedMetrics
		} else {
			report.RawMetrics = &telemetry.RawExecutionMetrics{}
		}
	}
	raw := report.RawMetrics
	if cache.CacheLookups > 0 {
		raw.CacheLookups = cache.CacheLookups
		raw.AssetCacheHitCount = cache.CacheHits
		raw.AssetCacheMissCount = cache.CacheMisses
		raw.CacheDownloadCount = cache.CacheDownloadCount
		raw.CacheDownloadBytes = cache.CacheDownloadBytes
		raw.CacheHitBytes = cache.CacheHitBytes
		raw.CacheMissBytes = cache.CacheMissBytes
		// Single-chain projection: BytesFromLocalCache is the attempt-scoped
		// cache hit volume from the resolver sink, NOT the provider's total
		// cache size. The sink is the sole authority for this value.
		raw.BytesFromLocalCache = cache.CacheHitBytes
		// Per-job resource attribution (migration 160): prefetch bytes.
		raw.JobPrefetchBytes = cache.PrefetchHitBytes
	}
	if len(records) > 0 {
		uniqueAssets := make(map[string]struct{}, len(records))
		for _, record := range records {
			if assetID := strings.TrimSpace(record.AssetID); assetID != "" {
				uniqueAssets[assetID] = struct{}{}
			}
		}
		raw.UniqueAssetsRequested = int64(len(uniqueAssets))
	}
	report.TypedMetrics = report.RawMetrics
}

// attachAssetOperationsToPhaseMarkers preserves the existing TaskResult wire
// schema while making per-asset records part of the report received by the
// Master. PhaseMarker.Notes is already persisted with the report; the JSON is
// self-describing for operators and downstream parsers.
func attachAssetOperationsToPhaseMarkers(report *taskrunner.TaskExecutionReport) {
	if report == nil || len(report.Metrics) == 0 {
		return
	}
	legacy := report.Metrics
	records, ok := legacy["asset_operations"].([]AssetOperationRecord)
	cacheEnabled, hasCacheEnabled := legacy["cache.enabled"]
	cacheLookups, hasCacheLookups := legacy["asset.cache.lookups"]
	cacheHits, hasCacheHits := legacy["asset.cache.hit.count"]
	cacheMisses, hasCacheMisses := legacy["asset.cache.miss.count"]
	cacheDownloadCount, hasDownloadCount := legacy["asset.cache.download.count"]
	cacheDownloadBytes, hasDownloadBytes := legacy["asset.cache.download.bytes"]
	if (!ok || len(records) == 0) && !hasCacheEnabled && !hasCacheLookups && !hasCacheHits && !hasCacheMisses && !hasDownloadCount && !hasDownloadBytes {
		return
	}
	parts := make([]string, 0, 2)
	if len(records) > 0 {
		encoded, err := json.Marshal(records)
		if err != nil {
			return
		}
		parts = append(parts, fmt.Sprintf("asset_operations=%s", encoded))
	}
	if hasCacheEnabled || hasCacheLookups || hasCacheHits || hasCacheMisses || hasDownloadCount || hasDownloadBytes {
		summary := map[string]interface{}{
			"enabled": cacheEnabled,
			"lookups": cacheLookups,
			"hits":    cacheHits,
			"misses":  cacheMisses,
		}
		if hasDownloadCount {
			summary["download_count"] = cacheDownloadCount
		}
		if hasDownloadBytes {
			summary["download_bytes"] = cacheDownloadBytes
		}
		encoded, err := json.Marshal(summary)
		if err != nil {
			return
		}
		parts = append(parts, fmt.Sprintf("cache_summary=%s", encoded))
	}
	notes := strings.Join(parts, " ")

	// Normal TaskRunner reports already contain canonical markers. Enrich the
	// prefetch marker so ordering and the one-marker-per-phase invariant remain
	// unchanged; the existing TaskResult builder serializes Notes.
	for i := range report.PhaseMarkers {
		if report.PhaseMarkers[i].Name == taskrunner.PhasePrefetch {
			if report.PhaseMarkers[i].Notes != "" {
				report.PhaseMarkers[i].Notes += " "
			}
			report.PhaseMarkers[i].Notes += notes
			return
		}
	}

	// Do not append a late marker: canonical reports must preserve phase order.
}
