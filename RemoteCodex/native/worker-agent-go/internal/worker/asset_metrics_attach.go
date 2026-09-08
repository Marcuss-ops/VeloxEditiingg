package worker

import (
	"encoding/json"
	"fmt"
	"strings"

	sharedtelemetry "velox-shared/telemetry"

	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/internal/telemetry"
)

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
			WarmCacheBytes:          prep.WarmCacheBytes,
			RuntimeDownloadBytes:    prep.RuntimeDownloadBytes,
			RequiredAssetBytes:      prep.RequiredAssetBytes,
			LatestPreparedAtMs:      prep.LatestPreparedAtMs,
			AttemptStartedAtMs:      tracker.attemptStartedAtMs,
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
