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
	report.AssetOperations = append([]AssetOperationRecord(nil), records...)

	// The per-attempt counters are accumulated by the canonical resolver
	// sink (single emission point inside CacheResolver.Resolve). They are
	// NEVER re-derived from the record list here: the records are per-asset
	// detail only. Counters intentionally remain zero when no lookup
	// occurred — zero is not a fabricated hit or miss.
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
	_ = prep // the typed AssetPreparation field above is the sole summary.
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
	if report == nil {
		return
	}
	records := report.AssetOperations
	raw := report.RawMetrics
	hasCache := raw != nil && (raw.CacheLookups > 0 || raw.AssetCacheHitCount > 0 || raw.AssetCacheMissCount > 0 || raw.CacheDownloadCount > 0)
	if len(records) == 0 && !hasCache {
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
	if hasCache {
		summary := map[string]interface{}{
			"enabled":        true,
			"lookups":        raw.CacheLookups,
			"hits":           raw.AssetCacheHitCount,
			"misses":         raw.AssetCacheMissCount,
			"download_count": raw.CacheDownloadCount,
			"download_bytes": raw.CacheDownloadBytes,
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
