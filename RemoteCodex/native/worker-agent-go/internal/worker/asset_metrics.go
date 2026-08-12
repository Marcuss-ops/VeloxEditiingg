package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"velox-worker-agent/internal/downloader"
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
}

type assetOperationTracker struct {
	mu           sync.Mutex
	records      []AssetOperationRecord
	cacheEnabled bool
	cache        AttemptCacheMetrics
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
type cacheResolutionSink struct{}

func (cacheResolutionSink) RecordResolution(ctx context.Context, resolution downloader.CacheResolution) {
	// Attempt view: zero-based per-attempt counters.
	if tracker := assetOperationTrackerFromContext(ctx); tracker != nil {
		tracker.recordResolution(resolution)
	}
	// Worker view: low-cardinality Prometheus counters.
	prom := telemetry.GetPrometheusMetrics()
	if resolution.CacheHit {
		prom.RecordAssetCacheHit("asset")
		prom.RecordCacheRequest("hit")
	} else {
		prom.RecordAssetCacheMiss("asset")
		prom.RecordCacheRequest("miss")
	}
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
		if resolution.Downloaded {
			h.SetMetadata("downloaded_bytes", resolution.DownloadBytes)
		}
		h.Complete()
	}
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
	legacy := report.LegacyMetrics()

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
	legacy["cache.enabled"] = tracker.cacheEnabled || len(records) > 0
	legacy["asset.cache.lookups"] = cache.CacheLookups
	legacy["cache.lookups"] = cache.CacheLookups
	legacy["unique.assets.requested"] = int64(len(uniqueAssets))
	legacy["asset.cache.hit.count"] = cache.CacheHits
	legacy["asset.cache.miss.count"] = cache.CacheMisses
	legacy["asset.cache.download.count"] = cache.CacheDownloadCount
	legacy["asset.cache.download.bytes"] = cache.CacheDownloadBytes
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
	if report == nil || !report.HasLegacyMetrics() {
		return
	}
	legacy := report.LegacyMetrics()
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
