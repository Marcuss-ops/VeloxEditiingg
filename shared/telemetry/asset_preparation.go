package telemetry

import (
	"encoding/json"
	"math"
	"strconv"
)

// AssetPreparationBreakdown is the STEP D drill-down INSIDE the
// asset_preparation waterfall bucket: where did the prepare window actually
// go. It is carried on the worker's durable TaskResult and decoded by the
// Master read model exactly as measured — never derived or fabricated on
// either side.
//
// Wall vs work is a load-bearing distinction here. Sub-phase values are
// sums across per-asset resolutions and MAY overlap each other (parallel
// downloads), so they must never be added together and compared against the
// bucket wall:
//
//	download_wall_ms  = Σ per-transfer windows  (serial-equivalent wait)
//	download_work_ms  = Σ byte-moving time      (true work)
//
// The union that fits inside asset_preparation.wall_ms is known only to the
// worker runtime; the Master therefore never re-computes an internal
// coverage_pct from these sums. An absent breakdown means the worker predates
// the field or no resolution was observed — it is reported as absent, not as
// zeros dressed up as measurements.
type AssetPreparationBreakdown struct {
	// Per-attempt asset accounting (the §22 counters).
	AssetsRequired int64 `json:"assets_required"`
	AssetsUnique   int64 `json:"assets_unique"`
	CacheHits      int64 `json:"cache_hits"`
	CacheMisses    int64 `json:"cache_misses"`
	// ReadyBeforeAttempt counts resolutions served from a verified local file
	// (no bytes moved); DownloadedDuringAttempt counts bytes transferred now.
	ReadyBeforeAttempt      int64 `json:"ready_before_attempt"`
	DownloadedDuringAttempt int64 `json:"downloaded_during_attempt"`

	// Sub-phase durations (sums across per-asset resolutions).
	// CacheLookupMS — wall time probing the local cache.
	CacheLookupMS int64 `json:"cache_lookup_ms"`
	// RemoteWaitMS/RemoteWaitCount — wall time transfers spent queued before a
	// download slot became available (the observable remote materialization
	// wait) and how many transfers waited at least once.
	RemoteWaitMS    int64 `json:"remote_wait_ms"`
	RemoteWaitCount int64 `json:"remote_wait_count"`
	// DownloadWallMS — Σ per-transfer byte-transfer spans;
	// DownloadWorkMS — Σ actual byte-moving time across attempts. Parallel
	// downloads make work_sum smaller than wall_sum without implying stall.
	DownloadWallMS int64 `json:"download_wall_ms"`
	DownloadWorkMS int64 `json:"download_work_ms"`
	// HashVerifyMS / MetadataProbeMS / MaterializeLocalMS — verify, metadata
	// probe and atomic local promotion overheads respectively.
	HashVerifyMS       int64 `json:"hash_verify_ms"`
	MetadataProbeMS    int64 `json:"metadata_probe_ms"`
	MaterializeLocalMS int64 `json:"materialize_local_ms"`

	// Byte-level attribution: derived from the single cacheResolutionSink.
	// CacheHitBytes counts bytes served from verified local cache;
	// CacheMissBytes counts bytes downloaded from remote; PrefetchHitBytes
	// is the subset of CacheHitBytes where origin == prefetch.
	CacheHitBytes    int64 `json:"cache_hit_bytes"`
	CacheMissBytes   int64 `json:"cache_miss_bytes"`
	PrefetchHitBytes int64 `json:"prefetch_hit_bytes"`

	// Origin counters: exactly one of {PrefetchHits, WarmCacheHits,
	// RuntimeDownloads} is incremented per resolution.
	PrefetchHits     int64 `json:"prefetch_hits"`
	WarmCacheHits    int64 `json:"warm_cache_hits"`
	RuntimeDownloads int64 `json:"runtime_downloads"`

	// Byte-level origin attribution: the complement pairs for the count
	// counters above. WarmCacheBytes is the subset of CacheHitBytes where
	// origin == warm_cache; RuntimeDownloadBytes is the subset of
	// CacheMissBytes where origin == runtime_download.
	WarmCacheBytes       int64 `json:"warm_cache_bytes"`
	RuntimeDownloadBytes int64 `json:"runtime_download_bytes"`

	// RequiredAssetBytes is the sum of SizeBytes across all required asset
	// resolutions. Used as the denominator for prepared_byte_ratio.
	RequiredAssetBytes int64 `json:"required_asset_bytes"`

	// LatestPreparedAtMs is the monotonic wall-clock millisecond timestamp
	// (epoch) of the most recently prepared asset in the prefetch scheduler.
	// Zero when no prefetch was observed. Used as the numerator for
	// prefetch_ready_lead_ms = attempt_started_at_ms - LatestPreparedAtMs.
	LatestPreparedAtMs int64 `json:"latest_prepared_at_ms"`

	// AttemptStartedAtMs is the monotonic wall-clock millisecond timestamp
	// (epoch) when the attempt started. Zero when not set. Used together
	// with LatestPreparedAtMs to derive prefetch_ready_lead_ms.
	AttemptStartedAtMs int64 `json:"attempt_started_at_ms"`
}

// Wire contract: the durable payload is the Master's protojson.Marshal of the
// TaskResult, which renders every int64 as a STRING and the field names in
// camelCase (e.g. "remoteWaitMs":"282110"). Hand-written reports may instead
// use the snake_case/spelled-out-number shape. UnmarshalJSON accepts both;
// MarshalJSON always emits the snake_case read-model shape.
type assetPreparationAlias AssetPreparationBreakdown

func (b AssetPreparationBreakdown) MarshalJSON() ([]byte, error) {
	return json.Marshal(assetPreparationAlias(b))
}

func (b *AssetPreparationBreakdown) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	pick := func(snake, camel string) int64 {
		value, ok := fields[snake]
		if !ok || len(value) == 0 {
			value, ok = fields[camel]
		}
		if !ok || len(value) == 0 {
			return 0
		}
		// protojson renders int64 as a quoted decimal string; plain JSON
		// writers emit numbers. Accept both.
		var number int64
		if json.Unmarshal(value, &number) == nil {
			return number
		}
		var text string
		if json.Unmarshal(value, &text) == nil {
			number, _ = strconv.ParseInt(text, 10, 64)
		}
		return number
	}
	b.AssetsRequired = pick("assets_required", "assetsRequired")
	b.AssetsUnique = pick("assets_unique", "assetsUnique")
	b.CacheHits = pick("cache_hits", "cacheHits")
	b.CacheMisses = pick("cache_misses", "cacheMisses")
	b.ReadyBeforeAttempt = pick("ready_before_attempt", "readyBeforeAttempt")
	b.DownloadedDuringAttempt = pick("downloaded_during_attempt", "downloadedDuringAttempt")
	b.CacheLookupMS = pick("cache_lookup_ms", "cacheLookupMs")
	b.RemoteWaitMS = pick("remote_wait_ms", "remoteWaitMs")
	b.RemoteWaitCount = pick("remote_wait_count", "remoteWaitCount")
	b.DownloadWallMS = pick("download_wall_ms", "downloadWallMs")
	b.DownloadWorkMS = pick("download_work_ms", "downloadWorkMs")
	b.HashVerifyMS = pick("hash_verify_ms", "hashVerifyMs")
	b.MetadataProbeMS = pick("metadata_probe_ms", "metadataProbeMs")
	b.MaterializeLocalMS = pick("materialize_local_ms", "materializeLocalMs")
	b.CacheHitBytes = pick("cache_hit_bytes", "cacheHitBytes")
	b.CacheMissBytes = pick("cache_miss_bytes", "cacheMissBytes")
	b.PrefetchHitBytes = pick("prefetch_hit_bytes", "prefetchHitBytes")
	b.PrefetchHits = pick("prefetch_hits", "prefetchHits")
	b.WarmCacheHits = pick("warm_cache_hits", "warmCacheHits")
	b.RuntimeDownloads = pick("runtime_downloads", "runtimeDownloads")
	b.WarmCacheBytes = pick("warm_cache_bytes", "warmCacheBytes")
	b.RuntimeDownloadBytes = pick("runtime_download_bytes", "runtimeDownloadBytes")
	b.RequiredAssetBytes = pick("required_asset_bytes", "requiredAssetBytes")
	b.LatestPreparedAtMs = pick("latest_prepared_at_ms", "latestPreparedAtMs")
	b.AttemptStartedAtMs = pick("attempt_started_at_ms", "attemptStartedAtMs")
	return nil
}

// PreparedAssetRatio returns the fraction of required assets that were
// prefetched before the attempt started. A value of 1.0 means every
// required asset was materialized by a FutureAssetPlan; 0.0 means none
// were. Returns NaN when no assets were required (division by zero).
func (b AssetPreparationBreakdown) PreparedAssetRatio() float64 {
	if b.AssetsRequired <= 0 {
		return math.NaN()
	}
	return float64(b.PrefetchHits) / float64(b.AssetsRequired)
}

// PreparedByteRatio returns the fraction of required asset bytes that
// were prefetched before the attempt started. This corrects for the
// pathologically misleading case where 25 small assets are prefetched
// (96% by count) but a 600 MB video arrives at runtime (>99% by bytes).
// Returns NaN when no asset bytes were required.
func (b AssetPreparationBreakdown) PreparedByteRatio() float64 {
	if b.RequiredAssetBytes <= 0 {
		return math.NaN()
	}
	return float64(b.PrefetchHitBytes) / float64(b.RequiredAssetBytes)
}

// PrefetchReadyLeadMS returns the millisecond gap between when the
// last required asset was prepared and when the attempt started. A
// positive value proves the prefetch completed before execution; zero
// or negative means the attempt started before preparation finished.
// Returns 0 when either timestamp is unset (missing telemetry).
func (b AssetPreparationBreakdown) PrefetchReadyLeadMS() int64 {
	if b.AttemptStartedAtMs <= 0 || b.LatestPreparedAtMs <= 0 {
		return 0
	}
	return b.AttemptStartedAtMs - b.LatestPreparedAtMs
}
