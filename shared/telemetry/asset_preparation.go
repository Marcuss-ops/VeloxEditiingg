package telemetry

import (
	"encoding/json"
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
	return nil
}
