package observability

import (
	"encoding/json"
	"strconv"
	"time"

	sharedtelemetry "velox-shared/telemetry"
)

type rawWaterfallReport struct {
	Waterfall []struct {
		Name        string    `json:"name"`
		StartedAt   time.Time `json:"startedAt"`
		CompletedAt time.Time `json:"completedAt"`
		DurationMS  int64     `json:"durationMs"`
		Status      string    `json:"status"`
	} `json:"waterfall"`
}

type rawAttemptMilestone struct {
	Name       string          `json:"name"`
	Sequence   json.RawMessage `json:"sequence"`
	ElapsedMS  json.RawMessage `json:"elapsed_ms"`
	ElapsedMs  json.RawMessage `json:"elapsedMs"`
	OccurredAt string          `json:"occurred_at"`
}

type rawAttemptMilestoneReport struct {
	Milestones []rawAttemptMilestone `json:"milestones"`
	// AssetPreparation is the STEP D drill-down inside the asset_preparation
	// bucket, carried on the durable TaskResult. protojson emits camelCase
	// (assetPreparation); the snake_case alias accepts hand-written reports.
	AssetPreparationCamel json.RawMessage `json:"assetPreparation"`
	AssetPreparationSnake json.RawMessage `json:"asset_preparation"`
}

// parseAttemptMilestoneSamples decodes the worker's monotonic milestone
// timeline from any raw report payload. It accepts both wire spellings of
// elapsed_ms (snake_case heartbeat JSON, camelCase protojson TaskResult).
func parseAttemptMilestoneSamples(raw string) []sharedtelemetry.AttemptMilestoneSample {
	if raw == "" {
		return nil
	}
	var report rawAttemptMilestoneReport
	if json.Unmarshal([]byte(raw), &report) != nil || len(report.Milestones) == 0 {
		return nil
	}
	samples := make([]sharedtelemetry.AttemptMilestoneSample, 0, len(report.Milestones))
	for _, milestone := range report.Milestones {
		if milestone.Name == "" {
			continue
		}
		elapsed := parseJSONInt(milestone.ElapsedMS)
		if elapsed == 0 {
			elapsed = parseJSONInt(milestone.ElapsedMs)
		}
		samples = append(samples, sharedtelemetry.AttemptMilestoneSample{
			Name: sharedtelemetry.AttemptMilestone(milestone.Name), Sequence: parseJSONUint(milestone.Sequence),
			ElapsedMS: elapsed, OccurredAt: milestone.OccurredAt,
		})
	}
	return samples
}

// decodeAttemptWaterfall reads the worker's monotonic milestone timeline from
// the durable raw report. It deliberately returns no waterfall when the
// report predates milestone support; callers must expose that as unknown.
func decodeAttemptWaterfall(raw string, attemptID string, wallMS int64) *AttemptWaterfall {
	if raw == "" || wallMS < 0 {
		return nil
	}
	samples := parseAttemptMilestoneSamples(raw)
	if len(samples) == 0 {
		return nil
	}
	waterfall := BuildAttemptWaterfall(attemptID, samples, wallMS)
	// Attach the worker's measured asset-preparation drill-down verbatim when
	// present; a malformed/absent block stays nil rather than becoming zeros.
	waterfall.AssetPreparation = parseAssetPreparationDrillDown(raw)
	return &waterfall
}

// parseAssetPreparationDrillDown extracts the STEP D asset-preparation
// breakdown from a raw TaskResult payload. protojson emits camelCase
// (assetPreparation); the snake_case alias accepts hand-written reports. A
// missing or malformed block stays nil — absence is observable, a silent
// zero is not.
func parseAssetPreparationDrillDown(raw string) *sharedtelemetry.AssetPreparationBreakdown {
	if raw == "" {
		return nil
	}
	var report rawAttemptMilestoneReport
	if json.Unmarshal([]byte(raw), &report) != nil {
		return nil
	}
	prepRaw := report.AssetPreparationCamel
	if len(prepRaw) == 0 || string(prepRaw) == "null" {
		prepRaw = report.AssetPreparationSnake
	}
	if len(prepRaw) == 0 || string(prepRaw) == "null" {
		return nil
	}
	var prep sharedtelemetry.AssetPreparationBreakdown
	var fields map[string]json.RawMessage
	if json.Unmarshal(prepRaw, &fields) != nil {
		return nil
	}
	// TaskResult is encoded with protojson camelCase, while hand-written
	// heartbeat/durable fixtures use the shared telemetry snake_case names.
	// Accept both spellings at this boundary without changing the shared wire
	// type's canonical JSON tags.
	get := func(snake, camel string) int64 {
		if value, ok := fields[camel]; ok {
			return parseJSONInt(value)
		}
		return parseJSONInt(fields[snake])
	}
	prep = sharedtelemetry.AssetPreparationBreakdown{
		AssetsRequired:          get("assets_required", "assetsRequired"),
		AssetsUnique:            get("assets_unique", "assetsUnique"),
		CacheHits:               get("cache_hits", "cacheHits"),
		CacheMisses:             get("cache_misses", "cacheMisses"),
		ReadyBeforeAttempt:      get("ready_before_attempt", "readyBeforeAttempt"),
		DownloadedDuringAttempt: get("downloaded_during_attempt", "downloadedDuringAttempt"),
		CacheLookupMS:           get("cache_lookup_ms", "cacheLookupMs"),
		RemoteWaitMS:            get("remote_wait_ms", "remoteWaitMs"),
		RemoteWaitCount:         get("remote_wait_count", "remoteWaitCount"),
		DownloadWallMS:          get("download_wall_ms", "downloadWallMs"),
		DownloadWorkMS:          get("download_work_ms", "downloadWorkMs"),
		HashVerifyMS:            get("hash_verify_ms", "hashVerifyMs"),
		MetadataProbeMS:         get("metadata_probe_ms", "metadataProbeMs"),
		MaterializeLocalMS:      get("materialize_local_ms", "materializeLocalMs"),
		CacheHitBytes:           get("cache_hit_bytes", "cacheHitBytes"),
		CacheMissBytes:          get("cache_miss_bytes", "cacheMissBytes"),
		PrefetchHitBytes:        get("prefetch_hit_bytes", "prefetchHitBytes"),
		PrefetchHits:            get("prefetch_hits", "prefetchHits"),
		WarmCacheHits:           get("warm_cache_hits", "warmCacheHits"),
		RuntimeDownloads:        get("runtime_downloads", "runtimeDownloads"),
	}
	return &prep
}

func parseJSONInt(raw json.RawMessage) int64 {
	if len(raw) == 0 || string(raw) == "null" {
		return 0
	}
	var value int64
	if json.Unmarshal(raw, &value) == nil {
		return value
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		value, _ = strconv.ParseInt(text, 10, 64)
	}
	return value
}

func parseJSONUint(raw json.RawMessage) uint64 {
	if len(raw) == 0 || string(raw) == "null" {
		return 0
	}
	var value uint64
	if json.Unmarshal(raw, &value) == nil {
		return value
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		value, _ = strconv.ParseUint(text, 10, 64)
	}
	return value
}

func decodeWaterfall(raw string, start, end *time.Time) ([]WaterfallStage, bool) {
	if raw == "" || start == nil || end == nil {
		return nil, false
	}
	var report rawWaterfallReport
	if json.Unmarshal([]byte(raw), &report) != nil || len(report.Waterfall) == 0 {
		return nil, false
	}
	result := make([]WaterfallStage, 0, len(report.Waterfall))
	var previous time.Time
	for _, stage := range report.Waterfall {
		if stage.Name == "" || stage.StartedAt.IsZero() || stage.CompletedAt.Before(stage.StartedAt) || stage.StartedAt.Before(*start) || stage.CompletedAt.After(*end) || (!previous.IsZero() && stage.StartedAt.Before(previous)) || stage.DurationMS != stage.CompletedAt.Sub(stage.StartedAt).Milliseconds() {
			return nil, false
		}
		result = append(result, WaterfallStage{Name: stage.Name, StartedAt: stage.StartedAt, CompletedAt: stage.CompletedAt, DurationMS: stage.DurationMS, Status: stage.Status})
		previous = stage.CompletedAt
	}
	return result, true
}
