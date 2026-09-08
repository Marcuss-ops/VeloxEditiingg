package worker

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/internal/prefetch"
	"velox-worker-agent/internal/telemetry"
)

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

	// latestPreparedAtMs returns the epoch-millisecond timestamp of the
	// most recently prepared asset across all PreparedJobs. The callback
	// is invoked once per OriginPrefetch resolution to track the latest
	// preparation time for prefetch_ready_lead_ms derivation.
	latestPreparedAtMs func() int64
}

func (s cacheResolutionSink) RecordResolution(ctx context.Context, resolution downloader.CacheResolution) {
	// Classify origin for cache hits.
	if resolution.CacheHit && resolution.Origin == "" {
		resolution.Origin = s.classifyOrigin(resolution)
	} else if !resolution.CacheHit && resolution.Origin == "" {
		resolution.Origin = downloader.OriginRuntimeDownload
	}
	if resolution.Origin == downloader.OriginPrefetch && resolution.PreparedAt.IsZero() {
		resolution.PreparedAt = s.preparedAtForResolution(resolution)
	}
	// A miss for an asset that is still covered by a certified PREPARED job is
	// an explicit zero-network invariant violation. Keep this event on the
	// canonical resolution path so it cannot be hidden by a lower-level
	// transferer or duplicated by report builders.
	if !resolution.CacheHit {
		if job, asset, ok := s.preparedResolutionMatch(resolution); ok {
			if rec := telemetry.RecorderFromContext(ctx); rec != nil {
				h := rec.Begin(telemetry.EventSpec{Origin: telemetry.OriginWorker, Scope: telemetry.ScopeTask, Component: "worker.prejob", Action: "prejob_runtime_download_violation"})
				h.SetMetadata("job_id", job.JobID)
				h.SetMetadata("task_id", job.TaskID)
				h.SetMetadata("asset_id", resolution.AssetID)
				h.SetMetadata("asset_key", asset.AssetKey)
				h.SetMetadata("outcome", string(resolution.Outcome))
				h.Abort("PREJOB_RUNTIME_DOWNLOAD", "certified PREPARED asset was not available locally")
			}
			telemetry.GetPrometheusMetrics().RecordPrefetchCorrupted("runtime_download_violation")
		}
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
		// Track the latest prefetch preparation time for
		// prefetch_ready_lead_ms derivation.
		if resolution.Origin == downloader.OriginPrefetch {
			if !resolution.PreparedAt.IsZero() {
				tracker.setLatestPreparedAtMs(resolution.PreparedAt.UnixMilli())
			} else if s.latestPreparedAtMs != nil {
				if ms := s.latestPreparedAtMs(); ms > 0 {
					tracker.setLatestPreparedAtMs(ms)
				}
			}
		}
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

// preparedResolutionMatch finds a certified PREPARED job whose asset identity
// matches a runtime miss. It intentionally uses the same full identity proof
// as prefetch-origin classification, but does not require CacheHit: the miss
// is precisely the violation being reported.
func (s cacheResolutionSink) preparedResolutionMatch(resolution downloader.CacheResolution) (prefetch.PreparedJob, prefetch.PreparedAssetMetadata, bool) {
	if s.preparedJobs == nil {
		return prefetch.PreparedJob{}, prefetch.PreparedAssetMetadata{}, false
	}
	for _, job := range s.preparedJobs() {
		if job.State != prefetch.PreparationStatePrepared {
			continue
		}
		for _, asset := range job.Assets {
			if resolutionPrefetchMatch(resolution, job, asset) {
				return job, asset, true
			}
		}
	}
	return prefetch.PreparedJob{}, prefetch.PreparedAssetMetadata{}, false
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
// scopes to the job/task/worker/asset identity when available.
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
	if resolution.WorkerID != "" && job.WorkerID != "" && resolution.WorkerID != job.WorkerID {
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
//
// Temporal proof: when the resolution carries a non-zero ResolvedAt, the
// PreparedJob entry's PreparedAt must precede it. This prevents stale
// PreparedJob entries from misclassifying re-downloads as prefetch when the
// asset was actually materialized after the original preparation.
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
			// Temporal proof: the asset must have been prepared before the
			// current resolution to prove it was prefetched. When ResolvedAt
			// is zero (legacy/test path), skip the temporal check.
			if !resolution.ResolvedAt.IsZero() && !asset.PreparedAt.IsZero() &&
				!asset.PreparedAt.Before(resolution.ResolvedAt) {
				return downloader.OriginWarmCache
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

func (s cacheResolutionSink) preparedAtForResolution(resolution downloader.CacheResolution) time.Time {
	if s.preparedJobs == nil {
		return time.Time{}
	}
	for _, job := range s.preparedJobs() {
		for _, asset := range job.Assets {
			if resolutionPrefetchMatch(resolution, job, asset) && asset.Origin != downloader.OriginWarmCache {
				return asset.PreparedAt
			}
		}
	}
	return time.Time{}
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
	// Worker-scoped match: when the resolution carries a WorkerID, require
	// it to match the PreparedJob's WorkerID. This prevents a prepared
	// asset from Worker X from classifying a resolution on Worker Y as
	// OriginPrefetch (e.g. after a worker swap or cache migration).
	if resolution.WorkerID != "" && job.WorkerID != "" && resolution.WorkerID != job.WorkerID {
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
