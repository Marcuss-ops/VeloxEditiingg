package downloader

// cache_resolution.go — the canonical structured outcome of one asset cache
// resolution, plus the single-emission resolver adapter.
//
// Phase A1 contract: cache telemetry is emitted exactly once per logical
// resolution, inside CacheResolver.Resolve. Every consumer (the per-attempt
// attempt metrics and the worker-lifetime Prometheus counters) is fed from
// this one point; no handler, adapter, transferer or report builder may
// re-count lookups, hits, misses or downloads.
//
// The outcome is classified at the actual lookup point (Transferer.Check),
// never re-derived by callers. The transferer reports CacheCheckResult.Outcome
// and the manager carries it onto DownloadedAsset; CacheResolver only
// projects that classification onto the structured shape.

import (
	"context"
	"errors"
	"time"

	"velox-shared/assetref"
)

// CacheOutcome is the canonical classification of one cache lookup. It is
// decided at the lookup point and is the ONLY vocabulary consumers use to
// reason about cache behaviour.
type CacheOutcome string

const (
	// CacheOutcomeHitValid: a verified on-disk file matched the requested
	// integrity metadata; zero bytes were downloaded.
	CacheOutcomeHitValid CacheOutcome = "HIT_VALID"
	// CacheOutcomeMissNotFound: no cache entry and no verified on-disk file
	// exists for the requested identity.
	CacheOutcomeMissNotFound CacheOutcome = "MISS_NOT_FOUND"
	// CacheOutcomeMissInvalid: an entry exists but is incomplete or its size
	// does not match the requested contract.
	CacheOutcomeMissInvalid CacheOutcome = "MISS_INVALID"
	// CacheOutcomeMissHashMismatch: an entry exists but its content hash
	// does not match the requested SHA-256 (corrupt or foreign bytes).
	CacheOutcomeMissHashMismatch CacheOutcome = "MISS_HASH_MISMATCH"
	// CacheOutcomeMissExpired: the durable index claims a complete entry but
	// the physical file is gone (evicted/expired underneath the index).
	CacheOutcomeMissExpired CacheOutcome = "MISS_EXPIRED"
)

// IsHit reports whether the outcome served the request from a verified
// local file without downloading.
func (o CacheOutcome) IsHit() bool { return o == CacheOutcomeHitValid }

// IsMiss reports whether the outcome is a classified miss. The empty string
// is intentionally not a miss: it means "no classification was produced".
func (o CacheOutcome) IsMiss() bool { return o != "" && !o.IsHit() }

// CacheSource is the low-cardinality origin of the resolved bytes. It is a
// fixed vocabulary so dashboards never see free-form strings.
type CacheSource string

const (
	// CacheSourceLocalDisk: the asset was served from the verified local
	// cache (HIT_VALID).
	CacheSourceLocalDisk CacheSource = "local_disk"
	// CacheSourceMaster: the bytes came from the master asset bridge
	// (download path, including classified misses).
	CacheSourceMaster CacheSource = "master_bridge"
)

// ResolutionOrigin classifies WHY an asset was already local at resolution
// time. The three cases are mutually exclusive and cover every resolution
// outcome:
//
//   - OriginPrefetch: the asset was downloaded by a FutureAssetPlan before
//     the current attempt started. A PreparedJob entry with matching SHA256
//     and size exists.
//   - OriginWarmCache: the asset was already local from a previous job or
//     session (cache hit without a PreparedJob entry).
//   - OriginRuntimeDownload: the asset was not local and was downloaded
//     during the current attempt (cache miss).
//
// The origin eliminates the ambiguity "was B fast because of prefetch or
// because A happened to fill the cache?" — the definitive answer comes from
// the PreparedJob evidence, not from the cache hit flag alone.
type ResolutionOrigin string

const (
	// OriginWarmCache: cache hit without a PreparedJob entry. The asset was
	// already local from a prior job, session, or manual seeding.
	OriginWarmCache ResolutionOrigin = "warm_cache"
	// OriginPrefetch: cache hit with a matching PreparedJob entry. The asset
	// was materialized by a FutureAssetPlan before the attempt started.
	OriginPrefetch ResolutionOrigin = "prefetch"
	// OriginRuntimeDownload: cache miss; bytes were transferred during the
	// current attempt.
	OriginRuntimeDownload ResolutionOrigin = "runtime_download"
)

// CacheResolution is the structured, telemetry-ready outcome of one asset
// resolution. It is the ONLY shape consumers read for cache accounting: the
// per-attempt counters (AttemptCacheMetrics) and the worker-lifetime
// Prometheus view are both derived from a single RecordResolution call.
type CacheResolution struct {
	AssetID string
	Outcome CacheOutcome
	// LocalPath is the verified local path when the resolution succeeded.
	LocalPath string
	// CacheHit mirrors Outcome.IsHit() for the legacy boolean consumers.
	CacheHit bool
	// Downloaded reports whether bytes were transferred from the source.
	Downloaded bool
	// DownloadBytes is the number of bytes transferred on the miss path
	// (0 on a verified hit).
	DownloadBytes int64
	// SizeBytes is the total size of the asset. On hits it comes from the
	// request contract (req.SizeBytes); on downloads it comes from the
	// transfer result. It is the authoritative byte count for cache_hit_bytes
	// and cache_miss_bytes attribution.
	SizeBytes int64
	Source    CacheSource
	SHA256    assetref.ContentHash
	// Timing carries the observable per-transfer sub-phase breakdown for the
	// per-attempt asset-preparation aggregator. Zero on hits and legacy paths.
	Timing AssetSubPhases
	// Origin classifies WHY the asset was local. Set by the resolution sink
	// after consulting the PreparedJob read model. Empty on L1-cache hits
	// where the origin is inherently prefetch (memory cache is populated
	// exclusively by the prefetch path).
	Origin ResolutionOrigin

	// JobID, TaskID and AssetKey carry the request identity so that
	// classifyOrigin can scope the PreparedJob lookup to the specific
	// job/task/asset, preventing cross-job SHA collisions from producing
	// false OriginPrefetch classifications.
	JobID    string
	TaskID   string
	AssetKey assetref.AssetKey

	// ResolvedAt records the wall-clock time when the asset was resolved
	// (cache hit verified or download completed). For OriginPrefetch
	// classification the temporal proof requires PreparedAt < ResolvedAt:
	// the asset must have been materialized before the current resolution
	// to prove it was prefetched rather than coincidentally warm.
	ResolvedAt time.Time
}

// ResolutionSink observes each completed resolution exactly once. It runs on
// the caller's resolution goroutine, so it MUST be non-blocking: value reads,
// in-memory counters and cheap structured events only — never I/O that could
// stall a task.
type ResolutionSink interface {
	RecordResolution(ctx context.Context, resolution CacheResolution)
}

// L1Cache is an optional verified memory-backed cache above the canonical
// manager. It is deliberately a lookup/copy seam; the durable NVMe cache and
// downloader remain the source of truth.
type L1Cache interface {
	Find(context.Context, DownloadRequest) (DownloadedAsset, bool, error)
	Put(context.Context, DownloadRequest, DownloadedAsset) error
}

// CacheResolver is the canonical structured-resolution surface. Resolve
// returns the cache accounting outcome and, when a sink is wired, emits the
// cache telemetry exactly once per completed resolution. This is the single
// point where cache lookups are counted.
type CacheResolver struct {
	manager AssetDownloadManager
	sink    ResolutionSink
	l1      L1Cache
}

// NewCacheResolver wraps a manager with the optional telemetry sink.
func NewCacheResolver(manager AssetDownloadManager, sink ResolutionSink) *CacheResolver {
	return &CacheResolver{manager: manager, sink: sink}
}

func (r *CacheResolver) SetL1Cache(l1 L1Cache) {
	if r != nil {
		r.l1 = l1
	}
}

// Resolve returns the structured resolution for req. The metric is emitted
// exactly once per lookup:
//
//   - success → one RecordResolution with the full classified outcome;
//   - non-cancellation failure → one RecordResolution classified as a miss
//     (the asset was looked up, was not served from a verified local file
//     and no bytes were obtained) — this keeps the lookups = hits + misses
//     accounting invariant honest and mirrors the historical worker
//     Prometheus behaviour of counting a started download as a miss;
//   - caller cancellation (ctx.Canceled / context.DeadlineExceeded) and
//     ErrEmptyKey → nothing is recorded (no lookup outcome exists).
func (r *CacheResolver) Resolve(ctx context.Context, req DownloadRequest) (CacheResolution, error) {
	if r == nil || r.manager == nil {
		return CacheResolution{}, ErrEmptyKey
	}
	if r.l1 != nil {
		if asset, ok, err := r.l1.Find(ctx, req); err != nil {
			return CacheResolution{}, err
		} else if ok {
			resolution := CacheResolution{AssetID: req.AssetID, Outcome: CacheOutcomeHitValid, LocalPath: asset.LocalPath, CacheHit: true, Source: CacheSourceLocalDisk, SHA256: asset.SHA256, SizeBytes: req.SizeBytes, JobID: req.JobID, TaskID: req.TaskID, AssetKey: req.AssetKey, ResolvedAt: time.Now().UTC()}
			if r.sink != nil {
				r.sink.RecordResolution(ctx, resolution)
			}
			return resolution, nil
		}
	}
	asset, err := r.manager.Resolve(ctx, req)
	if err != nil {
		if r.sink != nil && !errors.Is(err, ErrEmptyKey) &&
			!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			r.sink.RecordResolution(ctx, CacheResolution{
				AssetID:    req.AssetID,
				Outcome:    CacheOutcomeMissNotFound,
				Source:     CacheSourceMaster,
				JobID:      req.JobID,
				TaskID:     req.TaskID,
				AssetKey:   req.AssetKey,
				ResolvedAt: time.Now().UTC(),
			})
		}
		return CacheResolution{}, err
	}
	resolution := resolutionFromDownloadedAsset(asset, req)
	if r.sink != nil {
		r.sink.RecordResolution(ctx, resolution)
	}
	return resolution, nil
}

// resolutionFromDownloadedAsset projects the manager result onto the
// canonical resolution shape. The outcome was classified at the lookup point
// by Transferer.Check and carried through the transfer; the adapter never
// re-derives hit/miss.
func resolutionFromDownloadedAsset(asset DownloadedAsset, req DownloadRequest) CacheResolution {
	resolution := CacheResolution{
		AssetID:    asset.AssetID,
		Outcome:    asset.Outcome,
		LocalPath:  asset.LocalPath,
		CacheHit:   asset.CacheHit,
		Source:     CacheSourceMaster,
		SHA256:     asset.SHA256,
		Timing:     asset.Timing,
		JobID:      req.JobID,
		TaskID:     req.TaskID,
		AssetKey:   req.AssetKey,
		ResolvedAt: asset.ReadyAt,
	}
	if resolution.AssetID == "" {
		resolution.AssetID = req.AssetID
	}
	if asset.CacheHit {
		resolution.Source = CacheSourceLocalDisk
		resolution.SizeBytes = req.SizeBytes
	} else {
		// The manager reports zero downloaded bytes on the hit path; a
		// positive size therefore means bytes actually transferred.
		resolution.Downloaded = asset.SizeBytes > 0
		resolution.DownloadBytes = asset.SizeBytes
		resolution.SizeBytes = asset.SizeBytes
	}
	// Defensive fallback for legacy transferers (and byte fakes) that do not
	// classify: the CacheHit flag is still an honest outcome.
	if resolution.Outcome == "" {
		if asset.CacheHit {
			resolution.Outcome = CacheOutcomeHitValid
		} else {
			resolution.Outcome = CacheOutcomeMissNotFound
		}
	}
	return resolution
}
