// PR-5 / Scorecard v1 / F1 — typed proto→Go conversion helpers.
//
// Background. The wire-format message TaskResult.execution_metrics is the
// typed *controlpb.TaskExecutionMetrics message (proto v3). The master
// side already had IngestTaskResult close-write + artifact-register + Job
// roll-up wired end-to-end, but the typed metrics went unread at the gRPC
// handler — they lived only inside the proto envelope and never landed on
// task_attempt_metrics / task_attempt_cache_stats / task_attempt_cost_basis
// (migration 054 columns), leaving the scorecard exporter without data.
//
// These helpers build the typed Go structs from the wire payload. They are
// pure functions (no DB / no clock) so they're trivially testable. CacheStats
// is built with the hybrid (b) approach recommended by the scorecard review:
//
//   - CacheBytesUsed = BytesFromLocalCache (the only byte-volume sidecar
//     the worker can confidently surface today).
//   - CacheHits/Misses/Evictions/Corruptions/Entries = 0. The worker
//     doesn't yet surface these counters on the typed payload; the WARN
//     log emitted from handleTaskResult will be the clean signal for
//     PR-3 (worker-side resource sampler) to add the missing fields.
//
// CostBasis derives totals from the per-attempt scalars the worker DOES
// emit (cp.TimeSeconds = CPUTimeMS/1000, etc.) and combines them with the
// three price fields on the wire. Missing network egress is set to 0 —
// the worker can grow the proto if/when needed (no schema migration
// required for the call sites that read it; the typed column already
// exists on task_attempt_cost_basis).
package grpcserver

import (
	"log"
	"sync"

	"velox-server/internal/taskattempts"

	pb "velox-shared/controltransport/pb"
)

// segmentTimingsFromProto maps the worker's per-segment C++ sidecar
// timings onto the canonical taskattempts.SegmentTiming shape. Empty
// or nil input returns nil.
func segmentTimingsFromProto(attemptID, taskID, jobID, workerID string, protoSegments []*pb.SegmentTiming) []taskattempts.SegmentTiming {
	if len(protoSegments) == 0 {
		return nil
	}
	segments := make([]taskattempts.SegmentTiming, 0, len(protoSegments))
	for _, ps := range protoSegments {
		if ps == nil {
			continue
		}
		segments = append(segments, taskattempts.SegmentTiming{
			AttemptID:        attemptID,
			TaskID:           taskID,
			JobID:            jobID,
			WorkerID:         workerID,
			SegmentIndex:     int(ps.GetSegmentIndex()),
			SceneWorkerIndex: int(ps.GetSceneWorkerIndex()),
			SourceType:       ps.GetSourceType(),
			DurationMS:       ps.GetDurationMs(),
			AssetDownloadMS:  ps.GetAssetDownloadMs(),
			FfmpegEncodeMS:   ps.GetFfmpegEncodeMs(),
			SourceBytes:      ps.GetSourceBytes(),
			OutputBytes:      ps.GetOutputBytes(),
			FramesEncoded:    ps.GetFramesEncoded(),
			Codec:            ps.GetCodec(),
			Preset:           ps.GetPreset(),
			FfmpegThreads:    int(ps.GetFfmpegThreads()),
			Status:           ps.GetStatus(),
			ErrorCode:        ps.GetErrorCode(),
			ErrorMessage:     ps.GetErrorMessage(),
			SourceURLHash:    ps.GetSourceUrlHash(),
			CacheKey:         ps.GetCacheKey(),
			InputDurationMS:  ps.GetInputDurationMs(),
			OutputDurationMS: ps.GetOutputDurationMs(),
			MetadataJSON:     ps.GetMetadataJson(),
		})
	}
	return segments
}

// executionMetricsToAttemptMetrics builds the flat typed AttemptMetrics
// the persistence layer expects. All 17 fields of pb.TaskExecutionMetrics
// are mapped 1:1; missing fields on the wire default to zero (older
// workers / pre-PR-2 bridge that don't yet emit TypedExecutionMetrics).
func executionMetricsToAttemptMetrics(attemptID string, em *pb.TaskExecutionMetrics) taskattempts.AttemptMetrics {
	am := taskattempts.AttemptMetrics{AttemptID: attemptID}
	if em == nil {
		return am
	}

	// Legacy 7 + Scorecard v1 + cost fields — direct proto→struct map.
	am.InputBytes = em.GetInputBytes()
	am.OutputBytes = em.GetOutputBytes()
	am.BytesFromDrive = em.GetBytesFromDrive()
	am.BytesFromBlobstore = em.GetBytesFromBlobstore()
	am.BytesFromLocalCache = em.GetBytesFromLocalCache()
	am.CPUTimeMS = em.GetCpuTimeMs()
	am.PeakRSSBytes = em.GetPeakRssBytes()
	am.FramesDecoded = em.GetFramesDecoded()
	am.FramesComposited = em.GetFramesComposited()
	am.FramesEncoded = em.GetFramesEncoded()
	am.FFmpegSpeedRatio = em.GetFfmpegSpeedRatio()
	am.EncodePasses = em.GetEncodePasses()
	am.FinalConcatStreamCopy = em.GetFinalConcatStreamCopy()
	am.ConcatMode = em.GetConcatMode()
	// TempBytesWritten / DuplicateDownloadBytes / MediaDurationSeconds /
	// WallClockSeconds are NOT yet carried on the typed proto
	// (proto v3 / F1 cutover). They live on the SQL schema (migration 054)
	// and will be surfaced on the wire in a follow-up proto bump.
	// Persisted as zero today; the scorecard derived ratios already
	// handle zero-side safely via early-return branches.
	am.TempBytesWritten = 0
	am.DuplicateDownloadBytes = 0
	am.MediaDurationSeconds = 0
	am.WallClockSeconds = 0

	return am
}

// deriveCacheStats builds the per-attempt cache snapshot the persistence
// layer expects. Today the proto only carries byte volumes, NOT hit/miss/
// evict/corruption counters; the function therefore:
//
//   - sets CacheBytesUsed = BytesFromLocalCache (the only derivable signal),
//   - leaves CacheHits/Misses/Evictions/Corruptions/Entries = 0,
//   - emits a one-line breadcrumb on the WAS-WARNED path at most once per
//     process (callers can downgrade / silence in tests via SetLogger).
//
// PR-3 (worker-agent-go resource sampler follow-up) will surface real
// counters on pb.TaskExecutionMetrics; this helper then becomes a 1:1
// field mapper rather than a derivation.
func deriveCacheStats(attemptID string, am taskattempts.AttemptMetrics) taskattempts.AttemptCacheStats {
	cs := taskattempts.AttemptCacheStats{
		AttemptID:    attemptID,
		CacheEntries: 0,
		// CacheBytesUsed is the one number we can derive honestly today:
		// the worker DID report bytes_from_local_cache, which IS the size
		// of the local cache footprint by construction (downloads land
		// in cache → shadow on scorecard OK; for warm-cache the count
		// will track real cache size after CACHE_SIZE_LIMIT is wired).
		CacheBytesUsed: am.BytesFromLocalCache,
	}
	if am.BytesFromDrive > 0 || am.BytesFromBlobstore > 0 {
		// Cold-warm heuristic on misses: any byte drawn from BlobStore
		// or Drive is by definition a cache miss for the worker's
		// perspective, but mapping that to CacheMisses here would be
		// over-claimed (we don't know how many cache-miss events
		// produced those bytes — could be one big download). Emit the
		// WARN ONCE per process so test runs don't get spammed; the
		// signal an operator wants is "this is still the derivation
		// fallback path during the PR-3 rollout", not "this attempt
		// hit a cache miss 1800 times".
		cacheStatsDerivationWarn.Do(func() {
			log.Printf(
				"[GRPC-METRICS] AttemptCacheStats:CannotDeriveHitsMissesEvictions "+
					"bytes_from_drive=%d bytes_from_blobstore=%d — leaving counters 0; "+
					"PR-3 worker-side resource sampler will surface typed counters",
				am.BytesFromDrive, am.BytesFromBlobstore,
			)
		})
	}
	return cs
}

// cacheStatsDerivationWarn fires at most once per process to avoid
// spamming test fixtures that exercise cold-cache paths hundreds of
// times. Operators retain the signal in production logs because the
// derivation policy fires per cold-start.
var cacheStatsDerivationWarn sync.Once

// executionMetricsToCostBasis builds the cost envelope the persistence
// layer expects. The proto carries the per-pricing-unit price snapshot;
// the master derives the consumption totals from the per-attempt scalars
// already on TaskExecutionMetrics.
//
//	CPUTimeSecondsTotal = CPUTimeMS / 1000
//	StorageGBWritten    = TempBytesWritten / 1e9
//	NetworkGBEgressed   = 0  (no signal today; PR-3 follow-up: C++ engine
//	                         can emit -progress total_size + transport dump)
//	OutputMinutesTotal  = MediaDurationSeconds / 60  (0 today; same caveat
//	                         as wall/ media-duration — they live on the
//	                         tasks-jobs side and aren't on the typed metrics)
//
// All-zero on a 0-byte / old-worker attempt still produces a valid
// (zero) cost row so the scorecard exporter has a stable row to roll up.
func executionMetricsToCostBasis(attemptID string, em *pb.TaskExecutionMetrics) taskattempts.AttemptCostBasis {
	cb := taskattempts.AttemptCostBasis{
		AttemptID:           attemptID,
		CPUTimeSecondsTotal: 0,
		StorageGBWritten:    0,
		NetworkGBEgressed:   0,
		OutputMinutesTotal:  0,
	}
	if em == nil {
		cb.Compute()
		return cb
	}
	cb.CPUPricePerSecond = em.GetCpuPricePerSecond()
	cb.StoragePricePerGB = em.GetStoragePricePerGb()
	cb.NetworkPricePerGB = em.GetNetworkPricePerGb()
	cb.CPUTimeSecondsTotal = float64(em.GetCpuTimeMs()) / 1000.0
	// StorageGBWritten: derives from TempBytesWritten on the SQL column,
	// not yet on the typed proto. Until the worker emits TempBytesWritten
	// on the wire this stays 0 (Compute() short-circuits the
	// per-output-minute divide by zero anyway).
	cb.StorageGBWritten = 0
	// NetworkGBEgressed: TODO PR-3 — surface the artifact-upload
	// transport size on ArtifactUploaded/Heartbeat, then derive here.
	cb.NetworkGBEgressed = 0
	cb.Compute() // fills CostPerOutputMinute
	return cb
}
