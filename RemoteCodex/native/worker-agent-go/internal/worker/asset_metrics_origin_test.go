package worker

import (
	"testing"

	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/internal/telemetry"
)

// TestAssetPreparationSummary_OriginCounts verifies that the prepSnapshot
// propagates origin counters from AttemptCacheMetrics.
func TestAssetPreparationSummary_OriginCounts(t *testing.T) {
	tracker := &assetOperationTracker{cacheEnabled: true}
	tracker.recordResolution(downloader.CacheResolution{
		AssetID: "prefetch-1", CacheHit: true, Outcome: downloader.CacheOutcomeHitValid,
		Origin: downloader.OriginPrefetch,
	})
	tracker.recordResolution(downloader.CacheResolution{
		AssetID: "warm-1", CacheHit: true, Outcome: downloader.CacheOutcomeHitValid,
		Origin: downloader.OriginWarmCache,
	})
	tracker.recordResolution(downloader.CacheResolution{
		AssetID: "download-1", CacheHit: false, Outcome: downloader.CacheOutcomeMissNotFound,
		Downloaded: true, Origin: downloader.OriginRuntimeDownload,
	})
	prep := tracker.prepSnapshot()
	if prep.PrefetchHits != 1 {
		t.Fatalf("PrefetchHits = %d, want 1", prep.PrefetchHits)
	}
	if prep.WarmCacheHits != 1 {
		t.Fatalf("WarmCacheHits = %d, want 1", prep.WarmCacheHits)
	}
	if prep.RuntimeDownloads != 1 {
		t.Fatalf("RuntimeDownloads = %d, want 1", prep.RuntimeDownloads)
	}
}

// TestAttemptCacheMetrics_ByteCounters verifies that CacheHitBytes,
// CacheMissBytes, and PrefetchHitBytes are correctly accumulated from
// CacheResolution.SizeBytes and CacheResolution.DownloadBytes by the
// single cacheResolutionSink authority.
func TestAttemptCacheMetrics_ByteCounters(t *testing.T) {
	tracker := &assetOperationTracker{cacheEnabled: true}

	// Prefetch hit: 4096 bytes served from prefetch
	tracker.recordResolution(downloader.CacheResolution{
		AssetID: "prefetch-A", CacheHit: true, Outcome: downloader.CacheOutcomeHitValid,
		SizeBytes: 4096, Origin: downloader.OriginPrefetch,
	})
	// Warm cache hit: 8192 bytes served from warm cache
	tracker.recordResolution(downloader.CacheResolution{
		AssetID: "warm-B", CacheHit: true, Outcome: downloader.CacheOutcomeHitValid,
		SizeBytes: 8192, Origin: downloader.OriginWarmCache,
	})
	// Runtime download: 16384 bytes downloaded
	tracker.recordResolution(downloader.CacheResolution{
		AssetID: "cold-C", CacheHit: false, Outcome: downloader.CacheOutcomeMissNotFound,
		Downloaded: true, DownloadBytes: 16384, SizeBytes: 16384,
		Origin: downloader.OriginRuntimeDownload,
	})

	snap := tracker.cacheSnapshot()

	// CacheHitBytes = prefetch hit bytes + warm cache hit bytes
	if snap.CacheHitBytes != 4096+8192 {
		t.Fatalf("CacheHitBytes = %d, want %d", snap.CacheHitBytes, 4096+8192)
	}
	// CacheMissBytes = runtime download bytes
	if snap.CacheMissBytes != 16384 {
		t.Fatalf("CacheMissBytes = %d, want 16384", snap.CacheMissBytes)
	}
	// PrefetchHitBytes = only prefetch hit bytes
	if snap.PrefetchHitBytes != 4096 {
		t.Fatalf("PrefetchHitBytes = %d, want 4096", snap.PrefetchHitBytes)
	}
}

// TestAssetPreparationSummary_ByteCounters verifies that byte counters
// propagate from AttemptCacheMetrics into the preparation summary.
func TestAssetPreparationSummary_ByteCounters(t *testing.T) {
	tracker := &assetOperationTracker{cacheEnabled: true}
	tracker.recordResolution(downloader.CacheResolution{
		AssetID: "p1", CacheHit: true, Outcome: downloader.CacheOutcomeHitValid,
		SizeBytes: 1000, Origin: downloader.OriginPrefetch,
	})
	tracker.recordResolution(downloader.CacheResolution{
		AssetID: "w1", CacheHit: true, Outcome: downloader.CacheOutcomeHitValid,
		SizeBytes: 2000, Origin: downloader.OriginWarmCache,
	})
	tracker.recordResolution(downloader.CacheResolution{
		AssetID: "d1", CacheHit: false, Outcome: downloader.CacheOutcomeMissNotFound,
		Downloaded: true, DownloadBytes: 3000, SizeBytes: 3000,
		Origin: downloader.OriginRuntimeDownload,
	})
	prep := tracker.prepSnapshot()
	if prep.CacheHitBytes != 3000 {
		t.Fatalf("prep.CacheHitBytes = %d, want 3000", prep.CacheHitBytes)
	}
	if prep.CacheMissBytes != 3000 {
		t.Fatalf("prep.CacheMissBytes = %d, want 3000", prep.CacheMissBytes)
	}
	if prep.PrefetchHitBytes != 1000 {
		t.Fatalf("prep.PrefetchHitBytes = %d, want 1000", prep.PrefetchHitBytes)
	}
}

// TestProjectAttemptCacheFacts_ByteCounters verifies that byte counters
// are projected into RawExecutionMetrics.
func TestProjectAttemptCacheFacts_ByteCounters(t *testing.T) {
	cache := AttemptCacheMetrics{
		CacheLookups:       3,
		CacheHits:          2,
		CacheMisses:        1,
		CacheDownloadCount: 1,
		CacheDownloadBytes: 5000,
		CacheHitBytes:      3000,
		CacheMissBytes:     5000,
		PrefetchHitBytes:   2000,
	}
	report := &taskrunner.TaskExecutionReport{}
	projectAttemptCacheFacts(report, cache, nil)
	if report.RawMetrics == nil {
		t.Fatal("RawMetrics is nil after projection")
	}
	if report.RawMetrics.CacheHitBytes != 3000 {
		t.Fatalf("CacheHitBytes = %d, want 3000", report.RawMetrics.CacheHitBytes)
	}
	if report.RawMetrics.CacheMissBytes != 5000 {
		t.Fatalf("CacheMissBytes = %d, want 5000", report.RawMetrics.CacheMissBytes)
	}
	// BytesFromLocalCache must be derived from CacheHitBytes (single chain).
	if report.RawMetrics.BytesFromLocalCache != 3000 {
		t.Fatalf("BytesFromLocalCache = %d, want 3000 (CacheHitBytes)", report.RawMetrics.BytesFromLocalCache)
	}
}

// TestProjectAttemptCacheFacts_BytesFromLocalCacheDerivation verifies the
// single-chain projection: BytesFromLocalCache is derived from CacheHitBytes
// (the resolver sink's attempt-scoped cache hit volume), NOT from the
// provider's total cache size.
func TestProjectAttemptCacheFacts_BytesFromLocalCacheDerivation(t *testing.T) {
	cache := AttemptCacheMetrics{
		CacheLookups:         5,
		CacheHits:            3,
		CacheMisses:          2,
		CacheDownloadCount:   2,
		CacheDownloadBytes:   8000,
		CacheHitBytes:        12000,
		CacheMissBytes:       8000,
		PrefetchHitBytes:     5000,
		PrefetchHitCount:     1,
		OriginPrefetchCount:  1,
		OriginWarmCacheCount: 2,
		OriginDownloadCount:  2,
	}

	// Pre-set BytesFromLocalCache to a stale provider value (total cache
	// size) to verify the projection overwrites it with the sink's
	// attempt-scoped CacheHitBytes.
	report := &taskrunner.TaskExecutionReport{
		RawMetrics: &telemetry.RawExecutionMetrics{
			BytesFromLocalCache: 999999, // stale provider total
		},
	}
	projectAttemptCacheFacts(report, cache, nil)
	if report.RawMetrics == nil {
		t.Fatal("RawMetrics is nil after projection")
	}
	// BytesFromLocalCache must equal CacheHitBytes (single-chain derivation).
	if report.RawMetrics.BytesFromLocalCache != 12000 {
		t.Fatalf("BytesFromLocalCache = %d, want 12000 (CacheHitBytes)", report.RawMetrics.BytesFromLocalCache)
	}
	if report.RawMetrics.CacheHitBytes != 12000 {
		t.Fatalf("CacheHitBytes = %d, want 12000", report.RawMetrics.CacheHitBytes)
	}
	if report.RawMetrics.JobPrefetchBytes != 5000 {
		t.Fatalf("JobPrefetchBytes = %d, want 5000 (PrefetchHitBytes)", report.RawMetrics.JobPrefetchBytes)
	}
}

// TestProjectAttemptCacheFacts_ZeroLookupsPreservesExisting verifies that
// when the resolver observes zero lookups, the projection does NOT touch
// BytesFromLocalCache (preserving any legacy value set by mergeStatsInto).
func TestProjectAttemptCacheFacts_ZeroLookupsPreservesExisting(t *testing.T) {
	report := &taskrunner.TaskExecutionReport{
		RawMetrics: &telemetry.RawExecutionMetrics{
			BytesFromLocalCache: 42000,
		},
	}
	projectAttemptCacheFacts(report, AttemptCacheMetrics{}, nil)
	// Zero lookups = no resolver observation → preserve existing value.
	if report.RawMetrics.BytesFromLocalCache != 42000 {
		t.Fatalf("BytesFromLocalCache = %d, want 42000 (preserved)", report.RawMetrics.BytesFromLocalCache)
	}
}

// TestAttemptCacheMetrics_PrefetchHitCount verifies that PrefetchHitCount
// is incremented only for prefetch-origin resolutions.
func TestAttemptCacheMetrics_PrefetchHitCount(t *testing.T) {
	tracker := &assetOperationTracker{cacheEnabled: true}

	// Prefetch hit: count should increment
	tracker.recordResolution(downloader.CacheResolution{
		AssetID: "prefetch-A", CacheHit: true, Outcome: downloader.CacheOutcomeHitValid,
		SizeBytes: 4096, Origin: downloader.OriginPrefetch,
	})
	// Warm cache hit: count should NOT increment
	tracker.recordResolution(downloader.CacheResolution{
		AssetID: "warm-B", CacheHit: true, Outcome: downloader.CacheOutcomeHitValid,
		SizeBytes: 8192, Origin: downloader.OriginWarmCache,
	})
	// Runtime download: count should NOT increment
	tracker.recordResolution(downloader.CacheResolution{
		AssetID: "cold-C", CacheHit: false, Outcome: downloader.CacheOutcomeMissNotFound,
		Downloaded: true, DownloadBytes: 16384, SizeBytes: 16384,
		Origin: downloader.OriginRuntimeDownload,
	})
	// Another prefetch hit
	tracker.recordResolution(downloader.CacheResolution{
		AssetID: "prefetch-D", CacheHit: true, Outcome: downloader.CacheOutcomeHitValid,
		SizeBytes: 2048, Origin: downloader.OriginPrefetch,
	})

	snap := tracker.cacheSnapshot()
	if snap.PrefetchHitCount != 2 {
		t.Fatalf("PrefetchHitCount = %d, want 2", snap.PrefetchHitCount)
	}
	if snap.OriginPrefetchCount != 2 {
		t.Fatalf("OriginPrefetchCount = %d, want 2", snap.OriginPrefetchCount)
	}
	if snap.PrefetchHitBytes != 4096+2048 {
		t.Fatalf("PrefetchHitBytes = %d, want %d", snap.PrefetchHitBytes, 4096+2048)
	}
}
