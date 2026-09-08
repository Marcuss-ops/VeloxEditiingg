package worker

import (
	"context"
	"testing"
	"time"

	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/internal/prefetch"
)

func TestCacheResolutionSink_OriginClassification(t *testing.T) {
	// Prepare a sink with a PreparedJob that has asset-SHA matching
	preparedJobs := []prefetch.PreparedJob{
		{
			JobID:  "job-B",
			TaskID: "task-B",
			State:  "PREPARED",
			Assets: map[string]prefetch.PreparedAssetMetadata{
				"asset-B": {SHA256: "sha-prefetch-A", SizeBytes: 1024},
			},
		},
	}
	sink := cacheResolutionSink{
		preparedJobs: func() []prefetch.PreparedJob { return preparedJobs },
	}

	// Test 1: cache hit with matching PreparedJob → OriginPrefetch
	tracker1 := &assetOperationTracker{cacheEnabled: true}
	ctx1 := withAssetOperationTracker(context.Background(), tracker1)
	sink.RecordResolution(ctx1, downloader.CacheResolution{
		AssetID: "asset-B", CacheHit: true, Outcome: downloader.CacheOutcomeHitValid,
		SHA256: "sha-prefetch-A",
	})
	snap1 := tracker1.cacheSnapshot()
	if snap1.OriginPrefetchCount != 1 {
		t.Fatalf("OriginPrefetchCount = %d, want 1", snap1.OriginPrefetchCount)
	}
	if snap1.OriginWarmCacheCount != 0 {
		t.Fatalf("OriginWarmCacheCount = %d, want 0", snap1.OriginWarmCacheCount)
	}

	// Test 2: cache hit without matching PreparedJob → OriginWarmCache
	tracker2 := &assetOperationTracker{cacheEnabled: true}
	ctx2 := withAssetOperationTracker(context.Background(), tracker2)
	sink.RecordResolution(ctx2, downloader.CacheResolution{
		AssetID: "asset-C", CacheHit: true, Outcome: downloader.CacheOutcomeHitValid,
		SHA256: "sha-warm-cache",
	})
	snap2 := tracker2.cacheSnapshot()
	if snap2.OriginWarmCacheCount != 1 {
		t.Fatalf("OriginWarmCacheCount = %d, want 1", snap2.OriginWarmCacheCount)
	}
	if snap2.OriginPrefetchCount != 0 {
		t.Fatalf("OriginPrefetchCount = %d, want 0", snap2.OriginPrefetchCount)
	}

	// Test 3: cache miss → OriginRuntimeDownload
	tracker3 := &assetOperationTracker{cacheEnabled: true}
	ctx3 := withAssetOperationTracker(context.Background(), tracker3)
	sink.RecordResolution(ctx3, downloader.CacheResolution{
		AssetID: "asset-D", CacheHit: false, Outcome: downloader.CacheOutcomeMissNotFound,
		Downloaded: true, DownloadBytes: 4096,
	})
	snap3 := tracker3.cacheSnapshot()
	if snap3.OriginDownloadCount != 1 {
		t.Fatalf("OriginDownloadCount = %d, want 1", snap3.OriginDownloadCount)
	}
}

// TestCacheResolutionSink_NilPreparedJobsClassifiesAsWarmCache verifies that
// when no PreparedJobs callback is wired (nil), all cache hits are classified
// as OriginWarmCache. This is the safe default for tests that don't set up
// the prefetch scheduler.
func TestCacheResolutionSink_NilPreparedJobsClassifiesAsWarmCache(t *testing.T) {
	sink := cacheResolutionSink{preparedJobs: nil}
	tracker := &assetOperationTracker{cacheEnabled: true}
	ctx := withAssetOperationTracker(context.Background(), tracker)
	sink.RecordResolution(ctx, downloader.CacheResolution{
		AssetID: "asset-E", CacheHit: true, Outcome: downloader.CacheOutcomeHitValid,
		SHA256: "sha-any",
	})
	snap := tracker.cacheSnapshot()
	if snap.OriginWarmCacheCount != 1 {
		t.Fatalf("OriginWarmCacheCount = %d, want 1 (nil preparedJobs defaults to warm_cache)", snap.OriginWarmCacheCount)
	}
}

// TestCacheResolutionSink_ClassifyOriginPrefersMetadataOrigin verifies that
// classifyOrigin prefers the Origin field from PreparedAssetMetadata when
// available, falling back to the SHA-based heuristic.
func TestCacheResolutionSink_ClassifyOriginPrefersMetadataOrigin(t *testing.T) {
	preparedAt := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	// PreparedJob with an asset that has Origin explicitly set to prefetch.
	preparedJobs := []prefetch.PreparedJob{
		{
			JobID:  "job-X",
			TaskID: "task-X",
			State:  "PREPARED",
			Assets: map[string]prefetch.PreparedAssetMetadata{
				"asset-X": {
					SHA256: "sha-x", SizeBytes: 2048,
					Origin:     downloader.OriginPrefetch,
					PreparedAt: preparedAt,
				},
			},
		},
	}
	sink := cacheResolutionSink{
		preparedJobs: func() []prefetch.PreparedJob { return preparedJobs },
	}

	// Cache hit with matching SHA → should use the metadata's Origin.
	tracker := &assetOperationTracker{cacheEnabled: true}
	ctx := withAssetOperationTracker(context.Background(), tracker)
	sink.RecordResolution(ctx, downloader.CacheResolution{
		AssetID: "asset-X", CacheHit: true, Outcome: downloader.CacheOutcomeHitValid,
		SHA256: "sha-x",
	})
	snap := tracker.cacheSnapshot()
	if snap.OriginPrefetchCount != 1 {
		t.Fatalf("OriginPrefetchCount = %d, want 1 (metadata.Origin= prefetch)", snap.OriginPrefetchCount)
	}
	if got := tracker.prepSnapshot().LatestPreparedAtMs; got != preparedAt.UnixMilli() {
		t.Fatalf("LatestPreparedAtMs = %d, want %d", got, preparedAt.UnixMilli())
	}

	// PreparedJob with Origin set to warm_cache (edge case: asset was in cache
	// when plan arrived but not downloaded by the plan itself).
	preparedJobs2 := []prefetch.PreparedJob{
		{
			JobID:  "job-Y",
			TaskID: "task-Y",
			State:  "PREPARED",
			Assets: map[string]prefetch.PreparedAssetMetadata{
				"asset-Y": {
					SHA256: "sha-y", SizeBytes: 4096,
					Origin: downloader.OriginWarmCache,
				},
			},
		},
	}
	sink2 := cacheResolutionSink{
		preparedJobs: func() []prefetch.PreparedJob { return preparedJobs2 },
	}
	tracker2 := &assetOperationTracker{cacheEnabled: true}
	ctx2 := withAssetOperationTracker(context.Background(), tracker2)
	sink2.RecordResolution(ctx2, downloader.CacheResolution{
		AssetID: "asset-Y", CacheHit: true, Outcome: downloader.CacheOutcomeHitValid,
		SHA256: "sha-y",
	})
	snap2 := tracker2.cacheSnapshot()
	if snap2.OriginWarmCacheCount != 1 {
		t.Fatalf("OriginWarmCacheCount = %d, want 1 (metadata.Origin = warm_cache)", snap2.OriginWarmCacheCount)
	}
	if snap2.OriginPrefetchCount != 0 {
		t.Fatalf("OriginPrefetchCount = %d, want 0", snap2.OriginPrefetchCount)
	}
}
