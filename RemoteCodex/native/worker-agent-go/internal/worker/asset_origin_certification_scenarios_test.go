// Package worker — asset-origin certification tests.
//
// These are the three official certification scenarios that prove the
// asset resolution pipeline correctly distinguishes WHY an asset was
// local at attempt time. Each scenario asserts a precise metric
// signature so there is no ambiguity about the origin.
//
// The three origins are:
//
//   - COLD (OriginRuntimeDownload): asset was absent, downloaded during attempt.
//   - WARM (OriginWarmCache): asset was already local from a prior job/session.
//   - PREFETCH (OriginPrefetch): asset was pre-downloaded by FutureAssetPlan.
//
// SHA_A != SHA_B by construction. Each scenario runs with MaxActiveJobs=1
// and uses the cacheResolutionSink to classify origins.
package worker

import (
	"context"
	"testing"
	"time"
	"velox-shared/assetref"

	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/internal/prefetch"
)

func TestCertification_COLD_RuntimeDownload(t *testing.T) {
	payloadB := []byte("COLD-payload-for-asset-B-unique-content")
	shaB := sha256hex(payloadB)

	runCertScenario(t, certScenario{
		name:                 "COLD",
		payloadB:             payloadB,
		shaB:                 shaB,
		pathB:                "/cache/asset-B-cold.bin",
		preparedJobsFn:       nil, // no prefetch → no PreparedJob
		wantOrigin:           downloader.OriginRuntimeDownload,
		wantCacheHit:         false,
		wantDownloadCount:    1,
		wantDownloadBytes:    int64(len(payloadB)),
		wantCacheHitBytes:    0,
		wantCacheMissBytes:   int64(len(payloadB)),
		wantPrefetchHitBytes: 0,
	})
}

// ── Scenario 2: WARM ────────────────────────────────────────────────────────
// Asset WAS in cache from a prior job/session, but NO FutureAssetPlan
// pre-downloaded it. No PreparedJob entry matches.
//
// Expected signature:
//
//	origin = warm_cache
//	cache_hit = true
//	downloaded_during_attempt = 0
//	cache_hit_bytes = asset_size
//	cache_miss_bytes = 0
//	prefetch_hit_bytes = 0  (subset of cache_hit_bytes that was prefetch)
func TestCertification_WARM_WarmCache(t *testing.T) {
	payloadB := []byte("WARM-payload-for-asset-B-from-previous-job")
	shaB := sha256hex(payloadB)

	// No PreparedJobs → all cache hits are classified as warm_cache.
	runCertScenario(t, certScenario{
		name:                 "WARM",
		payloadB:             payloadB,
		shaB:                 shaB,
		pathB:                "/cache/asset-B-warm.bin",
		preparedJobsFn:       nil,
		wantOrigin:           downloader.OriginWarmCache,
		wantCacheHit:         true,
		wantDownloadCount:    0,
		wantDownloadBytes:    0,
		wantCacheHitBytes:    int64(len(payloadB)),
		wantCacheMissBytes:   0,
		wantPrefetchHitBytes: 0,
	})
}

// ── Scenario 3: PREFETCH ────────────────────────────────────────────────────
// Asset was NOT in cache initially, then FutureAssetPlan pre-downloaded
// it BEFORE the attempt started. A PreparedJob entry exists with matching
// SHA256 and size.
//
// Expected signature:
//
//	origin = prefetch
//	cache_hit = true
//	downloaded_during_attempt = 0
//	cache_hit_bytes = asset_size
//	cache_miss_bytes = 0
//	prefetch_hit_bytes = asset_size  (ALL cache_hit_bytes are prefetch)
func TestCertification_PREFETCH_FutureAssetPlan(t *testing.T) {
	payloadB := []byte("PREFETCH-payload-for-asset-B-pre-downloaded")
	shaB := sha256hex(payloadB)

	// Prepare a PreparedJob with a matching SHA256 and size.
	preparedJobs := []prefetch.PreparedJob{
		{
			JobID:      "job-B",
			TaskID:     "task-B",
			State:      prefetch.PreparationStatePrepared,
			PreparedAt: time.Now().UTC().Add(-10 * time.Second), // before attempt
			Assets: map[string]prefetch.PreparedAssetMetadata{
				"asset-B": {
					AssetKey:  "asset-B",
					AssetID:   "asset-B",
					SHA256:    shaB,
					SizeBytes: int64(len(payloadB)),
				},
			},
		},
	}

	runCertScenario(t, certScenario{
		name:     "PREFETCH",
		payloadB: payloadB,
		shaB:     shaB,
		pathB:    "/cache/asset-B-prefetch.bin",
		preparedJobsFn: func() []prefetch.PreparedJob {
			return preparedJobs
		},
		wantOrigin:           downloader.OriginPrefetch,
		wantCacheHit:         true,
		wantDownloadCount:    0,
		wantDownloadBytes:    0,
		wantCacheHitBytes:    int64(len(payloadB)),
		wantCacheMissBytes:   0,
		wantPrefetchHitBytes: int64(len(payloadB)),
	})
}

// ── Combined certification: all three scenarios in one attempt ───────────────
// This simulates a real multi-asset attempt where:
//   - asset-COLD is resolved → runtime_download
//   - asset-WARM is resolved → warm_cache
//   - asset-PREFETCH is resolved → prefetch
//
// The combined metric signature must show the correct split.
func TestCertification_Combined_AllOrigins(t *testing.T) {
	payloadCold := []byte("cold-asset-payload-123")
	payloadWarm := []byte("warm-asset-payload-456")
	payloadPrefetch := []byte("prefetch-asset-payload-789")

	shaCold := sha256hex(payloadCold)
	shaWarm := sha256hex(payloadWarm)
	shaPrefetch := sha256hex(payloadPrefetch)

	preparedJobs := []prefetch.PreparedJob{
		{
			JobID:      "job-prefetch",
			TaskID:     "task-prefetch",
			State:      prefetch.PreparationStatePrepared,
			PreparedAt: time.Now().UTC().Add(-10 * time.Second),
			Assets: map[string]prefetch.PreparedAssetMetadata{
				"asset-prefetch": {
					AssetKey:  "asset-prefetch",
					AssetID:   "asset-prefetch",
					SHA256:    shaPrefetch,
					SizeBytes: int64(len(payloadPrefetch)),
				},
			},
		},
	}

	sink := cacheResolutionSink{
		preparedJobs: func() []prefetch.PreparedJob {
			return preparedJobs
		},
	}

	tracker := &assetOperationTracker{cacheEnabled: true}
	ctx := withAssetOperationTracker(context.Background(), tracker)

	// Resolve all three assets.
	sink.RecordResolution(ctx, downloader.CacheResolution{
		AssetID: "asset-cold", Outcome: downloader.CacheOutcomeMissNotFound,
		Downloaded: true, DownloadBytes: int64(len(payloadCold)),
		SizeBytes: int64(len(payloadCold)), Source: downloader.CacheSourceMaster,
		SHA256: assetref.ContentHash(shaCold),
	})
	sink.RecordResolution(ctx, downloader.CacheResolution{
		AssetID: "asset-warm", Outcome: downloader.CacheOutcomeHitValid,
		CacheHit: true, LocalPath: "/cache/asset-warm.bin",
		SizeBytes: int64(len(payloadWarm)), Source: downloader.CacheSourceLocalDisk,
		SHA256: assetref.ContentHash(shaWarm),
	})
	sink.RecordResolution(ctx, downloader.CacheResolution{
		AssetID: "asset-prefetch", Outcome: downloader.CacheOutcomeHitValid,
		CacheHit: true, LocalPath: "/cache/asset-prefetch.bin",
		SizeBytes: int64(len(payloadPrefetch)), Source: downloader.CacheSourceLocalDisk,
		SHA256: assetref.ContentHash(shaPrefetch),
	})

	cache := tracker.cacheSnapshot()
	prep := tracker.prepSnapshot()

	// ── Origin counts ────────────────────────────────────────────────
	if cache.OriginDownloadCount != 1 {
		t.Fatalf("OriginDownloadCount = %d, want 1", cache.OriginDownloadCount)
	}
	if cache.OriginWarmCacheCount != 1 {
		t.Fatalf("OriginWarmCacheCount = %d, want 1", cache.OriginWarmCacheCount)
	}
	if cache.OriginPrefetchCount != 1 {
		t.Fatalf("OriginPrefetchCount = %d, want 1", cache.OriginPrefetchCount)
	}

	// ── Cache accounting ─────────────────────────────────────────────
	if cache.CacheLookups != 3 {
		t.Fatalf("CacheLookups = %d, want 3", cache.CacheLookups)
	}
	if cache.CacheHits != 2 {
		t.Fatalf("CacheHits = %d, want 2 (warm + prefetch)", cache.CacheHits)
	}
	if cache.CacheMisses != 1 {
		t.Fatalf("CacheMisses = %d, want 1 (cold)", cache.CacheMisses)
	}

	// ── Byte attribution ─────────────────────────────────────────────
	wantHitBytes := int64(len(payloadWarm) + len(payloadPrefetch))
	if cache.CacheHitBytes != wantHitBytes {
		t.Fatalf("CacheHitBytes = %d, want %d", cache.CacheHitBytes, wantHitBytes)
	}
	wantMissBytes := int64(len(payloadCold))
	if cache.CacheMissBytes != wantMissBytes {
		t.Fatalf("CacheMissBytes = %d, want %d", cache.CacheMissBytes, wantMissBytes)
	}
	wantPrefetchBytes := int64(len(payloadPrefetch))
	if cache.PrefetchHitBytes != wantPrefetchBytes {
		t.Fatalf("PrefetchHitBytes = %d, want %d", cache.PrefetchHitBytes, wantPrefetchBytes)
	}

	// ── Download accounting ──────────────────────────────────────────
	if cache.CacheDownloadCount != 1 {
		t.Fatalf("CacheDownloadCount = %d, want 1", cache.CacheDownloadCount)
	}
	if cache.CacheDownloadBytes != int64(len(payloadCold)) {
		t.Fatalf("CacheDownloadBytes = %d, want %d", cache.CacheDownloadBytes, len(payloadCold))
	}

	// ── Preparation summary ─────────────────────────────────────────
	if prep.AssetsTotal != 3 {
		t.Fatalf("AssetsTotal = %d, want 3", prep.AssetsTotal)
	}
	if prep.ReadyBefore != 2 {
		t.Fatalf("ReadyBefore = %d, want 2 (warm + prefetch)", prep.ReadyBefore)
	}
	if prep.DownloadedNow != 1 {
		t.Fatalf("DownloadedNow = %d, want 1 (cold)", prep.DownloadedNow)
	}
	if prep.PrefetchHits != 1 {
		t.Fatalf("PrefetchHits = %d, want 1", prep.PrefetchHits)
	}
	if prep.WarmCacheHits != 1 {
		t.Fatalf("WarmCacheHits = %d, want 1", prep.WarmCacheHits)
	}
	if prep.RuntimeDownloads != 1 {
		t.Fatalf("RuntimeDownloads = %d, want 1 (cold asset was downloaded during attempt)", prep.RuntimeDownloads)
	}
}

// ── End-to-end scheduler certification: PREFETCH scenario ───────────────────
// This is the full-stack test: creates a real Scheduler, sends a
// FutureAssetPlan, and verifies that asset-B reaches PREPARED via
// prefetch, then when the attempt resolves asset-B the origin is
// classified as prefetch.
