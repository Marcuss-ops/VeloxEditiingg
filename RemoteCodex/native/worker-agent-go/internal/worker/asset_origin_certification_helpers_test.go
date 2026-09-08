// Package worker — shared helpers for asset-origin certification tests.
package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"velox-shared/assetref"

	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/internal/prefetch"
)

// ── helpers ──────────────────────────────────────────────────────────────────

func sha256hex(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// certScenario is the reusable setup for all three certification scenarios.
type certScenario struct {
	name     string
	payloadB []byte
	shaB     string
	pathB    string
	// preparedJobsFn returns the PreparedJob list for origin classification.
	// nil means no prefetch happened → warm_cache or runtime_download.
	preparedJobsFn func() []prefetch.PreparedJob
	// wantOrigin is the expected ResolutionOrigin for asset-B.
	wantOrigin downloader.ResolutionOrigin
	// wantCacheHit is whether asset-B should be a cache hit.
	wantCacheHit bool
	// wantDownloadCount is the expected download count for asset-B.
	wantDownloadCount int64
	// wantDownloadBytes is the expected download bytes for asset-B.
	wantDownloadBytes int64
	// wantCacheHitBytes is the expected cache hit bytes for asset-B.
	wantCacheHitBytes int64
	// wantCacheMissBytes is the expected cache miss bytes for asset-B.
	wantCacheMissBytes int64
	// wantPrefetchHitBytes is the expected prefetch hit bytes for asset-B.
	wantPrefetchHitBytes int64
}

// runCertScenario executes one certification scenario and asserts the
// full metric signature.
func runCertScenario(t *testing.T, sc certScenario) {
	t.Helper()

	// Build a CacheResolution that simulates the outcome of resolving
	// asset-B in this scenario.
	resolution := downloader.CacheResolution{
		AssetID:   "asset-B",
		Outcome:   downloader.CacheOutcomeHitValid,
		LocalPath: sc.pathB,
		CacheHit:  sc.wantCacheHit,
		SHA256:    assetref.ContentHash(sc.shaB),
		SizeBytes: int64(len(sc.payloadB)),
		Source:    downloader.CacheSourceLocalDisk,
	}
	if !sc.wantCacheHit {
		resolution.Downloaded = true
		resolution.DownloadBytes = int64(len(sc.payloadB))
		resolution.Source = downloader.CacheSourceMaster
	}

	// Wire up the cacheResolutionSink with the PreparedJobs function.
	sink := cacheResolutionSink{
		preparedJobs: sc.preparedJobsFn,
	}

	// Create a tracker and context for the sink.
	tracker := &assetOperationTracker{cacheEnabled: true}
	ctx := withAssetOperationTracker(context.Background(), tracker)

	// Record the resolution through the canonical sink.
	sink.RecordResolution(ctx, resolution)

	// Extract the metric snapshots.
	cache := tracker.cacheSnapshot()
	prep := tracker.prepSnapshot()

	// ── Assert origin classification ──────────────────────────────────
	switch sc.wantOrigin {
	case downloader.OriginPrefetch:
		if cache.OriginPrefetchCount != 1 {
			t.Fatalf("[%s] OriginPrefetchCount = %d, want 1", sc.name, cache.OriginPrefetchCount)
		}
		if cache.OriginWarmCacheCount != 0 {
			t.Fatalf("[%s] OriginWarmCacheCount = %d, want 0", sc.name, cache.OriginWarmCacheCount)
		}
		if cache.OriginDownloadCount != 0 {
			t.Fatalf("[%s] OriginDownloadCount = %d, want 0", sc.name, cache.OriginDownloadCount)
		}
	case downloader.OriginWarmCache:
		if cache.OriginWarmCacheCount != 1 {
			t.Fatalf("[%s] OriginWarmCacheCount = %d, want 1", sc.name, cache.OriginWarmCacheCount)
		}
		if cache.OriginPrefetchCount != 0 {
			t.Fatalf("[%s] OriginPrefetchCount = %d, want 0", sc.name, cache.OriginPrefetchCount)
		}
		if cache.OriginDownloadCount != 0 {
			t.Fatalf("[%s] OriginDownloadCount = %d, want 0", sc.name, cache.OriginDownloadCount)
		}
	case downloader.OriginRuntimeDownload:
		if cache.OriginDownloadCount != 1 {
			t.Fatalf("[%s] OriginDownloadCount = %d, want 1", sc.name, cache.OriginDownloadCount)
		}
		if cache.OriginPrefetchCount != 0 {
			t.Fatalf("[%s] OriginPrefetchCount = %d, want 0", sc.name, cache.OriginPrefetchCount)
		}
		if cache.OriginWarmCacheCount != 0 {
			t.Fatalf("[%s] OriginWarmCacheCount = %d, want 0", sc.name, cache.OriginWarmCacheCount)
		}
	}

	// ── Assert cache hit/miss accounting ───────────────────────────────
	if sc.wantCacheHit {
		if cache.CacheHits != 1 {
			t.Fatalf("[%s] CacheHits = %d, want 1", sc.name, cache.CacheHits)
		}
		if cache.CacheMisses != 0 {
			t.Fatalf("[%s] CacheMisses = %d, want 0", sc.name, cache.CacheMisses)
		}
	} else {
		if cache.CacheMisses != 1 {
			t.Fatalf("[%s] CacheMisses = %d, want 1", sc.name, cache.CacheMisses)
		}
		if cache.CacheHits != 0 {
			t.Fatalf("[%s] CacheHits = %d, want 0", sc.name, cache.CacheHits)
		}
	}

	// ── Assert byte-level attribution ─────────────────────────────────
	if cache.CacheHitBytes != sc.wantCacheHitBytes {
		t.Fatalf("[%s] CacheHitBytes = %d, want %d", sc.name, cache.CacheHitBytes, sc.wantCacheHitBytes)
	}
	if cache.CacheMissBytes != sc.wantCacheMissBytes {
		t.Fatalf("[%s] CacheMissBytes = %d, want %d", sc.name, cache.CacheMissBytes, sc.wantCacheMissBytes)
	}
	if cache.PrefetchHitBytes != sc.wantPrefetchHitBytes {
		t.Fatalf("[%s] PrefetchHitBytes = %d, want %d", sc.name, cache.PrefetchHitBytes, sc.wantPrefetchHitBytes)
	}

	// ── Assert download accounting ────────────────────────────────────
	if cache.CacheDownloadCount != sc.wantDownloadCount {
		t.Fatalf("[%s] CacheDownloadCount = %d, want %d", sc.name, cache.CacheDownloadCount, sc.wantDownloadCount)
	}
	if cache.CacheDownloadBytes != sc.wantDownloadBytes {
		t.Fatalf("[%s] CacheDownloadBytes = %d, want %d", sc.name, cache.CacheDownloadBytes, sc.wantDownloadBytes)
	}

	// ── Assert preparation summary ────────────────────────────────────
	if prep.AssetsTotal != 1 {
		t.Fatalf("[%s] AssetsTotal = %d, want 1", sc.name, prep.AssetsTotal)
	}
	if sc.wantCacheHit {
		if prep.ReadyBefore != 1 {
			t.Fatalf("[%s] ReadyBefore = %d, want 1", sc.name, prep.ReadyBefore)
		}
		if prep.DownloadedNow != 0 {
			t.Fatalf("[%s] DownloadedNow = %d, want 0", sc.name, prep.DownloadedNow)
		}
	} else {
		if prep.DownloadedNow != 1 {
			t.Fatalf("[%s] DownloadedNow = %d, want 1", sc.name, prep.DownloadedNow)
		}
		if prep.ReadyBefore != 0 {
			t.Fatalf("[%s] ReadyBefore = %d, want 0", sc.name, prep.ReadyBefore)
		}
	}
}

// ── Scenario 1: COLD ────────────────────────────────────────────────────────
// Asset was NOT in cache, NOT prefetched. Downloaded during attempt.
//
// Expected signature:
//
//	origin = runtime_download
//	cache_hit = false
//	downloaded_during_attempt = 1
//	cache_hit_bytes = 0
//	cache_miss_bytes = asset_size
//	prefetch_hit_bytes = 0
