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
	"crypto/sha256"
	"encoding/hex"
	"os"
	"sync"
	"testing"
	"time"

	"velox-shared/assetref"
	"velox-shared/futureasset"
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
	name       string
	payloadB   []byte
	shaB       string
	pathB      string
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
		AssetID:    "asset-B",
		Outcome:    downloader.CacheOutcomeHitValid,
		LocalPath:  sc.pathB,
		CacheHit:   sc.wantCacheHit,
		SHA256:     assetref.ContentHash(sc.shaB),
		SizeBytes:  int64(len(sc.payloadB)),
		Source:     downloader.CacheSourceLocalDisk,
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
//   origin = runtime_download
//   cache_hit = false
//   downloaded_during_attempt = 1
//   cache_hit_bytes = 0
//   cache_miss_bytes = asset_size
//   prefetch_hit_bytes = 0
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
//   origin = warm_cache
//   cache_hit = true
//   downloaded_during_attempt = 0
//   cache_hit_bytes = asset_size
//   cache_miss_bytes = 0
//   prefetch_hit_bytes = 0  (subset of cache_hit_bytes that was prefetch)
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
//   origin = prefetch
//   cache_hit = true
//   downloaded_during_attempt = 0
//   cache_hit_bytes = asset_size
//   cache_miss_bytes = 0
//   prefetch_hit_bytes = asset_size  (ALL cache_hit_bytes are prefetch)
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
func TestCertification_PrefetchEndToEnd(t *testing.T) {
	payloadA := []byte("AAAA-prefetch-e2e-asset-A")
	payloadB := []byte("BBBB-prefetch-e2e-asset-B-unique")
	shaA := sha256hex(payloadA)
	shaB := sha256hex(payloadB)

	if shaA == shaB {
		t.Fatal("SHA_A == SHA_B: payloads are not distinct")
	}

	pathA := t.TempDir() + "/asset-A.bin"
	pathB := t.TempDir() + "/asset-B.bin"
	if err := os.WriteFile(pathA, payloadA, 0o644); err != nil {
		t.Fatal(err)
	}
	// pathB is NOT written — B must be downloaded by prefetch.

	var mu sync.Mutex
	downloads := map[string]bool{}
	transferer := downloader.TransfererFunc(func(_ context.Context, _ context.Context, req downloader.DownloadRequest, check bool, _ func(int64)) (downloader.CacheCheckResult, downloader.TransferResult, error) {
		if check {
			if string(req.SHA256) == shaA {
				return downloader.CacheCheckResult{CacheHit: true, LocalPath: pathA, SHA256: req.SHA256, Outcome: downloader.CacheOutcomeHitValid}, downloader.TransferResult{}, nil
			}
			return downloader.CacheCheckResult{Outcome: downloader.CacheOutcomeMissNotFound}, downloader.TransferResult{}, nil
		}
		mu.Lock()
		downloads[string(req.SHA256)] = true
		mu.Unlock()
		if err := os.WriteFile(pathB, payloadB, 0o644); err != nil {
			return downloader.CacheCheckResult{}, downloader.TransferResult{}, err
		}
		return downloader.CacheCheckResult{}, downloader.TransferResult{LocalPath: pathB, Bytes: int64(len(payloadB)), SHA256: req.SHA256}, nil
	})
	manager := downloader.NewManager(downloader.Config{Concurrency: 2}, transferer)
	defer manager.Close()

	preparedCh := make(chan prefetch.PreparedJob, 4)
	s := prefetch.NewScheduler(prefetch.Config{
		WorkerID:      "cert-worker",
		MaxConcurrent: 1,
		ByteBudget:    1024 * 1024,
		OnPrepared:    func(job prefetch.PreparedJob) { preparedCh <- job },
	})
	s.SetResolver(downloader.NewCacheResolver(manager, nil))
	defer s.Close()

	now := time.Now().UTC()
	plan := futureasset.Plan{
		Version:     1,
		PlanID:      "cert-prefetch-plan",
		WorkerID:    "cert-worker",
		GeneratedAt: now,
		ExpiresAt:   now.Add(5 * time.Minute),
		Limits: futureasset.Limits{
			PrefetchHorizon:     2,
			ProtectionLookahead: 2,
		},
		PrefetchJobs: []futureasset.Job{
			{
				JobID: "job-A", TaskID: "task-A", ReservationID: "res-A", Distance: 1,
				Assets: []futureasset.AssetManifest{{AssetKey: "asset-A", AssetID: "asset-A", SHA256: shaA, SizeBytes: int64(len(payloadA))}},
			},
			{
				JobID: "job-B", TaskID: "task-B", ReservationID: "res-B", Distance: 2,
				Assets: []futureasset.AssetManifest{{AssetKey: "asset-B", AssetID: "asset-B", SHA256: shaB, SizeBytes: int64(len(payloadB))}},
			},
		},
		Protect: []futureasset.ProtectedAsset{
			{AssetKey: "asset-A", FutureRefCount: 1, NextUseDistance: 1},
			{AssetKey: "asset-B", FutureRefCount: 1, NextUseDistance: 2},
		},
	}

	if err := s.Reconcile(plan); err != nil {
		t.Fatal(err)
	}

	// Wait for both jobs to reach PREPARED.
	preparedJobs := make(map[string]prefetch.PreparedJob)
	deadline := time.After(5 * time.Second)
	for len(preparedJobs) < 2 {
		select {
		case job := <-preparedCh:
			if job.State != prefetch.PreparationStatePrepared {
				t.Fatalf("job %s reached non-PREPARED state: %s", job.JobID, job.State)
			}
			preparedJobs[job.JobID] = job
		case <-deadline:
			t.Fatalf("only %d/2 jobs reached PREPARED within timeout", len(preparedJobs))
		}
	}

	// ── Now simulate the attempt resolving asset-B with origin classification ──
	assetB := preparedJobs["job-B"].Assets["asset-B"]

	// The PreparedJobs callback returns the same data the scheduler has.
	preparedJobsFn := func() []prefetch.PreparedJob {
		return []prefetch.PreparedJob{preparedJobs["job-B"]}
	}

	sink := cacheResolutionSink{preparedJobs: preparedJobsFn}
	tracker := &assetOperationTracker{cacheEnabled: true}
	ctx := withAssetOperationTracker(context.Background(), tracker)

	// The attempt resolves asset-B — it's a cache hit with prefetch origin.
	sink.RecordResolution(ctx, downloader.CacheResolution{
		AssetID:   "asset-B",
		Outcome:   downloader.CacheOutcomeHitValid,
		LocalPath: assetB.LocalPath,
		CacheHit:  true,
		SHA256:    assetref.ContentHash(shaB),
		SizeBytes: int64(len(payloadB)),
		Source:    downloader.CacheSourceLocalDisk,
	})

	cache := tracker.cacheSnapshot()

	// ── The definitive certification assertions ──────────────────────
	if cache.OriginPrefetchCount != 1 {
		t.Fatalf("OriginPrefetchCount = %d, want 1 (asset-B was prefetched)", cache.OriginPrefetchCount)
	}
	if cache.OriginWarmCacheCount != 0 {
		t.Fatalf("OriginWarmCacheCount = %d, want 0", cache.OriginWarmCacheCount)
	}
	if cache.OriginDownloadCount != 0 {
		t.Fatalf("OriginDownloadCount = %d, want 0 (no download during attempt)", cache.OriginDownloadCount)
	}
	if cache.CacheHitBytes != int64(len(payloadB)) {
		t.Fatalf("CacheHitBytes = %d, want %d", cache.CacheHitBytes, len(payloadB))
	}
	if cache.PrefetchHitBytes != int64(len(payloadB)) {
		t.Fatalf("PrefetchHitBytes = %d, want %d (ALL hit bytes are prefetch)", cache.PrefetchHitBytes, len(payloadB))
	}
	if cache.CacheDownloadBytes != 0 {
		t.Fatalf("CacheDownloadBytes = %d, want 0", cache.CacheDownloadBytes)
	}
}

// ── Warm cache classification: asset from prior session, no PreparedJob ─────
func TestCertification_WarmCacheFromPriorSession(t *testing.T) {
	payload := []byte("WARM-e2e-asset-from-previous-session")
	sha := sha256hex(payload)

	// Simulate a sink with no PreparedJobs (nil callback).
	sink := cacheResolutionSink{preparedJobs: nil}
	tracker := &assetOperationTracker{cacheEnabled: true}
	ctx := withAssetOperationTracker(context.Background(), tracker)

	sink.RecordResolution(ctx, downloader.CacheResolution{
		AssetID:   "asset-W",
		Outcome:   downloader.CacheOutcomeHitValid,
		LocalPath: "/cache/asset-W-from-prior.bin",
		CacheHit:  true,
		SHA256:    assetref.ContentHash(sha),
		SizeBytes: int64(len(payload)),
		Source:    downloader.CacheSourceLocalDisk,
	})

	cache := tracker.cacheSnapshot()

	if cache.OriginWarmCacheCount != 1 {
		t.Fatalf("OriginWarmCacheCount = %d, want 1 (nil PreparedJobs → warm_cache)", cache.OriginWarmCacheCount)
	}
	if cache.OriginPrefetchCount != 0 {
		t.Fatalf("OriginPrefetchCount = %d, want 0", cache.OriginPrefetchCount)
	}
	if cache.OriginDownloadCount != 0 {
		t.Fatalf("OriginDownloadCount = %d, want 0", cache.OriginDownloadCount)
	}
	if cache.CacheHitBytes != int64(len(payload)) {
		t.Fatalf("CacheHitBytes = %d, want %d", cache.CacheHitBytes, len(payload))
	}
	if cache.PrefetchHitBytes != 0 {
		t.Fatalf("PrefetchHitBytes = %d, want 0 (no PreparedJob → not prefetch)", cache.PrefetchHitBytes)
	}
}

// ── Cold cache classification: asset not present, downloaded during attempt ──
func TestCertification_ColdCacheDownloadDuringAttempt(t *testing.T) {
	payload := []byte("COLD-e2e-asset-downloaded-now")
	sha := sha256hex(payload)

	sink := cacheResolutionSink{preparedJobs: nil}
	tracker := &assetOperationTracker{cacheEnabled: true}
	ctx := withAssetOperationTracker(context.Background(), tracker)

	sink.RecordResolution(ctx, downloader.CacheResolution{
		AssetID:       "asset-C",
		Outcome:       downloader.CacheOutcomeMissNotFound,
		Downloaded:    true,
		DownloadBytes: int64(len(payload)),
		SizeBytes:     int64(len(payload)),
		Source:        downloader.CacheSourceMaster,
		SHA256:        assetref.ContentHash(sha),
	})

	cache := tracker.cacheSnapshot()

	if cache.OriginDownloadCount != 1 {
		t.Fatalf("OriginDownloadCount = %d, want 1 (cold → runtime_download)", cache.OriginDownloadCount)
	}
	if cache.OriginWarmCacheCount != 0 {
		t.Fatalf("OriginWarmCacheCount = %d, want 0", cache.OriginWarmCacheCount)
	}
	if cache.OriginPrefetchCount != 0 {
		t.Fatalf("OriginPrefetchCount = %d, want 0", cache.OriginPrefetchCount)
	}
	if cache.CacheMissBytes != int64(len(payload)) {
		t.Fatalf("CacheMissBytes = %d, want %d", cache.CacheMissBytes, len(payload))
	}
	if cache.CacheHitBytes != 0 {
		t.Fatalf("CacheHitBytes = %d, want 0", cache.CacheHitBytes)
	}
	if cache.PrefetchHitBytes != 0 {
		t.Fatalf("PrefetchHitBytes = %d, want 0", cache.PrefetchHitBytes)
	}
}
