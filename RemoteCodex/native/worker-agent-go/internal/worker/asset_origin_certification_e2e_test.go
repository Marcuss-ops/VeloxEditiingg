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
	"os"
	"sync"
	"testing"
	"time"
	"velox-shared/assetref"
	"velox-shared/futureasset"

	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/internal/prefetch"
)

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

// ── Cross-job SHA collision certification ───────────────────────────────────
// This test proves that when two different jobs share the same SHA256
// (identical content), the origin classification is scoped to the job.
//
// Scenario:
//   - Job B was prefetched, asset-S has SHA=shared-sha
//   - Job C also needs asset-S with the same SHA=shared-sha
//   - When resolving asset-S for Job C, the origin must be warm_cache
//     (not prefetch) because the PreparedJob entry belongs to Job B.
//
// Expected signature for Job C's resolution:
//
//	origin = warm_cache (despite SHA match with Job B's prefetch)
//	cache_hit = true
//	prefetch_hit_bytes = 0
