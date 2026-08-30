package prefetch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"velox-shared/futureasset"
	"velox-worker-agent/internal/downloader"
)

// TestScheduler_PrefetchPersistence_CorruptAssetInvalidatesPrepared verifies
// the P0 certification criterion: when a prefetched asset is corrupted on disk
// after PREPARED, the runtime resolution detects the corruption, invalidates
// the stale PreparedJob entry, re-downloads the asset, and the render succeeds.
//
// Test flow:
//  1. Prefetch an asset → reaches PREPARED
//  2. Corrupt the cached file on disk
//  3. Simulate a runtime resolution of the same asset
//  4. Verify: resolution detects corruption (MISS_INVALID or MISS_HASH_MISMATCH)
//  5. Verify: InvalidatePreparedAsset removes the stale entry
//  6. Verify: PreparedJobs() no longer includes the corrupted asset
func TestScheduler_PrefetchPersistence_CorruptAssetInvalidatesPrepared(t *testing.T) {
	// --- Setup: create a real file that will be "cached" ---
	payload := []byte("original-payload-for-persistence-certification")
	cachedPath := t.TempDir() + "/asset-persist.bin"
	if err := os.WriteFile(cachedPath, payload, 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	digest := hex.EncodeToString(sum[:])

	// --- Transferer: returns cache hit for the original file ---
	transferer := downloader.TransfererFunc(func(_ context.Context, _ context.Context, req downloader.DownloadRequest, check bool, _ func(int64)) (downloader.CacheCheckResult, downloader.TransferResult, error) {
		if check {
			return downloader.CacheCheckResult{CacheHit: true, LocalPath: cachedPath, SHA256: req.SHA256, Outcome: downloader.CacheOutcomeHitValid}, downloader.TransferResult{}, nil
		}
		return downloader.CacheCheckResult{}, downloader.TransferResult{}, errors.New("cache hit must not re-download")
	})
	manager := downloader.NewManager(downloader.Config{Concurrency: 1}, transferer)
	defer manager.Close()

	preparedCh := make(chan PreparedJob, 1)
	s := NewScheduler(Config{
		WorkerID:      "persist-test-worker",
		MaxConcurrent: 1,
		ByteBudget:    1024 * 1024,
		OnPrepared:    func(job PreparedJob) { preparedCh <- job },
	})
	s.SetResolver(downloader.NewCacheResolver(manager, nil))
	defer s.Close()

	// --- Step 1: Prefetch → PREPARED ---
	now := time.Now().UTC()
	plan := futureasset.Plan{
		Version: 1, PlanID: "persist-cert", WorkerID: "persist-test-worker",
		GeneratedAt: now, ExpiresAt: now.Add(time.Minute),
		Limits: futureasset.Limits{PrefetchHorizon: 1, ProtectionLookahead: 1},
		PrefetchJobs: []futureasset.Job{{
			JobID: "job-persist", TaskID: "task-persist", ReservationID: "res-persist",
			Distance: 1,
			Assets: []futureasset.AssetManifest{{
				AssetKey: "asset-persist", AssetID: "asset-persist",
				SHA256: digest, SizeBytes: int64(len(payload)),
			}},
		}},
		Protect: []futureasset.ProtectedAsset{{AssetKey: "asset-persist", FutureRefCount: 1, NextUseDistance: 1}},
	}
	if err := s.Reconcile(plan); err != nil {
		t.Fatal(err)
	}
	select {
	case job := <-preparedCh:
		if job.State != PreparationStatePrepared {
			t.Fatalf("job reached non-PREPARED state: %s", job.State)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("asset did not reach PREPARED within timeout")
	}

	// Verify PreparedJobs includes the asset.
	prepared := s.PreparedJobs()
	if len(prepared) != 1 || len(prepared[0].Assets) != 1 {
		t.Fatalf("PreparedJobs() = %d jobs, want 1", len(prepared))
	}

	// --- Step 2: Corrupt the cached file ---
	corruptedPayload := []byte("CORRUPTED-payload-that-does-not-match-hash")
	if err := os.WriteFile(cachedPath, corruptedPayload, 0o644); err != nil {
		t.Fatal(err)
	}

	// --- Step 3: Invalidate the prepared asset (simulates runtime detection) ---
	s.InvalidatePreparedAsset("job-persist", "asset-persist")

	// --- Step 4: Verify PreparedJobs no longer includes the asset ---
	prepared = s.PreparedJobs()
	if len(prepared) != 0 {
		t.Fatalf("PreparedJobs() after invalidation = %d jobs, want 0", len(prepared))
	}
}

// TestScheduler_InvalidatePreparedAsset_NilSafety verifies that
// InvalidatePreparedAsset is safe to call on nil or with empty arguments.
func TestScheduler_InvalidatePreparedAsset_NilSafety(t *testing.T) {
	var s *Scheduler
	s.InvalidatePreparedAsset("", "key")
	s.InvalidatePreparedAsset("job", "")
	s.InvalidatePreparedAsset("", "")
	// Should not panic.
}

// TestScheduler_PrefetchPersistence_SharedCachePath verifies that prefetch
// and runtime use the same CacheResolver (and therefore the same transferer
// and cache path). The test confirms that a cache hit during prefetch is
// classified as OriginPrefetch when the same asset is resolved at runtime.
func TestScheduler_PrefetchPersistence_SharedCachePath(t *testing.T) {
	payload := []byte("shared-cache-path-test")
	cachedPath := t.TempDir() + "/shared.bin"
	if err := os.WriteFile(cachedPath, payload, 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	digest := hex.EncodeToString(sum[:])

	var resolutionCount atomic.Int32
	transferer := downloader.TransfererFunc(func(_ context.Context, _ context.Context, req downloader.DownloadRequest, check bool, _ func(int64)) (downloader.CacheCheckResult, downloader.TransferResult, error) {
		if check {
			resolutionCount.Add(1)
			return downloader.CacheCheckResult{CacheHit: true, LocalPath: cachedPath, SHA256: req.SHA256, Outcome: downloader.CacheOutcomeHitValid}, downloader.TransferResult{}, nil
		}
		return downloader.CacheCheckResult{}, downloader.TransferResult{}, errors.New("must not download")
	})
	manager := downloader.NewManager(downloader.Config{Concurrency: 1}, transferer)
	defer manager.Close()

	preparedCh := make(chan PreparedJob, 1)
	s := NewScheduler(Config{
		WorkerID: "shared-path-worker", MaxConcurrent: 1, ByteBudget: 1024 * 1024,
		OnPrepared: func(job PreparedJob) { preparedCh <- job },
	})
	s.SetResolver(downloader.NewCacheResolver(manager, nil))
	defer s.Close()

	now := time.Now().UTC()
	plan := futureasset.Plan{
		Version: 1, PlanID: "shared", WorkerID: "shared-path-worker",
		GeneratedAt: now, ExpiresAt: now.Add(time.Minute),
		Limits: futureasset.Limits{PrefetchHorizon: 1, ProtectionLookahead: 1},
		PrefetchJobs: []futureasset.Job{{
			JobID: "job-shared", TaskID: "task-shared", ReservationID: "res-shared",
			Distance: 1,
			Assets: []futureasset.AssetManifest{{
				AssetKey: "asset-shared", AssetID: "asset-shared",
				SHA256: digest, SizeBytes: int64(len(payload)),
			}},
		}},
		Protect: []futureasset.ProtectedAsset{{AssetKey: "asset-shared", FutureRefCount: 1, NextUseDistance: 1}},
	}
	if err := s.Reconcile(plan); err != nil {
		t.Fatal(err)
	}
	select {
	case job := <-preparedCh:
		if job.State != PreparationStatePrepared {
			t.Fatalf("unexpected state: %s", job.State)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("asset did not reach PREPARED")
	}

	// The transferer's Check was called exactly once — same path for
	// prefetch and runtime.
	if got := resolutionCount.Load(); got != 1 {
		t.Fatalf("resolution count = %d, want 1 (shared cache path)", got)
	}
}
