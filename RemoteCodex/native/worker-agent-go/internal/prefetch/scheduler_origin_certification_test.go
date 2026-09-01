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

	"velox-shared/assetref"
	"velox-shared/futureasset"
	"velox-worker-agent/internal/downloader"
)

// ── Canonical Origin Classification Certification Tests ────────────────
//
// These three tests certify the three mutually-exclusive resolution
// origin paths. Each test simulates the exact scenario it names and
// verifies that the cacheResolutionSink classifies the origin correctly.
//
// COLD:  No FutureAssetPlan exists. The asset is not in cache.
//         The scheduler does not run. The attempt downloads the asset.
//         Origin = runtime_download.
//
// WARM:  The asset is already in cache from a prior job/session.
//         No PreparedJob entry exists for it. The scheduler does not
//         run (or runs but the asset is already a cache hit).
//         Origin = warm_cache.
//
// PREFETCH: A FutureAssetPlan triggers download of the asset before
//         the attempt. A PreparedJob entry with matching SHA256/size
//         exists. The scheduler runs and the asset is ready.
//         Origin = prefetch. prepared_ratio = 1.0.

type testOriginSink struct {
	s        *Scheduler
	lastDown downloader.CacheResolution
}

func (s *testOriginSink) RecordResolution(_ context.Context, resolution downloader.CacheResolution) {
	if resolution.CacheHit && resolution.Origin == "" {
		resolution.Origin = downloader.OriginWarmCache
		for _, job := range s.s.PreparedJobs() {
			for _, asset := range job.Assets {
				if asset.SHA256 == string(resolution.SHA256) && asset.SizeBytes > 0 {
					resolution.Origin = downloader.OriginPrefetch
				}
			}
		}
	} else if !resolution.CacheHit && resolution.Origin == "" {
		resolution.Origin = downloader.OriginRuntimeDownload
	}
	s.lastDown = resolution
}

func (s *testOriginSink) lastOrigin() downloader.ResolutionOrigin {
	return s.lastDown.Origin
}

func TestCertification_COLD_OriginRuntimeDownload(t *testing.T) {
	payload := []byte("COLD-payload-not-in-cache")
	sum := sha256.Sum256(payload)
	sha := hex.EncodeToString(sum[:])
	var downloadCount atomic.Int32
	transferer := downloader.TransfererFunc(func(_ context.Context, _ context.Context, req downloader.DownloadRequest, check bool, _ func(int64)) (downloader.CacheCheckResult, downloader.TransferResult, error) {
		if check {
			return downloader.CacheCheckResult{Outcome: downloader.CacheOutcomeMissNotFound}, downloader.TransferResult{}, nil
		}
		downloadCount.Add(1)
		return downloader.CacheCheckResult{}, downloader.TransferResult{LocalPath: "/cold/asset.bin", Bytes: int64(len(payload)), SHA256: req.SHA256}, nil
	})
	manager := downloader.NewManager(downloader.Config{Concurrency: 1}, transferer)
	defer manager.Close()
	s := NewScheduler(Config{WorkerID: "cold-worker", MaxConcurrent: 1, ByteBudget: 1024 * 1024})
	coldSink := &testOriginSink{s: s}
	s.SetResolver(downloader.NewCacheResolver(manager, coldSink))
	defer s.Close()
	req := downloader.DownloadRequest{AssetKey: "asset-cold", AssetID: "asset-cold", SHA256: assetref.ContentHash(sha), SizeBytes: int64(len(payload))}
	resolution, err := s.resolver.Resolve(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if resolution.CacheHit {
		t.Fatal("COLD: expected cache miss, got cache hit")
	}
	if !resolution.Downloaded {
		t.Fatal("COLD: expected Downloaded=true")
	}
	if downloadCount.Load() != 1 {
		t.Fatalf("COLD: download count = %d, want 1", downloadCount.Load())
	}
	if coldSink.lastOrigin() != downloader.OriginRuntimeDownload {
		t.Fatalf("COLD: origin = %q, want %q", coldSink.lastOrigin(), downloader.OriginRuntimeDownload)
	}
	prepared := s.PreparedJobs()
	if len(prepared) != 0 {
		t.Fatalf("COLD: prepared jobs = %d, want 0", len(prepared))
	}
}

func TestCertification_WARM_OriginWarmCache(t *testing.T) {
	payload := []byte("WARM-payload-already-in-cache")
	sum := sha256.Sum256(payload)
	sha := hex.EncodeToString(sum[:])
	path := t.TempDir() + "/warm-asset.bin"
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatal(err)
	}
	var downloadCount atomic.Int32
	transferer := downloader.TransfererFunc(func(_ context.Context, _ context.Context, req downloader.DownloadRequest, check bool, _ func(int64)) (downloader.CacheCheckResult, downloader.TransferResult, error) {
		if check {
			return downloader.CacheCheckResult{CacheHit: true, LocalPath: path, SHA256: req.SHA256, Outcome: downloader.CacheOutcomeHitValid}, downloader.TransferResult{}, nil
		}
		downloadCount.Add(1)
		return downloader.CacheCheckResult{}, downloader.TransferResult{}, errors.New("cache hit must not download")
	})
	manager := downloader.NewManager(downloader.Config{Concurrency: 1}, transferer)
	defer manager.Close()
	s := NewScheduler(Config{WorkerID: "warm-worker", MaxConcurrent: 1, ByteBudget: 1024 * 1024})
	warmSink := &testOriginSink{s: s}
	s.SetResolver(downloader.NewCacheResolver(manager, warmSink))
	defer s.Close()
	req := downloader.DownloadRequest{AssetKey: "asset-warm", AssetID: "asset-warm", SHA256: assetref.ContentHash(sha), SizeBytes: int64(len(payload))}
	resolution, err := s.resolver.Resolve(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !resolution.CacheHit {
		t.Fatal("WARM: expected cache hit, got cache miss")
	}
	if downloadCount.Load() != 0 {
		t.Fatalf("WARM: download count = %d, want 0", downloadCount.Load())
	}
	if warmSink.lastOrigin() != downloader.OriginWarmCache {
		t.Fatalf("WARM: origin = %q, want %q", warmSink.lastOrigin(), downloader.OriginWarmCache)
	}
	prepared := s.PreparedJobs()
	if len(prepared) != 0 {
		t.Fatalf("WARM: prepared jobs = %d, want 0 (no FutureAssetPlan)", len(prepared))
	}
}

func TestCertification_PREFETCH_OriginPrefetch(t *testing.T) {
	payload := []byte("PREFETCH-payload-downloaded-by-plan")
	sum := sha256.Sum256(payload)
	sha := hex.EncodeToString(sum[:])
	prefetchPath := t.TempDir() + "/prefetch-asset.bin"
	if err := os.WriteFile(prefetchPath, payload, 0o644); err != nil {
		t.Fatal(err)
	}
	var downloadCount atomic.Int32
	var seen atomic.Bool
	transferer := downloader.TransfererFunc(func(_ context.Context, _ context.Context, req downloader.DownloadRequest, check bool, _ func(int64)) (downloader.CacheCheckResult, downloader.TransferResult, error) {
		if check {
			if seen.Load() {
				return downloader.CacheCheckResult{CacheHit: true, LocalPath: prefetchPath, SHA256: req.SHA256, Outcome: downloader.CacheOutcomeHitValid}, downloader.TransferResult{}, nil
			}
			return downloader.CacheCheckResult{Outcome: downloader.CacheOutcomeMissNotFound}, downloader.TransferResult{}, nil
		}
		seen.Store(true)
		downloadCount.Add(1)
		return downloader.CacheCheckResult{}, downloader.TransferResult{LocalPath: prefetchPath, Bytes: int64(len(payload)), SHA256: req.SHA256}, nil
	})
	manager := downloader.NewManager(downloader.Config{Concurrency: 2}, transferer)
	defer manager.Close()
	preparedCh := make(chan PreparedJob, 4)
	s := NewScheduler(Config{WorkerID: "prefetch-worker", MaxConcurrent: 1, ByteBudget: 1024 * 1024, OnPrepared: func(job PreparedJob) { preparedCh <- job }})
	prefetchSink := &testOriginSink{s: s}
	s.SetResolver(downloader.NewCacheResolver(manager, prefetchSink))
	defer s.Close()
	now := time.Now().UTC()
	plan := futureasset.Plan{
		Version: 1, PlanID: "prefetch-cert", WorkerID: "prefetch-worker", GeneratedAt: now, ExpiresAt: now.Add(5 * time.Minute),
		Limits: futureasset.Limits{PrefetchHorizon: 1, ProtectionLookahead: 1},
		PrefetchJobs: []futureasset.Job{{JobID: "job-prefetch", TaskID: "task-prefetch", ReservationID: "reservation-prefetch", Distance: 1, Assets: []futureasset.AssetManifest{{AssetKey: "asset-prefetch", AssetID: "asset-prefetch", SHA256: sha, SizeBytes: int64(len(payload))}}}},
		Protect: []futureasset.ProtectedAsset{{AssetKey: "asset-prefetch", FutureRefCount: 1, NextUseDistance: 1}},
	}
	if err := s.Reconcile(plan); err != nil {
		t.Fatal(err)
	}
	select {
	case job := <-preparedCh:
		if job.State != PreparationStatePrepared {
			t.Fatalf("PREFETCH: state = %q, want PREPARED", job.State)
		}
		if len(job.Assets) != 1 {
			t.Fatalf("PREFETCH: assets = %d, want 1", len(job.Assets))
		}
		asset := job.Assets["asset-prefetch"]
		if asset.SHA256 != sha {
			t.Fatalf("PREFETCH: SHA256 = %q, want %q", asset.SHA256, sha)
		}
		if asset.SizeBytes != int64(len(payload)) {
			t.Fatalf("PREFETCH: size = %d, want %d", asset.SizeBytes, len(payload))
		}
		if asset.PreparedAt.Before(plan.GeneratedAt) {
			t.Fatalf("PREFETCH: prepared_at %s before plan GeneratedAt %s", asset.PreparedAt, plan.GeneratedAt)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("PREFETCH: did not reach PREPARED within timeout")
	}
	req := downloader.DownloadRequest{AssetKey: "asset-prefetch", AssetID: "asset-prefetch", SHA256: assetref.ContentHash(sha), SizeBytes: int64(len(payload))}
	resolution, err := s.resolver.Resolve(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !resolution.CacheHit {
		t.Fatal("PREFETCH: expected cache hit after plan download, got miss")
	}
	if prefetchSink.lastOrigin() != downloader.OriginPrefetch {
		t.Fatalf("PREFETCH: origin = %q, want %q", prefetchSink.lastOrigin(), downloader.OriginPrefetch)
	}
	prepared := s.PreparedJobs()
	totalAssets := 0
	prefetchedReady := 0
	for _, pj := range prepared {
		for _, a := range pj.Assets {
			totalAssets++
			if a.SHA256 == sha && a.SizeBytes == int64(len(payload)) {
				prefetchedReady++
			}
		}
	}
	if totalAssets == 0 {
		t.Fatal("PREFETCH: no prepared assets found")
	}
	preparedRatio := float64(prefetchedReady) / float64(totalAssets)
	if preparedRatio != 1.0 {
		t.Fatalf("PREFETCH: prepared_ratio = %.2f, want 1.0 (prefetched=%d, total=%d)", preparedRatio, prefetchedReady, totalAssets)
	}
	if downloadCount.Load() != 1 {
		t.Fatalf("PREFETCH: download count = %d, want 1 (plan downloaded, attempt should not)", downloadCount.Load())
	}
}
