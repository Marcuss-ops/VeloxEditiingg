package worker

import (
	"context"
	"testing"
	"time"

	"velox-shared/assetref"
	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/internal/prefetch"
	"velox-worker-agent/internal/taskrunner"
)

// ── Identity fields propagated to CacheResolution ──────────────────────────
// Verifies that JobID, TaskID, AssetKey, and ResolvedAt are carried on the
// CacheResolution through the CacheResolver.Resolve path.
func TestCertification_CacheResolutionIdentityFields(t *testing.T) {
	payload := []byte("identity-fields-asset")
	sha := sha256hex(payload)

	var capturedResolution downloader.CacheResolution
	sink := &captureResolverSink{fn: func(_ context.Context, r downloader.CacheResolution) {
		capturedResolution = r
	}}

	manager := &fakedDownloadManager{
		asset: downloader.DownloadedAsset{
			AssetID:   "asset-ID",
			LocalPath: "/cache/asset-ID.bin",
			SHA256:    assetref.ContentHash(sha),
			SizeBytes: int64(len(payload)),
			CacheHit:  true,
			ReadyAt:   time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC),
		},
	}

	resolver := downloader.NewCacheResolver(manager, sink)
	_, err := resolver.Resolve(context.Background(), downloader.DownloadRequest{
		JobID:     "job-IDENTITY",
		TaskID:    "task-IDENTITY",
		AssetKey:  assetref.AssetKey("asset-ID-key"),
		AssetID:   "asset-ID",
		SHA256:    assetref.ContentHash(sha),
		SizeBytes: int64(len(payload)),
	})
	if err != nil {
		t.Fatal(err)
	}

	if capturedResolution.JobID != "job-IDENTITY" {
		t.Fatalf("JobID = %q, want job-IDENTITY", capturedResolution.JobID)
	}
	if capturedResolution.TaskID != "task-IDENTITY" {
		t.Fatalf("TaskID = %q, want task-IDENTITY", capturedResolution.TaskID)
	}
	if string(capturedResolution.AssetKey) != "asset-ID-key" {
		t.Fatalf("AssetKey = %q, want asset-ID-key", capturedResolution.AssetKey)
	}
	if capturedResolution.ResolvedAt.IsZero() {
		t.Fatal("ResolvedAt must be set from DownloadedAsset.ReadyAt")
	}
	if !capturedResolution.ResolvedAt.Equal(time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("ResolvedAt = %v, want 2026-08-28T12:00:00Z", capturedResolution.ResolvedAt)
	}
}

// captureResolverSink records resolutions for assertion.
type captureResolverSink struct {
	fn func(ctx context.Context, r downloader.CacheResolution)
}

func (c *captureResolverSink) RecordResolution(ctx context.Context, r downloader.CacheResolution) {
	if c.fn != nil {
		c.fn(ctx, r)
	}
}

// fakedDownloadManager returns a fixed DownloadedAsset.
type fakedDownloadManager struct {
	asset downloader.DownloadedAsset
	err   error
}

func (f *fakedDownloadManager) Resolve(_ context.Context, _ downloader.DownloadRequest) (downloader.DownloadedAsset, error) {
	return f.asset, f.err
}

func (f *fakedDownloadManager) Snapshot(_ assetref.AssetKey) (downloader.DownloadSnapshot, bool) {
	return downloader.DownloadSnapshot{}, false
}

func (f *fakedDownloadManager) Subscribe(_ assetref.AssetKey) (<-chan downloader.DownloadSnapshot, func()) {
	return nil, func() {}
}

func (f *fakedDownloadManager) JobSnapshot(_ string) downloader.JobDownloadSnapshot {
	return downloader.JobDownloadSnapshot{}
}

func (f *fakedDownloadManager) LatestOperational() downloader.OperationalSnapshot {
	return downloader.OperationalSnapshot{}
}

var _ downloader.AssetDownloadManager = (*fakedDownloadManager)(nil)

// ── Wire breakdown bytes/origin certification ──────────────────────────────
// This test proves that AssetPreparationBreakdown on the wire carries the
// correct byte-level attribution and origin counters when the cacheResolutionSink
// classifies resolutions with Origin set.
//
// The breakdown must carry:
//   - cache_hit_bytes = sum of SizeBytes for cache hits
//   - cache_miss_bytes = sum of DownloadBytes for cache misses
//   - prefetch_hit_bytes = subset of cache_hit_bytes where origin == prefetch
//   - prefetch_hits / warm_cache_hits / runtime_downloads counts
func TestCertification_WireBreakdownBytesAndOrigin(t *testing.T) {
	payloadWarm := []byte("warm-asset-payload")
	payloadPrefetch := []byte("prefetch-asset-payload")
	payloadCold := []byte("cold-asset-payload")

	shaWarm := sha256hex(payloadWarm)
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
					SHA256:    sha256hex(payloadPrefetch),
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

	// 1. Warm cache hit
	sink.RecordResolution(ctx, downloader.CacheResolution{
		AssetID: "asset-warm", Outcome: downloader.CacheOutcomeHitValid,
		CacheHit: true, SizeBytes: int64(len(payloadWarm)),
		SHA256: assetref.ContentHash(shaWarm), Source: downloader.CacheSourceLocalDisk,
	})
	// 2. Prefetch hit
	sink.RecordResolution(ctx, downloader.CacheResolution{
		AssetID: "asset-prefetch", Outcome: downloader.CacheOutcomeHitValid,
		CacheHit: true, SizeBytes: int64(len(payloadPrefetch)),
		SHA256: assetref.ContentHash(sha256hex(payloadPrefetch)),
		Source: downloader.CacheSourceLocalDisk,
	})
	// 3. Cold download
	sink.RecordResolution(ctx, downloader.CacheResolution{
		AssetID: "asset-cold", Outcome: downloader.CacheOutcomeMissNotFound,
		Downloaded: true, DownloadBytes: int64(len(payloadCold)),
		SizeBytes: int64(len(payloadCold)),
		SHA256:    assetref.ContentHash(sha256hex(payloadCold)),
		Source:    downloader.CacheSourceMaster,
	})

	// Project onto wire breakdown.
	report := taskrunner.TaskExecutionReport{}
	attachAssetOperations(&report, tracker)

	if report.AssetPreparation == nil {
		t.Fatal("AssetPreparation breakdown missing")
	}
	bd := *report.AssetPreparation

	// Byte attribution
	wantHitBytes := int64(len(payloadWarm) + len(payloadPrefetch))
	if bd.CacheHitBytes != wantHitBytes {
		t.Fatalf("CacheHitBytes = %d, want %d", bd.CacheHitBytes, wantHitBytes)
	}
	wantMissBytes := int64(len(payloadCold))
	if bd.CacheMissBytes != wantMissBytes {
		t.Fatalf("CacheMissBytes = %d, want %d", bd.CacheMissBytes, wantMissBytes)
	}
	wantPrefetchBytes := int64(len(payloadPrefetch))
	if bd.PrefetchHitBytes != wantPrefetchBytes {
		t.Fatalf("PrefetchHitBytes = %d, want %d", bd.PrefetchHitBytes, wantPrefetchBytes)
	}

	// Origin counters
	if bd.PrefetchHits != 1 {
		t.Fatalf("PrefetchHits = %d, want 1", bd.PrefetchHits)
	}
	if bd.WarmCacheHits != 1 {
		t.Fatalf("WarmCacheHits = %d, want 1", bd.WarmCacheHits)
	}
	if bd.RuntimeDownloads != 1 {
		t.Fatalf("RuntimeDownloads = %d, want 1", bd.RuntimeDownloads)
	}

	// Count fields
	if bd.AssetsRequired != 3 {
		t.Fatalf("AssetsRequired = %d, want 3", bd.AssetsRequired)
	}
	if bd.CacheHits != 2 {
		t.Fatalf("CacheHits = %d, want 2", bd.CacheHits)
	}
	if bd.CacheMisses != 1 {
		t.Fatalf("CacheMisses = %d, want 1", bd.CacheMisses)
	}
}
