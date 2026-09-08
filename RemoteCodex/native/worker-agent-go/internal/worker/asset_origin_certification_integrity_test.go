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

func TestCertification_CrossJobSHACollision(t *testing.T) {
	payload := []byte("shared-content-for-collision-test")
	sharedSHA := sha256hex(payload)

	// Job B was prefetched with this asset.
	preparedJobs := []prefetch.PreparedJob{
		{
			JobID:      "job-B",
			TaskID:     "task-B",
			State:      prefetch.PreparationStatePrepared,
			PreparedAt: time.Now().UTC().Add(-10 * time.Second),
			Assets: map[string]prefetch.PreparedAssetMetadata{
				"asset-S": {
					AssetKey:  "asset-S",
					AssetID:   "asset-S",
					SHA256:    sharedSHA,
					SizeBytes: int64(len(payload)),
				},
			},
		},
	}

	sink := cacheResolutionSink{
		preparedJobs: func() []prefetch.PreparedJob {
			return preparedJobs
		},
	}

	// ── Resolving asset-S for Job B → should be OriginPrefetch ───────
	trackerB := &assetOperationTracker{cacheEnabled: true}
	ctxB := withAssetOperationTracker(context.Background(), trackerB)
	sink.RecordResolution(ctxB, downloader.CacheResolution{
		AssetID:   "asset-S",
		Outcome:   downloader.CacheOutcomeHitValid,
		CacheHit:  true,
		SHA256:    assetref.ContentHash(sharedSHA),
		SizeBytes: int64(len(payload)),
		Source:    downloader.CacheSourceLocalDisk,
		JobID:     "job-B",
		TaskID:    "task-B",
		AssetKey:  "asset-S",
	})
	cacheB := trackerB.cacheSnapshot()
	if cacheB.OriginPrefetchCount != 1 {
		t.Fatalf("Job B OriginPrefetchCount = %d, want 1 (prefetched)", cacheB.OriginPrefetchCount)
	}
	if cacheB.OriginWarmCacheCount != 0 {
		t.Fatalf("Job B OriginWarmCacheCount = %d, want 0", cacheB.OriginWarmCacheCount)
	}

	// ── Resolving asset-S for Job C → must be OriginWarmCache ───────
	// Same SHA, same size, but JobID/TaskID don't match the PreparedJob.
	trackerC := &assetOperationTracker{cacheEnabled: true}
	ctxC := withAssetOperationTracker(context.Background(), trackerC)
	sink.RecordResolution(ctxC, downloader.CacheResolution{
		AssetID:   "asset-S",
		Outcome:   downloader.CacheOutcomeHitValid,
		CacheHit:  true,
		SHA256:    assetref.ContentHash(sharedSHA),
		SizeBytes: int64(len(payload)),
		Source:    downloader.CacheSourceLocalDisk,
		JobID:     "job-C",
		TaskID:    "task-C",
		AssetKey:  "asset-S",
	})
	cacheC := trackerC.cacheSnapshot()
	if cacheC.OriginWarmCacheCount != 1 {
		t.Fatalf("Job C OriginWarmCacheCount = %d, want 1 (same SHA but different job)", cacheC.OriginWarmCacheCount)
	}
	if cacheC.OriginPrefetchCount != 0 {
		t.Fatalf("Job C OriginPrefetchCount = %d, want 0 (not prefetched for this job)", cacheC.OriginPrefetchCount)
	}
	if cacheC.PrefetchHitBytes != 0 {
		t.Fatalf("Job C PrefetchHitBytes = %d, want 0", cacheC.PrefetchHitBytes)
	}
}

// ── Cross-job SHA collision: same job, different task → warm_cache ──────────
// Even within the same JobID, if the TaskID doesn't match the PreparedJob,
// the resolution must be classified as warm_cache.
func TestCertification_CrossTaskSHACollision(t *testing.T) {
	payload := []byte("shared-content-cross-task-test")
	sharedSHA := sha256hex(payload)

	// PreparedJob for task-A within job-A.
	preparedJobs := []prefetch.PreparedJob{
		{
			JobID:      "job-A",
			TaskID:     "task-A",
			State:      prefetch.PreparationStatePrepared,
			PreparedAt: time.Now().UTC().Add(-10 * time.Second),
			Assets: map[string]prefetch.PreparedAssetMetadata{
				"asset-X": {
					AssetKey:  "asset-X",
					AssetID:   "asset-X",
					SHA256:    sharedSHA,
					SizeBytes: int64(len(payload)),
				},
			},
		},
	}

	sink := cacheResolutionSink{
		preparedJobs: func() []prefetch.PreparedJob {
			return preparedJobs
		},
	}

	// Resolving the same SHA for task-B within the same job → warm_cache
	tracker := &assetOperationTracker{cacheEnabled: true}
	ctx := withAssetOperationTracker(context.Background(), tracker)
	sink.RecordResolution(ctx, downloader.CacheResolution{
		AssetID:   "asset-X",
		Outcome:   downloader.CacheOutcomeHitValid,
		CacheHit:  true,
		SHA256:    assetref.ContentHash(sharedSHA),
		SizeBytes: int64(len(payload)),
		Source:    downloader.CacheSourceLocalDisk,
		JobID:     "job-A",
		TaskID:    "task-B",
		AssetKey:  "asset-X",
	})
	cache := tracker.cacheSnapshot()
	if cache.OriginWarmCacheCount != 1 {
		t.Fatalf("OriginWarmCacheCount = %d, want 1 (same job, different task)", cache.OriginWarmCacheCount)
	}
	if cache.OriginPrefetchCount != 0 {
		t.Fatalf("OriginPrefetchCount = %d, want 0", cache.OriginPrefetchCount)
	}
}

// ── Temporal proof: PreparedAt must precede ResolvedAt ─────────────────────
// The origin proof requires that the asset was prepared (materialized by
// FutureAssetPlan) BEFORE the current resolution. When PreparedAt >= ResolvedAt,
// the classification falls back to OriginWarmCache.
func TestCertification_TemporalProof_PreparedAtMustPrecedeResolvedAt(t *testing.T) {
	payload := []byte("temporal-proof-asset")
	sha := sha256hex(payload)

	now := time.Now().UTC()

	// PreparedJob with PreparedAt 10 seconds ago.
	preparedJobs := []prefetch.PreparedJob{
		{
			JobID:      "job-T",
			TaskID:     "task-T",
			State:      prefetch.PreparationStatePrepared,
			PreparedAt: now.Add(-10 * time.Second),
			Assets: map[string]prefetch.PreparedAssetMetadata{
				"asset-T": {
					AssetKey:   "asset-T",
					AssetID:    "asset-T",
					SHA256:     sha,
					SizeBytes:  int64(len(payload)),
					PreparedAt: now.Add(-10 * time.Second),
				},
			},
		},
	}

	sink := cacheResolutionSink{
		preparedJobs: func() []prefetch.PreparedJob { return preparedJobs },
	}

	// Case 1: ResolvedAt is AFTER PreparedAt → OriginPrefetch.
	tracker1 := &assetOperationTracker{cacheEnabled: true}
	ctx1 := withAssetOperationTracker(context.Background(), tracker1)
	sink.RecordResolution(ctx1, downloader.CacheResolution{
		AssetID:    "asset-T",
		Outcome:    downloader.CacheOutcomeHitValid,
		CacheHit:   true,
		SHA256:     assetref.ContentHash(sha),
		SizeBytes:  int64(len(payload)),
		Source:     downloader.CacheSourceLocalDisk,
		JobID:      "job-T",
		TaskID:     "task-T",
		AssetKey:   "asset-T",
		ResolvedAt: now, // after PreparedAt (-10s)
	})
	cache1 := tracker1.cacheSnapshot()
	if cache1.OriginPrefetchCount != 1 {
		t.Fatalf("ResolvedAt after PreparedAt: OriginPrefetchCount = %d, want 1", cache1.OriginPrefetchCount)
	}

	// Case 2: ResolvedAt is BEFORE PreparedAt → OriginWarmCache (temporal proof fails).
	tracker2 := &assetOperationTracker{cacheEnabled: true}
	ctx2 := withAssetOperationTracker(context.Background(), tracker2)
	sink.RecordResolution(ctx2, downloader.CacheResolution{
		AssetID:    "asset-T",
		Outcome:    downloader.CacheOutcomeHitValid,
		CacheHit:   true,
		SHA256:     assetref.ContentHash(sha),
		SizeBytes:  int64(len(payload)),
		Source:     downloader.CacheSourceLocalDisk,
		JobID:      "job-T",
		TaskID:     "task-T",
		AssetKey:   "asset-T",
		ResolvedAt: now.Add(-20 * time.Second), // before PreparedAt (-10s)
	})
	cache2 := tracker2.cacheSnapshot()
	if cache2.OriginWarmCacheCount != 1 {
		t.Fatalf("ResolvedAt before PreparedAt: OriginWarmCacheCount = %d, want 1", cache2.OriginWarmCacheCount)
	}
	if cache2.OriginPrefetchCount != 0 {
		t.Fatalf("ResolvedAt before PreparedAt: OriginPrefetchCount = %d, want 0", cache2.OriginPrefetchCount)
	}

	// Case 3: ResolvedAt equals PreparedAt → OriginWarmCache (not strictly before).
	tracker3 := &assetOperationTracker{cacheEnabled: true}
	ctx3 := withAssetOperationTracker(context.Background(), tracker3)
	sink.RecordResolution(ctx3, downloader.CacheResolution{
		AssetID:    "asset-T",
		Outcome:    downloader.CacheOutcomeHitValid,
		CacheHit:   true,
		SHA256:     assetref.ContentHash(sha),
		SizeBytes:  int64(len(payload)),
		Source:     downloader.CacheSourceLocalDisk,
		JobID:      "job-T",
		TaskID:     "task-T",
		AssetKey:   "asset-T",
		ResolvedAt: now.Add(-10 * time.Second), // exactly equals PreparedAt
	})
	cache3 := tracker3.cacheSnapshot()
	if cache3.OriginWarmCacheCount != 1 {
		t.Fatalf("ResolvedAt equals PreparedAt: OriginWarmCacheCount = %d, want 1", cache3.OriginWarmCacheCount)
	}
}

// ── Temporal proof: zero ResolvedAt skips temporal check (backward compat) ──
// When ResolvedAt is zero (legacy/test paths), the temporal check is skipped
// and the identity match alone determines the origin.
func TestCertification_TemporalProof_ZeroResolvedAtSkipsCheck(t *testing.T) {
	payload := []byte("zero-resolvedat-asset")
	sha := sha256hex(payload)

	preparedJobs := []prefetch.PreparedJob{
		{
			JobID:      "job-Z",
			TaskID:     "task-Z",
			State:      prefetch.PreparationStatePrepared,
			PreparedAt: time.Now().UTC(),
			Assets: map[string]prefetch.PreparedAssetMetadata{
				"asset-Z": {
					AssetKey:  "asset-Z",
					AssetID:   "asset-Z",
					SHA256:    sha,
					SizeBytes: int64(len(payload)),
				},
			},
		},
	}

	sink := cacheResolutionSink{
		preparedJobs: func() []prefetch.PreparedJob { return preparedJobs },
	}

	// ResolvedAt is zero → temporal check skipped → OriginPrefetch.
	tracker := &assetOperationTracker{cacheEnabled: true}
	ctx := withAssetOperationTracker(context.Background(), tracker)
	sink.RecordResolution(ctx, downloader.CacheResolution{
		AssetID:   "asset-Z",
		Outcome:   downloader.CacheOutcomeHitValid,
		CacheHit:  true,
		SHA256:    assetref.ContentHash(sha),
		SizeBytes: int64(len(payload)),
		Source:    downloader.CacheSourceLocalDisk,
		JobID:     "job-Z",
		TaskID:    "task-Z",
		AssetKey:  "asset-Z",
		// ResolvedAt is zero value → temporal check skipped
	})
	cache := tracker.cacheSnapshot()
	if cache.OriginPrefetchCount != 1 {
		t.Fatalf("zero ResolvedAt: OriginPrefetchCount = %d, want 1 (temporal check skipped)", cache.OriginPrefetchCount)
	}
}

// ── Temporal proof: zero PreparedAt skips temporal check (backward compat) ──
// When PreparedAt on the PreparedAssetMetadata is zero (legacy/test paths),
// the temporal check is skipped and the identity match alone determines the origin.
func TestCertification_TemporalProof_ZeroPreparedAtSkipsCheck(t *testing.T) {
	payload := []byte("zero-preparedat-asset")
	sha := sha256hex(payload)

	preparedJobs := []prefetch.PreparedJob{
		{
			JobID:      "job-Z",
			TaskID:     "task-Z",
			State:      prefetch.PreparationStatePrepared,
			PreparedAt: time.Now().UTC(),
			Assets: map[string]prefetch.PreparedAssetMetadata{
				"asset-Z": {
					AssetKey:  "asset-Z",
					AssetID:   "asset-Z",
					SHA256:    sha,
					SizeBytes: int64(len(payload)),
					// PreparedAt is zero → temporal check skipped
				},
			},
		},
	}

	sink := cacheResolutionSink{
		preparedJobs: func() []prefetch.PreparedJob { return preparedJobs },
	}

	// ResolvedAt is in the past, but PreparedAt is zero → skip → OriginPrefetch.
	tracker := &assetOperationTracker{cacheEnabled: true}
	ctx := withAssetOperationTracker(context.Background(), tracker)
	sink.RecordResolution(ctx, downloader.CacheResolution{
		AssetID:    "asset-Z",
		Outcome:    downloader.CacheOutcomeHitValid,
		CacheHit:   true,
		SHA256:     assetref.ContentHash(sha),
		SizeBytes:  int64(len(payload)),
		Source:     downloader.CacheSourceLocalDisk,
		JobID:      "job-Z",
		TaskID:     "task-Z",
		AssetKey:   "asset-Z",
		ResolvedAt: time.Now().UTC(),
	})
	cache := tracker.cacheSnapshot()
	if cache.OriginPrefetchCount != 1 {
		t.Fatalf("zero PreparedAt: OriginPrefetchCount = %d, want 1 (temporal check skipped)", cache.OriginPrefetchCount)
	}
}
