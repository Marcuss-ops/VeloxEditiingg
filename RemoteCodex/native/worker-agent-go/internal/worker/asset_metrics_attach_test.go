package worker

import (
	"strings"
	"testing"
	"time"

	sharedtelemetry "velox-shared/telemetry"

	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/internal/telemetry"
)

func TestAttachAssetOperationsProjectsResolverCacheCounters(t *testing.T) {
	tracker := &assetOperationTracker{cacheEnabled: true}
	// The counters are fed exclusively by the canonical resolver sink.
	tracker.recordResolution(downloader.CacheResolution{AssetID: "hit", CacheHit: true, Outcome: downloader.CacheOutcomeHitValid, Source: downloader.CacheSourceLocalDisk})
	tracker.recordResolution(downloader.CacheResolution{AssetID: "miss", CacheHit: false, Outcome: downloader.CacheOutcomeMissNotFound, Downloaded: true, DownloadBytes: 4096, Source: downloader.CacheSourceMaster})
	tracker.add(AssetOperationRecord{AssetID: "hit", CacheStatus: "hit"})
	tracker.add(AssetOperationRecord{AssetID: "miss", CacheStatus: "miss"})
	report := taskrunner.TaskExecutionReport{
		RawMetrics: &telemetry.RawExecutionMetrics{
			AssetCacheMissCount: 77,
			CacheDownloadCount:  9,
		},
	}
	attachAssetOperations(&report, tracker)
	if report.RawMetrics == nil {
		t.Fatal("raw metrics are nil after resolver projection")
	}
	if report.RawMetrics.CacheLookups != 2 || report.RawMetrics.AssetCacheHitCount != 1 || report.RawMetrics.AssetCacheMissCount != 1 || report.RawMetrics.CacheDownloadCount != 1 || report.RawMetrics.CacheDownloadBytes != 4096 || report.RawMetrics.UniqueAssetsRequested != 2 {
		t.Fatalf("raw cache counters = %+v", *report.RawMetrics)
	}
	if report.TypedMetrics != report.RawMetrics {
		t.Fatal("typed metrics must alias the canonical raw envelope")
	}
	if len(report.AssetOperations) != 2 {
		t.Fatalf("asset operation records = %d, want 2", len(report.AssetOperations))
	}
}

// TestAssetPreparationSummary_AggregatesPerAttemptDrillDown locks the STEP D
// contract: the per-attempt asset-preparation summary aggregates the
// per-transfer sub-phases from the canonical resolver sink and exposes ready-
// before vs downloaded-during counts, keeping wall and work distinct.
func TestAssetPreparationSummary_AggregatesPerAttemptDrillDown(t *testing.T) {
	tracker := &assetOperationTracker{cacheEnabled: true}
	// One warm (cache-hit) asset and one download with full sub-phase timing.
	tracker.recordResolution(downloader.CacheResolution{
		AssetID: "warm.mp4", CacheHit: true, Outcome: downloader.CacheOutcomeHitValid,
		Source: downloader.CacheSourceLocalDisk,
		Timing: downloader.AssetSubPhases{CacheLookupMS: 12},
	})
	tracker.recordResolution(downloader.CacheResolution{
		AssetID: "cold.mp4", CacheHit: false, Outcome: downloader.CacheOutcomeMissNotFound,
		Downloaded: true, DownloadBytes: 8192, Source: downloader.CacheSourceMaster,
		Timing: downloader.AssetSubPhases{
			CacheLookupMS: 15, RemoteWaitMS: 9000, DownloadWallMS: 3200,
			DownloadWorkMS: 3100, HashVerifyMS: 40, MetadataProbeMS: 5, MaterializeLocalMS: 55,
		},
	})

	prep := tracker.prepSnapshot()
	if prep.AssetsTotal != 2 || prep.AssetsUnique != 2 {
		t.Fatalf("assets total/unique = %d/%d, want 2/2", prep.AssetsTotal, prep.AssetsUnique)
	}
	if prep.ReadyBefore != 1 || prep.DownloadedNow != 1 || prep.CacheHits != 1 || prep.CacheMisses != 1 {
		t.Fatalf("ready/downloaded/hits/misses = %d/%d/%d/%d", prep.ReadyBefore, prep.DownloadedNow, prep.CacheHits, prep.CacheMisses)
	}
	if prep.CacheLookupMS != 27 || prep.RemoteWaitMS != 9000 || prep.RemoteWaitCount != 1 {
		t.Fatalf("lookup/remote = %d/%d/%d, want 27/9000/1", prep.CacheLookupMS, prep.RemoteWaitMS, prep.RemoteWaitCount)
	}
	if prep.DownloadWallMS != 3200 || prep.DownloadWorkSum != 3100 || prep.HashVerifyMS != 40 || prep.MetadataProbeMS != 5 || prep.MaterializeLocalMS != 55 {
		t.Fatalf("download/hash/probe/materialize = %d/%d/%d/%d/%d", prep.DownloadWallMS, prep.DownloadWorkSum, prep.HashVerifyMS, prep.MetadataProbeMS, prep.MaterializeLocalMS)
	}

	// The report carries the drill-down through its structured field.
	report := taskrunner.TaskExecutionReport{}
	attachAssetOperations(&report, tracker)

	// Typed wire drill-down: buildTaskResult maps this struct 1:1 onto
	// pb.AssetPreparationBreakdown so the Master decodes the same measured
	// values from raw_report_json.
	if report.AssetPreparation == nil {
		t.Fatal("typed asset_preparation drill-down missing after resolver observations")
	}
	tp := *report.AssetPreparation
	want := sharedtelemetry.AssetPreparationBreakdown{
		AssetsRequired: 2, AssetsUnique: 2, CacheHits: 1, CacheMisses: 1,
		ReadyBeforeAttempt: 1, DownloadedDuringAttempt: 1,
		CacheLookupMS: 27, RemoteWaitMS: 9000, RemoteWaitCount: 1,
		DownloadWallMS: 3200, DownloadWorkMS: 3100,
		HashVerifyMS: 40, MetadataProbeMS: 5, MaterializeLocalMS: 55,
		// Bytes/origin are zero: this test records resolutions without
		// Origin set (tracker.recordResolution, not via cacheResolutionSink).
	}
	if tp != want {
		t.Fatalf("typed breakdown = %+v, want %+v", tp, want)
	}
}

func TestAttachAssetOperationsPreservesAbsentCacheFacts(t *testing.T) {
	report := taskrunner.TaskExecutionReport{
		RawMetrics: &telemetry.RawExecutionMetrics{
			CacheLookups:          12,
			AssetCacheHitCount:    12,
			AssetCacheMissCount:   0,
			CacheDownloadCount:    2,
			CacheDownloadBytes:    1024,
			UniqueAssetsRequested: 4,
		},
	}
	tracker := &assetOperationTracker{cacheEnabled: true}

	attachAssetOperations(&report, tracker)

	// An idle resolver emitted no fact. Do not turn the absent observation into
	// zeroes or erase facts already supplied by the canonical raw producer.
	got := report.RawMetrics
	if got == nil || got.CacheLookups != 12 || got.AssetCacheHitCount != 12 || got.CacheDownloadCount != 2 || got.CacheDownloadBytes != 1024 || got.UniqueAssetsRequested != 4 {
		t.Fatalf("absent resolver facts changed raw metrics: %+v", got)
	}
	// Same honesty for the typed drill-down: an idle attempt carries NO
	// breakdown instead of a zero-filled fake measurement.
	if report.AssetPreparation != nil {
		t.Fatalf("idle tracker produced a fabricated breakdown: %+v", *report.AssetPreparation)
	}
}

// TestAttemptCacheMetrics_StartAtZeroPerAttempt locks Phase A1's core
// contract: per-attempt cache accounting starts at zero and is fed only by
// the canonical resolver sink. A warm second attempt never inherits the
// previous attempt's miss/download counters (the plan's example: attempt A
// 169 lookups / 143 hits / 26 misses; attempt B 169 / 169 / 0).
func TestAttemptCacheMetrics_StartAtZeroPerAttempt(t *testing.T) {
	trackerA := &assetOperationTracker{}
	trackerB := &assetOperationTracker{}

	// Attempt A: cold — 143 hits + 26 misses (26 downloads of 1 KiB each).
	for i := 0; i < 169; i++ {
		if i < 143 {
			trackerA.recordResolution(downloader.CacheResolution{AssetID: "a", CacheHit: true, Outcome: downloader.CacheOutcomeHitValid, Source: downloader.CacheSourceLocalDisk})
		} else {
			trackerA.recordResolution(downloader.CacheResolution{AssetID: "a", CacheHit: false, Outcome: downloader.CacheOutcomeMissNotFound, Downloaded: true, DownloadBytes: 1024, Source: downloader.CacheSourceMaster})
		}
	}
	// Attempt B: warm — every lookup is a verified hit, zero misses. The
	// tracker is a fresh per-attempt accumulator: it must NOT inherit A's
	// 26 misses (the historical worker-cumulative contamination bug).
	for i := 0; i < 169; i++ {
		trackerB.recordResolution(downloader.CacheResolution{AssetID: "a", CacheHit: true, Outcome: downloader.CacheOutcomeHitValid, Source: downloader.CacheSourceLocalDisk})
	}

	a := trackerA.cacheSnapshot()
	if a.CacheLookups != 169 || a.CacheHits != 143 || a.CacheMisses != 26 || a.CacheDownloadCount != 26 || a.CacheDownloadBytes != 26*1024 {
		t.Fatalf("attempt A counters = %+v, want 169/143/26/26/26624", a)
	}
	b := trackerB.cacheSnapshot()
	if b.CacheLookups != 169 || b.CacheHits != 169 || b.CacheMisses != 0 || b.CacheDownloadCount != 0 || b.CacheDownloadBytes != 0 {
		t.Fatalf("attempt B counters = %+v, want 169/169/0/0/0 (warm second wave must start at zero)", b)
	}
}

func TestAttachAssetOperationsToPhaseMarkersMakesRecordsReportable(t *testing.T) {
	start := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	end := start.Add(250 * time.Millisecond)
	report := taskrunner.TaskExecutionReport{
		AssetOperations: []AssetOperationRecord{{
			AssetID:             "asset-1",
			CacheStatus:         "miss",
			DownloadStartedAt:   start,
			DownloadCompletedAt: end,
			DownloadMS:          250,
			DownloadedBytes:     42,
			SHA256Verified:      true,
			IntegrityCheck:      "sha256",
			IntegrityValid:      true,
			LocalPath:           "/worker/cache/asset-1.mp3",
			Source:              "master_asset_bridge",
		}},
		PhaseMarkers: []taskrunner.PhaseMarker{{
			Name:        taskrunner.PhasePrefetch,
			StartedAt:   start,
			CompletedAt: end,
			Status:      "ok",
		}},
	}

	attachAssetOperationsToPhaseMarkers(&report)

	if len(report.PhaseMarkers) != 1 {
		t.Fatalf("phase markers = %d, want 1", len(report.PhaseMarkers))
	}
	marker := report.PhaseMarkers[0]
	if marker.Name != taskrunner.PhasePrefetch || marker.Status != "ok" {
		t.Fatalf("marker = %+v, want prefetch/ok", marker)
	}
	if marker.StartedAt != start || marker.CompletedAt != end {
		t.Fatalf("marker timestamps = %s/%s, want %s/%s", marker.StartedAt, marker.CompletedAt, start, end)
	}
	if !strings.Contains(marker.Notes, "asset_operations=") || !strings.Contains(marker.Notes, "asset-1") {
		t.Fatalf("marker notes do not contain asset report: %q", marker.Notes)
	}
}
