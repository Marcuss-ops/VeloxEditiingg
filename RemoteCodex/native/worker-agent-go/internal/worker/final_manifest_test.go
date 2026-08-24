package worker

import (
	"context"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"velox-shared/contract/assembly"
	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/pkg/logger"
)

func TestWorkerApplyFinalManifestDeltaUsesIncrementalCacheResolution(t *testing.T) {
	var checks atomic.Int32
	transferer := downloader.TransfererFunc(func(_ context.Context, _ context.Context, req downloader.DownloadRequest, check bool, _ func(int64)) (downloader.CacheCheckResult, downloader.TransferResult, error) {
		if !check {
			t.Fatal("final manifest resolution must not reach byte transfer in this cache-hit test")
		}
		checks.Add(1)
		return downloader.CacheCheckResult{
			CacheHit: true, LocalPath: "/cache/" + req.AssetID,
			SHA256: req.SHA256, Outcome: downloader.CacheOutcomeHitValid,
		}, downloader.TransferResult{}, nil
	})
	manager := downloader.NewManager(downloader.Config{Concurrency: 1}, transferer)
	defer manager.Close()
	w := &Worker{
		cacheResolver: downloader.NewCacheResolver(manager, nil),
		logger:        logger.New(logger.InfoLevel, io.Discard),
	}
	prepHash := "sha256:" + strings.Repeat("0", 64)
	digestA := strings.Repeat("a", 64)
	digestB := strings.Repeat("b", 64)
	artifactA := assembly.PublishedArtifact{
		JobID: "worker-final-job", TimelineRevision: 9, AssetID: "scene-a",
		StorageURL: "https://master.test/scene-a", SHA256: digestA, SizeBytes: 10,
		ProfileID: assembly.CanonicalAssemblyProfileLegacyV1, Producer: assembly.ProducerChronon,
	}
	artifactB := artifactA
	artifactB.AssetID = "scene-b"
	artifactB.StorageURL = "https://master.test/scene-b"
	artifactB.SHA256 = digestB

	firstDelta := assembly.FinalManifestDelta{
		ContractVersion: assembly.ContractVersion, JobID: artifactA.JobID, BaseRevision: 1,
		Revision: 2, PreparationHash: prepHash, TimelineRevision: 9,
		TimelineHash: "sha256:" + strings.Repeat("1", 64), ExpectedProfile: assembly.CanonicalAssemblyProfileLegacyV1,
		UpsertedArtifacts: []assembly.PublishedArtifact{artifactA},
	}
	first, err := w.ApplyFinalManifestDelta(context.Background(), firstDelta)
	if err != nil {
		t.Fatalf("first delta: %v", err)
	}
	if !first.Ready || first.State != "READY" || checks.Load() != 1 {
		t.Fatalf("first result=%#v checks=%d", first, checks.Load())
	}

	replay, err := w.ApplyFinalManifestDelta(context.Background(), firstDelta)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replay.Disposition != assembly.RevisionReplay || len(replay.ResolvedArtifacts) != 0 || checks.Load() != 1 {
		t.Fatalf("replay=%#v checks=%d; replay resolved bytes again", replay, checks.Load())
	}

	secondDelta := firstDelta
	secondDelta.BaseRevision = 2
	secondDelta.Revision = 3
	secondDelta.UpsertedArtifacts = []assembly.PublishedArtifact{artifactB}
	second, err := w.ApplyFinalManifestDelta(context.Background(), secondDelta)
	if err != nil {
		t.Fatalf("incremental delta: %v", err)
	}
	if !second.Ready || len(second.Manifest.Artifacts) != 2 || checks.Load() != 2 {
		t.Fatalf("incremental result=%#v checks=%d", second, checks.Load())
	}
}
