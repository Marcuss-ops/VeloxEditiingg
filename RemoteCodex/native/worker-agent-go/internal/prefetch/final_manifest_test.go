package prefetch

import (
	"context"
	"strings"
	"sync"
	"testing"

	"velox-shared/contract/assembly"
	"velox-worker-agent/internal/downloader"
)

type finalManifestResolver struct {
	mu       sync.Mutex
	requests []downloader.DownloadRequest
	cacheHit bool
}

func (r *finalManifestResolver) Resolve(_ context.Context, request downloader.DownloadRequest) (downloader.CacheResolution, error) {
	r.mu.Lock()
	r.requests = append(r.requests, request)
	r.mu.Unlock()
	return downloader.CacheResolution{
		AssetID: request.AssetID, Outcome: downloader.CacheOutcomeHitValid,
		LocalPath: "/cache/" + request.AssetID, CacheHit: r.cacheHit,
		SHA256: request.SHA256, DownloadBytes: func() int64 {
			if r.cacheHit {
				return 0
			}
			return request.SizeBytes
		}(),
	}, nil
}

func (r *finalManifestResolver) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.requests)
}

func finalWorkerBase() assembly.FinalAssemblyManifest {
	return assembly.FinalAssemblyManifest{
		ContractVersion: assembly.ContractVersion, JobID: "job-final-worker", Revision: 1,
		PreparationHash: "sha256:" + strings.Repeat("0", 64), TimelineRevision: 4,
		TimelineHash: "sha256:" + strings.Repeat("t", 64), ExpectedProfile: assembly.CanonicalAssemblyProfileLegacyV1,
		Artifacts: []assembly.PublishedArtifact{},
	}
}

func finalWorkerArtifact(jobID, id, digest string) assembly.PublishedArtifact {
	return assembly.PublishedArtifact{
		JobID: jobID, TimelineRevision: 4, AssetID: id,
		StorageURL: "https://master.test/artifacts/" + id, SHA256: digest, SizeBytes: 100,
		ProfileID: assembly.CanonicalAssemblyProfileLegacyV1, Producer: assembly.ProducerChronon,
	}
}

func finalWorkerDelta(base, revision uint64, prep string, artifacts ...assembly.PublishedArtifact) assembly.FinalManifestDelta {
	return assembly.FinalManifestDelta{
		ContractVersion: assembly.ContractVersion, JobID: "job-final-worker", BaseRevision: base,
		Revision: revision, PreparationHash: prep, TimelineRevision: 4,
		TimelineHash: "sha256:" + strings.Repeat("t", 64), ExpectedProfile: assembly.CanonicalAssemblyProfileLegacyV1,
		UpsertedArtifacts: artifacts,
	}
}

func TestFinalManifestReconcilerResolvesOnlyIncrementalChrononArtifacts(t *testing.T) {
	resolver := &finalManifestResolver{}
	reconciler, err := NewFinalManifestReconciler(finalWorkerBase(), resolver, nil)
	if err != nil {
		t.Fatal(err)
	}
	firstArtifact := finalWorkerArtifact("job-final-worker", "scene-001", strings.Repeat("a", 64))
	firstDelta := finalWorkerDelta(1, 2, finalWorkerBase().PreparationHash, firstArtifact)
	first, err := reconciler.ApplyDelta(context.Background(), firstDelta)
	if err != nil {
		t.Fatalf("first delta: %v", err)
	}
	if first.State != FinalManifestReady || !first.Ready || resolver.count() != 1 {
		t.Fatalf("first result=%#v resolve_count=%d", first, resolver.count())
	}

	secondArtifact := finalWorkerArtifact("job-final-worker", "scene-002", strings.Repeat("b", 64))
	secondDelta := finalWorkerDelta(2, 3, finalWorkerBase().PreparationHash, secondArtifact)
	second, err := reconciler.ApplyDelta(context.Background(), secondDelta)
	if err != nil {
		t.Fatalf("second delta: %v", err)
	}
	if second.State != FinalManifestReady || len(second.Manifest.Artifacts) != 2 || len(second.ResolvedArtifacts) != 1 {
		t.Fatalf("second result=%#v", second)
	}
	if got := resolver.count(); got != 2 {
		t.Fatalf("incremental resolution count=%d, want 2", got)
	}

	replay, err := reconciler.ApplyDelta(context.Background(), secondDelta)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replay.Disposition != assembly.RevisionReplay || len(replay.ResolvedArtifacts) != 0 || resolver.count() != 2 {
		t.Fatalf("replay=%#v resolve_count=%d; existing artifact was resolved again", replay, resolver.count())
	}

	invalidate := finalWorkerDelta(3, 4, finalWorkerBase().PreparationHash)
	invalidate.InvalidatedArtifactIDs = []string{"scene-002"}
	result, err := reconciler.ApplyDelta(context.Background(), invalidate)
	if err != nil {
		t.Fatalf("invalidation: %v", err)
	}
	if result.State != FinalManifestReady || len(result.Manifest.Artifacts) != 1 || result.Manifest.Artifacts[0].AssetID != "scene-001" {
		t.Fatalf("after invalidation=%#v", result)
	}
	if resolver.count() != 2 {
		t.Fatalf("invalidation caused a download, resolve_count=%d", resolver.count())
	}
}

func TestFinalManifestReconcilerRechecksChangedArtifactAndAcceptsCacheHit(t *testing.T) {
	resolver := &finalManifestResolver{cacheHit: true}
	reconciler, err := NewFinalManifestReconciler(finalWorkerBase(), resolver, nil)
	if err != nil {
		t.Fatal(err)
	}
	artifact := finalWorkerArtifact("job-final-worker", "scene-001", strings.Repeat("a", 64))
	firstDelta := finalWorkerDelta(1, 2, finalWorkerBase().PreparationHash, artifact)
	first, err := reconciler.ApplyDelta(context.Background(), firstDelta)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.ResolvedArtifacts) != 1 || !first.ResolvedArtifacts[0].CacheHit || !first.Ready {
		t.Fatalf("cache-hit result=%#v", first)
	}

	changed := artifact
	changed.SHA256 = strings.Repeat("c", 64)
	secondDelta := finalWorkerDelta(2, 3, finalWorkerBase().PreparationHash, changed)
	second, err := reconciler.ApplyDelta(context.Background(), secondDelta)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.ResolvedArtifacts) != 1 || resolver.count() != 2 {
		t.Fatalf("changed artifact result=%#v resolve_count=%d", second, resolver.count())
	}
	if second.Manifest.Artifacts[0].SHA256 != changed.SHA256 {
		t.Fatalf("manifest retained old digest: %#v", second.Manifest.Artifacts)
	}
}
