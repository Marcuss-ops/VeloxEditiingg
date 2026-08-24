package prefetch

import (
	"context"
	"sync"
	"testing"
	"time"

	"velox-shared/contract/assembly"
	"velox-worker-agent/internal/downloader"
)

type blockingFinalManifestResolver struct {
	started chan struct{}
	release chan struct{}
	mu      sync.Mutex
	calls   int
}

func (r *blockingFinalManifestResolver) Resolve(ctx context.Context, request downloader.DownloadRequest) (downloader.CacheResolution, error) {
	r.mu.Lock()
	r.calls++
	r.mu.Unlock()
	select {
	case r.started <- struct{}{}:
	default:
	}
	select {
	case <-r.release:
	case <-ctx.Done():
		return downloader.CacheResolution{}, ctx.Err()
	}
	return downloader.CacheResolution{
		AssetID: request.AssetID, LocalPath: "/verified/" + request.AssetID,
		SHA256: request.SHA256, DownloadBytes: request.SizeBytes,
		Outcome: downloader.CacheOutcomeMissNotFound,
	}, nil
}

func TestController_ConcurrentSamePlanHasSingleAppliedVersion(t *testing.T) {
	controller := NewController("worker")
	input := plan(t, 1, nowForConcurrentTest(), job("concurrent-job", "concurrent-reservation"))

	const callers = 24
	results := make(chan ReconcileResult, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := controller.Apply(input)
			results <- result
			errs <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errs)

	applied, stale := 0, 0
	for result := range results {
		if result.Applied {
			applied++
		}
		if result.Stale {
			stale++
		}
	}
	for err := range errs {
		if err != nil {
			t.Errorf("concurrent controller Apply() error = %v", err)
		}
	}
	if applied != 1 || stale != callers-1 || controller.Version() != 1 {
		t.Fatalf("applied=%d stale=%d version=%d, want 1/%d/1", applied, stale, controller.Version(), callers-1)
	}
}

func TestFinalManifestReconciler_ConcurrentDeltasCommitOneAndPreserveAtomicState(t *testing.T) {
	resolver := &blockingFinalManifestResolver{started: make(chan struct{}, 2), release: make(chan struct{})}
	reconciler, err := NewFinalManifestReconciler(finalWorkerBase(), resolver, nil)
	if err != nil {
		t.Fatal(err)
	}
	base := finalWorkerBase().PreparationHash
	deltaA := finalWorkerDelta(1, 2, base, finalWorkerArtifact("job-final-worker", "scene-a", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	deltaB := finalWorkerDelta(1, 2, base, finalWorkerArtifact("job-final-worker", "scene-b", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"))

	results := make(chan FinalManifestResult, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, delta := range []assembly.FinalManifestDelta{deltaA, deltaB} {
		wg.Add(1)
		go func(delta assembly.FinalManifestDelta) {
			defer wg.Done()
			result, err := reconciler.ApplyDelta(context.Background(), delta)
			results <- result
			errs <- err
		}(delta)
	}

	for i := 0; i < 2; i++ {
		<-resolver.started
	}
	close(resolver.release)
	wg.Wait()
	close(results)
	close(errs)

	successes, conflicts := 0, 0
	for err := range errs {
		if err == nil {
			successes++
			continue
		}
		conflicts++
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("concurrent final deltas successes=%d conflicts=%d, want 1/1", successes, conflicts)
	}
	snapshot := reconciler.Snapshot()
	if snapshot.Manifest.Revision != 2 || len(snapshot.Manifest.Artifacts) != 1 || !snapshot.Ready {
		t.Fatalf("final manifest after concurrent deltas=%#v, want one committed READY artifact", snapshot)
	}
}

func nowForConcurrentTest() (result time.Time) {
	return time.Now().UTC().Add(time.Minute)
}
