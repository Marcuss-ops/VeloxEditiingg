package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"velox-shared/assetref"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/internal/workercache"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

type dispatchLeaseProbeExecutor struct {
	wait    bool
	fail    bool
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (e *dispatchLeaseProbeExecutor) Descriptor() executor.Descriptor {
	return executor.Descriptor{
		ID: "render_batch", Version: 1,
		ResourceClass: executor.ResourceCPU, TemporalMode: executor.TemporalGlobal,
		Deterministic: true, Cacheable: true,
	}
}

func (e *dispatchLeaseProbeExecutor) Validate(executor.TaskSpec) error { return nil }

func (e *dispatchLeaseProbeExecutor) Execute(ctx context.Context, _ executor.ExecutionContext, _ executor.TaskSpec) (executor.ExecutionResult, error) {
	e.once.Do(func() { close(e.started) })
	if e.wait {
		select {
		case <-e.release:
		case <-ctx.Done():
			return executor.ExecutionResult{}, ctx.Err()
		}
	}
	if e.fail {
		return executor.ExecutionResult{}, errors.New("forced render failure")
	}
	return executor.ExecutionResult{Status: "succeeded"}, nil
}

func newDispatchV2LeaseFixture(t *testing.T, probe *dispatchLeaseProbeExecutor) (*Worker, *workercache.Cache, map[string]interface{}, []string) {
	t.Helper()
	assets := map[string][]byte{
		"v2-video": []byte("prepared-video-bytes"),
		"v2-audio": []byte("final-audio-bytes"),
	}
	payload := compiledPlanAssetPayload(t, assets)
	cache, err := workercache.Open(filepath.Join(t.TempDir(), "cache.db"))
	if err != nil {
		t.Fatalf("open worker cache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	for assetID, body := range assets {
		path := filepath.Join(t.TempDir(), assetID+".asset")
		if err := os.WriteFile(path, body, 0o640); err != nil {
			t.Fatalf("write %s: %v", assetID, err)
		}
		hash := assetSHA(body)
		if err := cache.Store(context.Background(), workercache.Entry{
			AssetKey:         assetref.AssetKey(assetID),
			ContentHash:      assetref.ContentHash(hash),
			LocalPath:        path,
			SizeBytes:        int64(len(body)),
			DownloadComplete: true,
		}); err != nil {
			t.Fatalf("store %s: %v", assetID, err)
		}
	}

	registry := executor.NewRegistry()
	if err := registry.Register(probe); err != nil {
		t.Fatalf("register probe executor: %v", err)
	}
	log := logger.New(logger.InfoLevel, os.Stderr)
	w := &Worker{
		config: &config.WorkerConfig{WorkerID: "worker-v2-lease", WorkDir: t.TempDir()},
		logger: log, activeTasks: make(map[string]*ActiveTaskExecution),
		taskRunner: taskrunner.NewTaskRunner(registry, log),
		clipCache:  cache, canonicalAssetCache: workercache.NewCanonicalAssetStore(cache),
	}
	t.Cleanup(func() {
		if w.assetManager != nil {
			w.assetManager.Close()
		}
	})
	keys := extractAssetKeysFromJSON(payload)
	return w, cache, payload, keys
}

func dispatchV2LeaseTask(payload map[string]interface{}, jobID string) *PendingTaskExecution {
	return &PendingTaskExecution{
		TaskID: "task-" + jobID, JobID: jobID, AttemptID: "attempt-" + jobID,
		ExecutorID: "render_batch", ExecutorVersion: 1,
		Spec: executor.TaskSpec{
			Version: 1, JobID: jobID, ExecutorID: "render_batch", Payload: payload,
		},
	}
}

func assertV2Leases(t *testing.T, cache *workercache.Cache, keys []string, jobID string, wantCount int) {
	t.Helper()
	for _, key := range keys {
		entry, found, err := cache.Find(context.Background(), key)
		if err != nil || !found {
			t.Fatalf("find %s: found=%v err=%v", key, found, err)
		}
		if entry.ActiveJobID != jobID || entry.ActiveLeaseCount != wantCount {
			t.Fatalf("asset %s lease = job:%q count:%d, want job:%q count:%d", key, entry.ActiveJobID, entry.ActiveLeaseCount, jobID, wantCount)
		}
	}
}

func TestDispatchTaskRunner_V2LeasesAllAssetsAndReleasesOnSuccess(t *testing.T) {
	probe := &dispatchLeaseProbeExecutor{wait: true, started: make(chan struct{}), release: make(chan struct{})}
	w, cache, payload, keys := newDispatchV2LeaseFixture(t, probe)
	jobID := "job-v2-lease-success"
	resultCh := make(chan error, 1)
	go func() {
		_, err := w.dispatchTaskRunner(context.Background(), dispatchV2LeaseTask(payload, jobID))
		resultCh <- err
	}()

	<-probe.started
	assertV2Leases(t, cache, keys, jobID, 1)
	if !containsString(keys, "v2-audio") {
		t.Fatalf("lease set %v does not include final_audio", keys)
	}
	close(probe.release)
	if err := <-resultCh; err != nil {
		t.Fatalf("dispatch success returned error: %v", err)
	}
	assertV2Leases(t, cache, keys, "", 0)
}

func TestDispatchTaskRunner_V2ReleasesAllLeasesOnExecutorError(t *testing.T) {
	probe := &dispatchLeaseProbeExecutor{fail: true, started: make(chan struct{}), release: make(chan struct{})}
	w, cache, payload, keys := newDispatchV2LeaseFixture(t, probe)
	jobID := "job-v2-lease-error"
	if _, err := w.dispatchTaskRunner(context.Background(), dispatchV2LeaseTask(payload, jobID)); err == nil {
		t.Fatal("dispatch executor error returned nil")
	}
	assertV2Leases(t, cache, keys, "", 0)
}

func TestDispatchTaskRunner_V2RejectsMissingClipCache(t *testing.T) {
	probe := &dispatchLeaseProbeExecutor{started: make(chan struct{}), release: make(chan struct{})}
	w, cache, payload, keys := newDispatchV2LeaseFixture(t, probe)
	w.clipCache = nil
	if _, err := w.dispatchTaskRunner(context.Background(), dispatchV2LeaseTask(payload, "job-v2-lease-no-cache")); err == nil {
		t.Fatal("V2 dispatch without clip cache returned nil")
	}
	assertV2Leases(t, cache, keys, "", 0)
}

func TestDispatchTaskRunner_V2ReleasesAllLeasesAfterCancellation(t *testing.T) {
	probe := &dispatchLeaseProbeExecutor{wait: true, started: make(chan struct{}), release: make(chan struct{})}
	w, cache, payload, keys := newDispatchV2LeaseFixture(t, probe)
	jobID := "job-v2-lease-cancel"
	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan error, 1)
	go func() {
		_, err := w.dispatchTaskRunner(ctx, dispatchV2LeaseTask(payload, jobID))
		resultCh <- err
	}()
	<-probe.started
	assertV2Leases(t, cache, keys, jobID, 1)
	cancel()
	if err := <-resultCh; err == nil {
		t.Fatal("canceled dispatch returned nil")
	}
	assertV2Leases(t, cache, keys, "", 0)
}

func TestDispatchTaskRunner_V2ReleasesAllLeasesAfterTimeout(t *testing.T) {
	probe := &dispatchLeaseProbeExecutor{wait: true, started: make(chan struct{}), release: make(chan struct{})}
	w, cache, payload, keys := newDispatchV2LeaseFixture(t, probe)
	jobID := "job-v2-lease-timeout"
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := w.dispatchTaskRunner(ctx, dispatchV2LeaseTask(payload, jobID)); err == nil {
		t.Fatal("timed-out dispatch returned nil")
	}
	assertV2Leases(t, cache, keys, "", 0)
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
