package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/internal/workercache"
	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

// TestDispatchTaskRunner_LegacyClipPayloadAcquiresLease is the FASE 6
// regression test. Legacy clip jobs express their assets as velox-drive://
// (or velox-asset://) wire references. dispatchTaskRunner must read the lease
// keys from the ORIGINAL payload BEFORE resolveTaskAssets rewrites those refs
// into local filesystem paths, otherwise the legacy walker sees an empty key
// set and silently skips the lease (the 0 lease-acquire bug).
//
// The test drives the full dispatch path hermetically:
//  1. The master asset bridge (httptest server) serves one legacy clip.
//  2. A blocking probe executor lets us assert the lease is held while the
//     render is in flight.
//  3. After release, the lease must be cleared.
func TestDispatchTaskRunner_LegacyClipPayloadAcquiresLease(t *testing.T) {
	const assetID = "legacy-clip"
	body := []byte("legacy-clip-bytes")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/api/v1/agent/assets/")
		if id != assetID {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "video/mp4")
		_, _ = w.Write(body)
	}))
	defer server.Close()

	cache, err := workercache.Open(filepath.Join(t.TempDir(), "legacy-lease.db"))
	if err != nil {
		t.Fatalf("open worker cache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	probe := &dispatchLeaseProbeExecutor{wait: true, started: make(chan struct{}), release: make(chan struct{})}
	registry := executor.NewRegistry()
	if err := registry.Register(probe); err != nil {
		t.Fatalf("register probe executor: %v", err)
	}
	log := logger.New(logger.InfoLevel, os.Stderr)
	w := &Worker{
		config:      &config.WorkerConfig{WorkerID: "worker-legacy-lease", MasterURL: server.URL, WorkDir: t.TempDir()},
		apiClient:   api.NewClient(server.URL),
		logger:      log,
		activeTasks: make(map[string]*ActiveTaskExecution),
		taskRunner:  taskrunner.NewTaskRunner(registry, log),
		clipCache:   cache,
		canonicalAssetCache: workercache.NewCanonicalAssetStore(cache),
	}

	payload := map[string]interface{}{
		"scenes": []interface{}{
			map[string]interface{}{
				"clip": map[string]interface{}{
					"url": "velox-drive://" + assetID,
				},
			},
		},
	}

	// Pin the mechanism: the original payload exposes the wire ref, and the
	// post-resolve payload must NOT (the resolver rewrites it to a local path).
	if got := extractAssetKeysFromJSON(payload); len(got) != 1 || got[0] != assetID {
		t.Fatalf("original payload lease keys = %v, want [%s]", got, assetID)
	}

	const jobID = "job-legacy-lease"
	pte := &PendingTaskExecution{
		TaskID: "task-" + jobID, JobID: jobID, AttemptID: "attempt-" + jobID,
		ExecutorID: "render_batch", ExecutorVersion: 1,
		Spec: executor.TaskSpec{
			Version: 1, JobID: jobID, ExecutorID: "render_batch", Payload: payload,
		},
	}

	resultCh := make(chan error, 1)
	go func() {
		_, err := w.dispatchTaskRunner(context.Background(), pte)
		resultCh <- err
	}()

	// The executor is running: the resolver has completed (asset downloaded +
	// stored) and the lease must now be held.
	<-probe.started
	entry, found, err := cache.Find(context.Background(), assetID)
	if err != nil || !found {
		t.Fatalf("find %s after dispatch: found=%v err=%v", assetID, found, err)
	}
	if entry.ActiveJobID != jobID || entry.ActiveLeaseCount != 1 {
		t.Fatalf("asset %s lease = job:%q count:%d, want job:%q count:1", assetID, entry.ActiveJobID, entry.ActiveLeaseCount, jobID)
	}
	// The lease can only be acquired if the resolver already downloaded and
	// stored the asset (AcquireJobClips rejects missing/undownloaded rows).
	// Confirm the row is a committed download, proving resolution ran before
	// the lease while the lease keys still came from the original payload.
	if !entry.DownloadComplete || entry.LocalPath == "" {
		t.Fatalf("asset %s row not a committed download: complete=%v path=%q", assetID, entry.DownloadComplete, entry.LocalPath)
	}
	// The resolver deep-copies the payload, so the caller's original payload
	// (with its velox-drive:// wire ref) is left intact. This is exactly why
	// the lease keys must be read from the original BEFORE resolveTaskAssets:
	// the resolved copy no longer carries the wire ref.
	if keys := extractAssetKeysFromJSON(pte.Spec.Payload); len(keys) != 1 || keys[0] != assetID {
		t.Fatalf("original payload lease keys after dispatch = %v, want [%s] (resolver is non-destructive)", keys, assetID)
	}

	close(probe.release)
	if err := <-resultCh; err != nil {
		t.Fatalf("dispatch success returned error: %v", err)
	}

	entry, found, err = cache.Find(context.Background(), assetID)
	if err != nil || !found {
		t.Fatalf("find %s after release: found=%v err=%v", assetID, found, err)
	}
	if entry.ActiveJobID != "" || entry.ActiveLeaseCount != 0 {
		t.Fatalf("asset %s lease after release = job:%q count:%d, want cleared", assetID, entry.ActiveJobID, entry.ActiveLeaseCount)
	}
}

// TestDispatchTaskRunner_LegacyClipPayloadReleasesLeaseOnExecutorError pins the
// release-on-error branch of the legacy lease path: the lease is acquired and
// then released even when the executor fails.
func TestDispatchTaskRunner_LegacyClipPayloadReleasesLeaseOnExecutorError(t *testing.T) {
	const assetID = "legacy-clip-err"
	body := []byte("legacy-clip-err-bytes")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/api/v1/agent/assets/")
		if id != assetID {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "video/mp4")
		_, _ = w.Write(body)
	}))
	defer server.Close()

	cache, err := workercache.Open(filepath.Join(t.TempDir(), "legacy-lease-err.db"))
	if err != nil {
		t.Fatalf("open worker cache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	probe := &dispatchLeaseProbeExecutor{fail: true, started: make(chan struct{}), release: make(chan struct{})}
	registry := executor.NewRegistry()
	if err := registry.Register(probe); err != nil {
		t.Fatalf("register probe executor: %v", err)
	}
	log := logger.New(logger.InfoLevel, os.Stderr)
	w := &Worker{
		config:      &config.WorkerConfig{WorkerID: "worker-legacy-lease-err", MasterURL: server.URL, WorkDir: t.TempDir()},
		apiClient:   api.NewClient(server.URL),
		logger:      log,
		activeTasks: make(map[string]*ActiveTaskExecution),
		taskRunner:  taskrunner.NewTaskRunner(registry, log),
		clipCache:   cache,
		canonicalAssetCache: workercache.NewCanonicalAssetStore(cache),
	}

	payload := map[string]interface{}{
		"scenes": []interface{}{
			map[string]interface{}{
				"clip": map[string]interface{}{
					"url": "velox-drive://" + assetID,
				},
			},
		},
	}

	const jobID = "job-legacy-lease-err"
	pte := &PendingTaskExecution{
		TaskID: "task-" + jobID, JobID: jobID, AttemptID: "attempt-" + jobID,
		ExecutorID: "render_batch", ExecutorVersion: 1,
		Spec: executor.TaskSpec{
			Version: 1, JobID: jobID, ExecutorID: "render_batch", Payload: payload,
		},
	}

	if _, err := w.dispatchTaskRunner(context.Background(), pte); err == nil {
		t.Fatal("dispatch executor error returned nil")
	}

	entry, found, err := cache.Find(context.Background(), assetID)
	if err != nil || !found {
		t.Fatalf("find %s after error: found=%v err=%v", assetID, found, err)
	}
	if entry.ActiveJobID != "" || entry.ActiveLeaseCount != 0 {
		t.Fatalf("asset %s lease after error = job:%q count:%d, want cleared", assetID, entry.ActiveJobID, entry.ActiveLeaseCount)
	}
}
