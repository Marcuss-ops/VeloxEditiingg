package worker

import (
	"velox-worker-agent/internal/workercache"
	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/storage"
)

// worker_accessors.go owns the small read-only seam methods that expose the
// Worker's wired dependencies (API client, notification channels, storage
// resolver) and the composition-root-only wiring/terminal-signal helpers.
// worker_init.go owns construction; these methods are the access surface.

// APIClient returns the authenticated data-plane client used by the worker.
// The registration handshake populates its bearer token; long-lived remote
// services such as the protected-assets poller must therefore share this
// exact client rather than constructing an unauthenticated second client.
func (w *Worker) APIClient() *api.Client {
	if w == nil {
		return nil
	}
	return w.apiClient
}

// JobDone returns a notification channel which receives a best-effort signal
// after the worker receives terminal confirmation for a task result. The
// channel is intentionally buffered/coalescing: cleanup needs a prompt
// wake-up, not one event per DAG subtask.
func (w *Worker) JobDone() <-chan struct{} {
	if w == nil {
		return nil
	}
	return w.jobDone
}

// StorageResolver returns the canonical Fase E1 storage resolver (cache /
// temp / artifact placement with the tmpfs threshold gate). Non-nil for
// every Worker returned by New(). Later phases (E2 executor placement, F
// artifact staging) consume storage through this seam instead of
// scattering os.TempDir() calls.
func (w *Worker) StorageResolver() *storage.Resolver {
	if w == nil {
		return nil
	}
	return w.storageResolver
}

// AssetCacheDiskUsagePercent reports the disk-usage percentage of the
// filesystem backing the worker's asset cache (the same directory that holds
// the content-addressed blobs). It is the composition root's pressure signal
// for the workercache.CleanupLoop: a nil-safe statfs probe that returns 0 on
// any error so a transient statfs failure never fabricates pressure.
func (w *Worker) AssetCacheDiskUsagePercent() int {
	return w.prefetchDiskUsagePercent()
}

// AttachClipCache is used only by the composition root while the worker is
// still stopped. It keeps construction of the durable SQLite index next to
// the process lifecycle while preserving the existing Option API for tests.
func (w *Worker) AttachClipCache(c *workercache.Cache) {
	if w == nil || c == nil {
		panic("worker.AttachClipCache: cache is required")
	}
	w.clipCache = c
	w.canonicalAssetCache = workercache.NewCanonicalAssetStore(c)
	if w.prefetchScheduler != nil {
		w.prefetchScheduler.SetProtectionStore(w.canonicalAssetCache)
	}
}

// signalTaskTerminal wakes the cache cleanup loop only after the master has
// acknowledged a terminal TaskResult. Render completion, artifact upload,
// and local task teardown must not signal cleanup on their own.
func (w *Worker) signalTaskTerminal() {
	if w == nil || w.jobDone == nil {
		return
	}
	select {
	case w.jobDone <- struct{}{}:
	default:
	}
}
