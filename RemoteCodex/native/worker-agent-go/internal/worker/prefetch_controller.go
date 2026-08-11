package worker

import "velox-worker-agent/internal/prefetch"

// futureAssetController returns the worker's snapshot reconciler. Tests that
// construct a Worker literal without New() still get the same fail-closed,
// no-I/O controller on first use.
func (w *Worker) futureAssetController() *prefetch.Controller {
	if w.prefetchController == nil {
		workerID := ""
		if w.config != nil {
			workerID = w.config.WorkerID
		}
		w.prefetchController = prefetch.NewController(workerID)
	}
	return w.prefetchController
}
