package worker

import (
	"syscall"

	"velox-worker-agent/internal/prefetch"
)

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

func (w *Worker) futureAssetScheduler() *prefetch.Scheduler {
	if w.prefetchScheduler == nil {
		workerID := ""
		if w.config != nil {
			workerID = w.config.WorkerID
		}
		cfg := prefetch.Config{WorkerID: workerID}
		if w.config != nil {
			cfg.MaxConcurrent = w.config.PrefetchMaxConcurrent
			cfg.ByteBudget = w.config.PrefetchByteBudget
			cfg.DiskRestrictedPercent = w.config.PrefetchDiskRestrictedPercent
			cfg.DiskCriticalPercent = w.config.PrefetchDiskCriticalPercent
			cfg.DiskRecoveryPercent = w.config.PrefetchDiskRecoveryPercent
		}
		w.prefetchScheduler = prefetch.NewScheduler(cfg)
	}
	resolver := w.assetCacheResolver()
	if ram := w.prefetchScheduler.RAMCache(); ram != nil {
		resolver.SetL1Cache(ram)
	}
	w.prefetchScheduler.SetResolver(resolver)
	w.prefetchScheduler.SetProtectionStore(w.canonicalAssetCache)
	w.prefetchScheduler.SetDiskUsagePercent(w.prefetchDiskUsagePercent)
	return w.prefetchScheduler
}

func (w *Worker) prefetchDiskUsagePercent() int {
	if w == nil {
		return 0
	}
	var stat syscall.Statfs_t
	if err := syscall.Statfs(w.assetCacheDir(), &stat); err != nil || stat.Blocks == 0 {
		return 0
	}
	used := stat.Blocks - stat.Bfree
	return int((used * 100) / stat.Blocks)
}
