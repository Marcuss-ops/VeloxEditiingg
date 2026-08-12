package worker

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"velox-worker-agent/internal/workercache"
)

const (
	leaseReconcileInterval = 5 * time.Second
	leaseReconcileBatch    = 64
	leaseReconcileInitial  = 1 * time.Second
	leaseReconcileMax      = 5 * time.Minute
)

// leaseReleaseRetryDelay returns a bounded durable backoff. AttemptCount is
// persisted in SQLite, so a worker restart cannot reset a hot failure loop.
func leaseReleaseRetryDelay(attemptCount int) time.Duration {
	if attemptCount <= 0 {
		return leaseReconcileInitial
	}
	delay := float64(leaseReconcileInitial) * math.Pow(2, float64(attemptCount))
	if delay >= float64(leaseReconcileMax) {
		return leaseReconcileMax
	}
	return time.Duration(delay)
}

// reconcileLeaseReleasesOnce drains one due batch. A missing asset row is
// treated as success: its FK cascade already removed the lease, so retaining
// the reconciliation row would create a permanent poison item.
func (w *Worker) reconcileLeaseReleasesOnce(ctx context.Context) error {
	if w == nil || w.clipCache == nil {
		return nil
	}
	entries, err := w.clipCache.ListDueLeaseReleases(ctx, time.Now().UTC(), leaseReconcileBatch)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		releaseErr := w.clipCache.Release(ctx, entry.AssetKey, entry.JobID)
		if releaseErr == nil || errors.Is(releaseErr, workercache.ErrNotFound) {
			if deleteErr := w.clipCache.DeleteLeaseRelease(leaseCleanupContext(ctx), entry.AssetKey, entry.JobID); deleteErr != nil && w.logger != nil {
				w.logger.Warn("[LEASE-RECONCILE] delete completed item failed asset=%s job=%s: %v", entry.AssetKey, entry.JobID, deleteErr)
			}
			continue
		}

		nextAttempt := time.Now().UTC().Add(leaseReleaseRetryDelay(entry.AttemptCount + 1))
		if retryErr := w.clipCache.MarkLeaseReleaseRetry(
			leaseCleanupContext(ctx), entry.AssetKey, entry.JobID, releaseErr.Error(), entry.AttemptCount+1, nextAttempt, time.Now().UTC(),
		); retryErr != nil && w.logger != nil {
			w.logger.Warn("[LEASE-RECONCILE] persist retry failed asset=%s job=%s: %v (release=%v)", entry.AssetKey, entry.JobID, retryErr, releaseErr)
		}
	}
	return nil
}

// startLeaseReconciliationLoop starts one session-scoped consumer. The queue
// itself is durable in workercache, so reconnects/restarts simply run the
// initial pass again and continue the persisted schedule.
func (w *Worker) startLeaseReconciliationLoop(ctx context.Context) {
	if w == nil || w.clipCache == nil {
		return
	}
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		if err := w.reconcileLeaseReleasesOnce(ctx); err != nil && w.logger != nil {
			w.logger.Warn("[LEASE-RECONCILE] initial pass failed: %v", err)
		}
		ticker := time.NewTicker(leaseReconcileInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-w.stopChan:
				return
			case <-ticker.C:
				if err := w.reconcileLeaseReleasesOnce(ctx); err != nil && w.logger != nil {
					w.logger.Warn("[LEASE-RECONCILE] pass failed: %v", err)
				}
			}
		}
	}()
}

// ReconcileLeaseReleasesOnce is an explicit operational/test hook for a
// deterministic pass without starting the session loop.
func (w *Worker) ReconcileLeaseReleasesOnce(ctx context.Context) error {
	if w == nil {
		return fmt.Errorf("worker.ReconcileLeaseReleasesOnce: nil worker")
	}
	return w.reconcileLeaseReleasesOnce(ctx)
}
