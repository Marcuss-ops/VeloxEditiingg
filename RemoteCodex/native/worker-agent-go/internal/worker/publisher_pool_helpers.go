package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"velox-worker-agent/internal/telemetry"
)

func (w *Worker) acquireResumePublisher(ctx context.Context, key string) (func(), error) {
	return w.acquireResumePublishers(ctx, []string{key})
}

// tryAcquireResumePublishers is deliberately non-blocking. Resume is
// opportunistic: foreground publication owns the artifact until it releases
// its publisher slot and artifact lock, after which the next tick retries.
func (w *Worker) tryAcquireResumePublishers(keys []string) (func(), bool) {
	if w == nil || w.publisherPool == nil || w.artifactLocks == nil {
		return nil, false
	}
	if !w.publisherPool.TryAcquire() {
		return nil, false
	}
	releaseLocks, ok := w.artifactLocks.TryAcquireMany(keys)
	if !ok {
		w.publisherPool.Release()
		return nil, false
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			releaseLocks()
			w.publisherPool.Release()
		})
	}, true
}

func (w *Worker) acquireResumePublishers(ctx context.Context, keys []string) (func(), error) {
	if w == nil || w.publisherPool == nil {
		return nil, fmt.Errorf("publisher pool is not configured")
	}
	if w.artifactLocks == nil {
		return nil, fmt.Errorf("artifact lock registry is not configured")
	}
	poolWaitStarted := time.Now()
	if err := w.publisherPool.Acquire(ctx); err != nil {
		return nil, err
	}
	telemetry.GetPrometheusMetrics().RecordArtifactLockWait(time.Since(poolWaitStarted))
	lockWaitStarted := time.Now()
	release, err := w.artifactLocks.AcquireMany(ctx, keys)
	telemetry.GetPrometheusMetrics().RecordArtifactLockWait(time.Since(lockWaitStarted))
	if err != nil {
		w.publisherPool.Release()
		return nil, err
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			release()
			w.publisherPool.Release()
		})
	}, nil
}
