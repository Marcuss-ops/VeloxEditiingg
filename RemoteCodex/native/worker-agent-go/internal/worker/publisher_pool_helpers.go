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
