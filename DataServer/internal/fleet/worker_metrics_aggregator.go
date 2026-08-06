package fleet

// worker_metrics_aggregator.go coordinates the fleet metrics snapshot job.
// SQL ownership belongs to internal/store; this package only coordinates the
// typed repository contract and persists the resulting snapshot.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"velox-server/internal/store"
)

const (
	// WorkerMetricsSnapshotBatchLimit bounds one scheduler batch. Keeping the
	// batch finite prevents a large fleet from holding all snapshot work in
	// memory while still giving the provider pool enough work to stay busy.
	WorkerMetricsSnapshotBatchLimit = 25
	// WorkerMetricsSnapshotConcurrency bounds provider/aggregation calls. The
	// SQLite insert phase remains serialized after the concurrent computations.
	WorkerMetricsSnapshotConcurrency = 4
	// WorkerMetricsSnapshotTickBudget prevents one slow provider from blocking
	// the next scheduled snapshot tick indefinitely. Callers should derive a
	// child context with this budget for each tick.
	WorkerMetricsSnapshotTickBudget = 45 * time.Second
)

// AggregatorDataSource is the narrow consumer-side interface used by the
// scheduler. Production wires a typed store adapter; tests inject a fake.
type AggregatorDataSource interface {
	WorkerIDs(ctx context.Context) ([]string, error)
	ComputeWorkerMetricsSnapshot(ctx context.Context, workerID string, now time.Time) (store.WorkerMetricsSnapshot, error)
	InsertWorkerMetricsSnapshot(ctx context.Context, snap store.WorkerMetricsSnapshot) error
}

// ComputeAndPersistSnapshot computes and persists one typed snapshot per
// known worker. Query semantics and source-table access live in internal/store.
//
// Work is scheduled in batches of WorkerMetricsSnapshotBatchLimit. Within a
// batch, at most WorkerMetricsSnapshotConcurrency provider computations run at
// once; inserts remain sequential because SQLite is the authoritative writer.
// A worker-level failure is isolated so the remaining workers in the tick can
// still refresh. The returned error is an aggregate of all failures observed.
func ComputeAndPersistSnapshot(ctx context.Context, ds AggregatorDataSource, now time.Time) (int, error) {
	if ds == nil {
		return 0, fmt.Errorf("aggregator: data source is nil")
	}
	workerIDs, err := ds.WorkerIDs(ctx)
	if err != nil {
		return 0, fmt.Errorf("aggregator: listing workers: %w", err)
	}

	persisted := 0
	var errs []error
	for _, batch := range workerSnapshotBatches(workerIDs, WorkerMetricsSnapshotBatchLimit) {
		if err := ctx.Err(); err != nil {
			errs = append(errs, fmt.Errorf("aggregator: snapshot budget: %w", err))
			break
		}
		results := computeWorkerSnapshotBatch(ctx, ds, batch, now, WorkerMetricsSnapshotConcurrency)
		for _, result := range results {
			if result.err != nil {
				errs = append(errs, result.err)
				continue
			}
			if err := ds.InsertWorkerMetricsSnapshot(ctx, result.snapshot); err != nil {
				errs = append(errs, fmt.Errorf("aggregator: insert worker=%s: %w", result.workerID, err))
				continue
			}
			persisted++
		}
	}
	return persisted, errors.Join(errs...)
}

type workerSnapshotResult struct {
	workerID string
	snapshot store.WorkerMetricsSnapshot
	err      error
}

func computeWorkerSnapshotBatch(ctx context.Context, ds AggregatorDataSource, workerIDs []string, now time.Time, concurrency int) []workerSnapshotResult {
	if concurrency <= 0 {
		concurrency = 1
	}
	if concurrency > len(workerIDs) {
		concurrency = len(workerIDs)
	}
	results := make([]workerSnapshotResult, len(workerIDs))
	if len(workerIDs) == 0 {
		return results
	}

	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for i, workerID := range workerIDs {
		wg.Add(1)
		go func(i int, workerID string) {
			defer wg.Done()
			result := workerSnapshotResult{workerID: workerID}
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				result.err = fmt.Errorf("aggregator: worker=%s: %w", workerID, ctx.Err())
				results[i] = result
				return
			}
			defer func() { <-sem }()

			snapshot, err := ds.ComputeWorkerMetricsSnapshot(ctx, workerID, now)
			if err != nil {
				result.err = fmt.Errorf("aggregator: worker=%s: %w", workerID, err)
			} else {
				result.snapshot = snapshot
			}
			results[i] = result
		}(i, workerID)
	}
	wg.Wait()
	return results
}

func workerSnapshotBatches(workerIDs []string, batchLimit int) [][]string {
	if batchLimit <= 0 {
		batchLimit = WorkerMetricsSnapshotBatchLimit
	}
	batches := make([][]string, 0, (len(workerIDs)+batchLimit-1)/batchLimit)
	for start := 0; start < len(workerIDs); start += batchLimit {
		end := start + batchLimit
		if end > len(workerIDs) {
			end = len(workerIDs)
		}
		batches = append(batches, workerIDs[start:end])
	}
	return batches
}

// WorkerMetricsAggregatorDataSource is the production binding used by the
// bootstrap. The store interface intentionally exposes only typed operations.
type WorkerMetricsAggregatorDataSource struct {
	Store interface {
		WorkerIDs(ctx context.Context) ([]string, error)
		ComputeWorkerMetricsSnapshot(ctx context.Context, workerID string, now time.Time) (store.WorkerMetricsSnapshot, error)
		InsertWorkerMetricsSnapshot(ctx context.Context, snap store.WorkerMetricsSnapshot) error
		ListLatestWorkerMetrics(ctx context.Context, limit int) ([]store.WorkerMetricsSnapshot, error)
		GetLatestWorkerMetricsForWorker(ctx context.Context, workerID string) (store.WorkerMetricsSnapshot, error)
	}
}

func (s WorkerMetricsAggregatorDataSource) WorkerIDs(ctx context.Context) ([]string, error) {
	if s.Store == nil {
		return nil, fmt.Errorf("aggregator: store is nil")
	}
	return s.Store.WorkerIDs(ctx)
}

func (s WorkerMetricsAggregatorDataSource) ComputeWorkerMetricsSnapshot(ctx context.Context, workerID string, now time.Time) (store.WorkerMetricsSnapshot, error) {
	if s.Store == nil {
		return store.WorkerMetricsSnapshot{}, fmt.Errorf("aggregator: store is nil")
	}
	return s.Store.ComputeWorkerMetricsSnapshot(ctx, workerID, now)
}

func (s WorkerMetricsAggregatorDataSource) InsertWorkerMetricsSnapshot(ctx context.Context, snap store.WorkerMetricsSnapshot) error {
	if s.Store == nil {
		return fmt.Errorf("aggregator: store is nil")
	}
	return s.Store.InsertWorkerMetricsSnapshot(ctx, snap)
}
