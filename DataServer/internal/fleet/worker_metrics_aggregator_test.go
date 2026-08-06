package fleet

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"velox-server/internal/store"
)

type snapshotAggregatorFake struct {
	workerIDs []string

	mu          sync.Mutex
	computes    []string
	persisted   []string
	active      int
	maxActive   int
	computeErrs map[string]error
	computeWait time.Duration
}

func (f *snapshotAggregatorFake) WorkerIDs(context.Context) ([]string, error) {
	return append([]string(nil), f.workerIDs...), nil
}

func (f *snapshotAggregatorFake) ComputeWorkerMetricsSnapshot(ctx context.Context, workerID string, now time.Time) (store.WorkerMetricsSnapshot, error) {
	f.mu.Lock()
	f.active++
	if f.active > f.maxActive {
		f.maxActive = f.active
	}
	f.computes = append(f.computes, workerID)
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.active--
		f.mu.Unlock()
	}()

	if f.computeWait > 0 {
		timer := time.NewTimer(f.computeWait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return store.WorkerMetricsSnapshot{}, ctx.Err()
		}
	}
	if err := f.computeErrs[workerID]; err != nil {
		return store.WorkerMetricsSnapshot{}, err
	}
	return store.WorkerMetricsSnapshot{WorkerID: workerID, SnapshottedAt: now}, nil
}

func (f *snapshotAggregatorFake) InsertWorkerMetricsSnapshot(_ context.Context, snapshot store.WorkerMetricsSnapshot) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.persisted = append(f.persisted, snapshot.WorkerID)
	return nil
}

func TestComputeAndPersistSnapshot_100WorkersUsesBatchAndFourConcurrency(t *testing.T) {
	workerIDs := make([]string, 100)
	for i := range workerIDs {
		workerIDs[i] = fmt.Sprintf("worker-%03d", i)
	}
	fake := &snapshotAggregatorFake{workerIDs: workerIDs, computeErrs: map[string]error{}, computeWait: 5 * time.Millisecond}

	persisted, err := ComputeAndPersistSnapshot(context.Background(), fake, time.Now().UTC())
	if err != nil {
		t.Fatalf("ComputeAndPersistSnapshot: %v", err)
	}
	if persisted != 100 {
		t.Fatalf("persisted=%d, want 100", persisted)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.computes) != 100 {
		t.Fatalf("compute calls=%d, want 100", len(fake.computes))
	}
	if len(fake.persisted) != 100 {
		t.Fatalf("persist calls=%d, want 100", len(fake.persisted))
	}
	if fake.maxActive != WorkerMetricsSnapshotConcurrency {
		t.Fatalf("max concurrent computes=%d, want %d", fake.maxActive, WorkerMetricsSnapshotConcurrency)
	}
}

func TestComputeAndPersistSnapshot_IsolatesWorkerErrors(t *testing.T) {
	wantErr := errors.New("provider unavailable")
	fake := &snapshotAggregatorFake{
		workerIDs:   []string{"worker-a", "worker-b", "worker-c"},
		computeErrs: map[string]error{"worker-b": wantErr},
	}

	persisted, err := ComputeAndPersistSnapshot(context.Background(), fake, time.Now().UTC())
	if persisted != 2 {
		t.Fatalf("persisted=%d, want 2", persisted)
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("error=%v, want wrapped provider error", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.persisted) != 2 {
		t.Fatalf("persist calls=%d, want 2", len(fake.persisted))
	}
}

func TestComputeAndPersistSnapshot_StopsAtTickBudget(t *testing.T) {
	workerIDs := make([]string, 50)
	for i := range workerIDs {
		workerIDs[i] = fmt.Sprintf("worker-%03d", i)
	}
	fake := &snapshotAggregatorFake{workerIDs: workerIDs, computeErrs: map[string]error{}, computeWait: 25 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()

	persisted, err := ComputeAndPersistSnapshot(ctx, fake, time.Now().UTC())
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v, want context deadline", err)
	}
	if persisted >= len(workerIDs) {
		t.Fatalf("persisted=%d, budget should stop the tick before all %d workers", persisted, len(workerIDs))
	}
}

func TestWorkerSnapshotBatches_25(t *testing.T) {
	ids := make([]string, 100)
	batches := workerSnapshotBatches(ids, WorkerMetricsSnapshotBatchLimit)
	if len(batches) != 4 {
		t.Fatalf("batches=%d, want 4", len(batches))
	}
	for i, batch := range batches {
		if len(batch) != 25 {
			t.Fatalf("batch %d size=%d, want 25", i, len(batch))
		}
	}
}
