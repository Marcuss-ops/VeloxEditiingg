package assembly

import (
	"sync"
	"testing"
	"time"
)

func TestPromotePreparationToExecutionFallsBackWhenPreparedWorkerIsOccupied(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	preparation, err := NewPreparationLease("prep-occupied", "job-occupied", "worker-warm", "sha256:prep", now, 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	preparation, err = preparation.Transition(PreparationPrefetching, now)
	if err != nil {
		t.Fatal(err)
	}
	preparation, err = preparation.Transition(PreparationPrepared, now)
	if err != nil {
		t.Fatal(err)
	}

	lease, decision, err := PromotePreparationToExecution(
		preparation,
		"exec-occupied",
		now,
		time.Minute,
		[]WorkerPlacementSnapshot{
			{
				WorkerID: "worker-warm", Available: true, CapacityAuthoritative: true,
				ActiveExecutionSlots: 1, MaxExecutionSlots: 1,
				CachedSHA256: []string{"clip"},
			},
			{
				WorkerID: "worker-cold", Available: true, CapacityAuthoritative: true,
				ActiveExecutionSlots: 0, MaxExecutionSlots: 1,
				CachedSHA256: nil,
			},
		},
		PlacementRequest{AssetSHA256: []string{"clip"}},
	)
	if err != nil {
		t.Fatalf("PromotePreparationToExecution() error = %v", err)
	}
	if decision.WorkerID != "worker-cold" || lease.WorkerID != "worker-cold" {
		t.Fatalf("occupied-worker fallback decision=%#v lease=%#v", decision, lease)
	}
	if lease.FallbackFromWorkerID != "worker-warm" {
		t.Fatalf("fallback_from_worker_id=%q, want worker-warm", lease.FallbackFromWorkerID)
	}
	if preparation.WorkerID != "worker-warm" {
		t.Fatalf("preparation worker identity changed to %q", preparation.WorkerID)
	}
}

func TestPromotePreparationToExecutionIsSafeForConcurrentCallers(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	preparation, err := NewPreparationLease("prep-concurrent", "job-concurrent", "worker-a", "sha256:prep", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	preparation, err = preparation.Transition(PreparationPrefetching, now)
	if err != nil {
		t.Fatal(err)
	}
	preparation, err = preparation.Transition(PreparationPrepared, now)
	if err != nil {
		t.Fatal(err)
	}
	workers := []WorkerPlacementSnapshot{
		{WorkerID: "worker-a", Available: false, CapacityAuthoritative: true, MaxExecutionSlots: 1},
		{WorkerID: "worker-b", Available: true, CapacityAuthoritative: true, MaxExecutionSlots: 1},
	}

	const callers = 32
	leases := make(chan ExecutionLease, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lease, _, err := PromotePreparationToExecution(preparation, "exec-concurrent", now, time.Minute, workers, PlacementRequest{})
			if err != nil {
				errs <- err
				return
			}
			leases <- lease
		}()
	}
	wg.Wait()
	close(leases)
	close(errs)

	if len(errs) != 0 {
		for err := range errs {
			t.Errorf("concurrent promotion error: %v", err)
		}
	}
	count := 0
	for lease := range leases {
		count++
		if lease.WorkerID != "worker-b" || lease.FallbackFromWorkerID != "worker-a" {
			t.Errorf("concurrent promotion lease=%#v", lease)
		}
	}
	if count != callers {
		t.Fatalf("successful promotions=%d, want %d", count, callers)
	}
}
