// Package worker — tests for storePendingTask / takePendingTask helpers
// using the typed PendingTaskExecution struct.
package worker

import (
	"sync"
	"testing"

	"velox-worker-agent/internal/executor"
)

// TestPendingTasks_StoreTakeRoundTrip exercises the happy-path
// store-then-take sequence using PendingTaskExecution.
func TestPendingTasks_StoreTakeRoundTrip(t *testing.T) {
	w := &Worker{
		pendingTasks: make(map[string]*PendingTaskExecution),
	}
	pte := &PendingTaskExecution{
		TaskID:        "task-rt-1",
		JobID:         "job-rt-1",
		AttemptID:     "attempt-rt-1",
		AttemptNumber: 1,
		LeaseID:       "lease-rt-1",
		ExecutorID:    "scene.composite.v1",
		Spec: executor.TaskSpec{
			Version:    1,
			JobID:      "job-rt-1",
			ExecutorID: "scene.composite.v1",
		},
	}
	w.storePendingTask("task-rt-1", pte)

	got := w.takePendingTask("task-rt-1")
	if got == nil {
		t.Fatal("takePendingTask returned nil for stored task")
	}
	if got != pte {
		t.Errorf("takePendingTask returned a different pointer than storePendingTask received")
	}
	if got.JobID != "job-rt-1" || got.LeaseID != "lease-rt-1" || got.AttemptNumber != 1 {
		t.Errorf("identity fields lost in store/take: %+v", got)
	}
	if got.TaskID != "task-rt-1" {
		t.Errorf("TaskID lost: got %q", got.TaskID)
	}

	// After take, the map slot must be empty so the next take returns nil.
	if again := w.takePendingTask("task-rt-1"); again != nil {
		t.Errorf("expected nil after first take, got %+v", again)
	}
}

// TestPendingTasks_TakeUnknownReturnsNil ensures the safety gate works.
func TestPendingTasks_TakeUnknownReturnsNil(t *testing.T) {
	w := &Worker{
		pendingTasks: make(map[string]*PendingTaskExecution),
	}
	if got := w.takePendingTask("task-unknown"); got != nil {
		t.Errorf("expected nil for unknown task_id, got %+v", got)
	}
}

// TestPendingTasks_OverwritePreservesLatest ensures storePendingTask does NOT error
// on duplicate task_id — the most recent PendingTaskExecution wins.
func TestPendingTasks_OverwritePreservesLatest(t *testing.T) {
	w := &Worker{
		pendingTasks: make(map[string]*PendingTaskExecution),
	}
	first := &PendingTaskExecution{JobID: "job-1", ExecutorVersion: 1}
	second := &PendingTaskExecution{JobID: "job-2", ExecutorVersion: 2}
	w.storePendingTask("task-overwrite", first)
	w.storePendingTask("task-overwrite", second)
	got := w.takePendingTask("task-overwrite")
	if got == nil {
		t.Fatal("expected non-nil result after overwrite")
	}
	if got.JobID != "job-2" {
		t.Errorf("expected latest pointer (job-2), got %q", got.JobID)
	}
	if got.ExecutorVersion != 2 {
		t.Errorf("expected ExecutorVersion=2 on latest, got %d", got.ExecutorVersion)
	}
}

// TestPendingTasks_ConcurrentStoreAndTake validates the pendingTasksMu mutex.
func TestPendingTasks_ConcurrentStoreAndTake(t *testing.T) {
	w := &Worker{
		pendingTasks: make(map[string]*PendingTaskExecution),
	}
	const N = 200
	pte := &PendingTaskExecution{
		JobID:         "job-conc",
		LeaseID:       "lease-conc",
		TaskID:        "task-conc",
		AttemptID:     "attempt-conc",
		AttemptNumber: 1,
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < N; i++ {
			w.storePendingTask("task-conc", pte)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < N; i++ {
			_ = w.takePendingTask("task-conc")
		}
	}()
	wg.Wait()

	if got := w.takePendingTask("task-conc"); got != nil {
		if got != pte {
			t.Errorf("expected same pointer when a final store wins, got %+v", got)
		}
		if again := w.takePendingTask("task-conc"); again != nil {
			t.Errorf("expected empty map after draining leftover entry, got %+v", again)
		}
	}
}
