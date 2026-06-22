// Package worker — tests for storePendingTask / takePendingTask helpers
// added by PR-2 (fix/canonical-attempt-identity). The map is keyed by
// task_id and protected by pendingTasksMu so that the MsgTaskOffer
// store-side and MsgTaskLeaseGranted take-side can run in any order
// without a race.
package worker

import (
	"sync"
	"testing"

	"velox-worker-agent/pkg/api"
)

// TestPendingTasks_StoreTakeRoundTrip exercises the happy-path
// store-then-take sequence: the same *api.Job handed to
// storePendingTask must come back from takePendingTask with the
// exact same pointer + identity fields preserved.
func TestPendingTasks_StoreTakeRoundTrip(t *testing.T) {
	w := &Worker{
		pendingTasks: make(map[string]*api.Job),
	}
	job := &api.Job{
		JobID:   "job-rt-1",
		LeaseID: "lease-rt-1",
		Attempt: 1,
		Parameters: map[string]interface{}{
			"_task_id":    "task-rt-1",
			"_attempt_id": "attempt-rt-1",
		},
		JobType: "scene.composite.v1",
	}
	w.storePendingTask("task-rt-1", job)

	got := w.takePendingTask("task-rt-1")
	if got == nil {
		t.Fatal("takePendingTask returned nil for stored task")
	}
	if got != job {
		t.Errorf("takePendingTask returned a different pointer than storePendingTask received")
	}
	if got.JobID != "job-rt-1" || got.LeaseID != "lease-rt-1" || got.Attempt != 1 {
		t.Errorf("identity fields lost in store/take: %+v", got)
	}
	if v, _ := got.Parameters["_task_id"].(string); v != "task-rt-1" {
		t.Errorf("_task_id lost: got %q", v)
	}
	if v, _ := got.Parameters["_attempt_id"].(string); v != "attempt-rt-1" {
		t.Errorf("_attempt_id lost: got %q", v)
	}

	// After take, the map slot must be empty so the next take returns nil.
	if again := w.takePendingTask("task-rt-1"); again != nil {
		t.Errorf("expected nil after first take, got %+v", again)
	}
}

// TestPendingTasks_TakeUnknownReturnsNil ensures the safety gate
// that MsgTaskLeaseGranted relies on: an unknown task_id must return
// nil so the handler can log + drop (no panic, no nil-deref later).
func TestPendingTasks_TakeUnknownReturnsNil(t *testing.T) {
	w := &Worker{
		pendingTasks: make(map[string]*api.Job),
	}
	if got := w.takePendingTask("task-unknown"); got != nil {
		t.Errorf("expected nil for unknown task_id, got %+v", got)
	}
}

// TestPendingTasks_OverwritePreservesLatest ensures that if the
// master ever resends a TaskOffer (idempotent retry, for example)
// the most recent *api.Job wins — storePendingTask does NOT error
// on duplicate task_id. This is a property the live code needs so
// a retry pipeline does not silently leak the prior version.
func TestPendingTasks_OverwritePreservesLatest(t *testing.T) {
	w := &Worker{
		pendingTasks: make(map[string]*api.Job),
	}
	first := &api.Job{JobID: "job-1", Parameters: map[string]interface{}{"task_spec_version": 1}}
	second := &api.Job{JobID: "job-2", Parameters: map[string]interface{}{"task_spec_version": 2}}
	w.storePendingTask("task-overwrite", first)
	w.storePendingTask("task-overwrite", second)
	got := w.takePendingTask("task-overwrite")
	if got == nil {
		t.Fatal("expected non-nil result after overwrite")
	}
	if got.JobID != "job-2" {
		t.Errorf("expected latest job pointer (job-2), got %q", got.JobID)
	}
	if v, _ := got.Parameters["task_spec_version"].(int); v != 2 {
		t.Errorf("expected task_spec_version=2 on latest, got %v", got.Parameters["task_spec_version"])
	}
}

// TestPendingTasks_ConcurrentStoreAndTake validates that the
// pendingTasksMu mutex serializes concurrent store/take operations
// without a Go race-detector panic. Reproduces the receive-loop
// race: MsgTaskOffer storing while MsgTaskLeaseGranted (or a
// future sharded dispatcher) might be taking.
func TestPendingTasks_ConcurrentStoreAndTake(t *testing.T) {
	w := &Worker{
		pendingTasks: make(map[string]*api.Job),
	}
	const N = 200
	job := &api.Job{
		JobID:   "job-conc",
		LeaseID: "lease-conc",
		Parameters: map[string]interface{}{
			"_task_id": "task-conc",
		},
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < N; i++ {
			w.storePendingTask("task-conc", job)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < N; i++ {
			_ = w.takePendingTask("task-conc")
		}
	}()
	wg.Wait()

	// Final state: unknown task -> nil. Even if a stale take happened
	// to return the job pointer in flight, the post-test invariant
	// holds because take deletes the entry.
	if got := w.takePendingTask("task-conc"); got != nil {
		t.Errorf("expected nil after paired store/take storms, got %+v", got)
	}
}
