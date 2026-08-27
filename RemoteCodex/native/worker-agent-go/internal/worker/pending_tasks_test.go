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
	decision := w.storePendingTask("task-rt-1", pte)
	if decision != OfferInserted {
		t.Fatalf("expected OfferInserted for first store, got %d", decision)
	}

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

// TestPendingTasks_DuplicateOfferReturnsDuplicate ensures a TaskOffer with
// identical identity fields (attempt_id, attempt_number, lease_id, revision)
// is classified as OfferDuplicate and the existing entry is preserved.
func TestPendingTasks_DuplicateOfferReturnsDuplicate(t *testing.T) {
	w := &Worker{
		pendingTasks: make(map[string]*PendingTaskExecution),
	}
	first := &PendingTaskExecution{
		TaskID:          "task-dup",
		JobID:           "job-1",
		AttemptID:       "attempt-1",
		AttemptNumber:   1,
		LeaseID:         "lease-1",
		Revision:        1,
		ExecutorID:      "scene.composite.v1",
		ExecutorVersion: 1,
	}
	duplicate := &PendingTaskExecution{
		TaskID:          "task-dup",
		JobID:           "job-1",
		AttemptID:       "attempt-1",
		AttemptNumber:   1,
		LeaseID:         "lease-1",
		Revision:        1,
		ExecutorID:      "scene.composite.v1",
		ExecutorVersion: 2, // Different non-identity field — still a duplicate.
	}

	w.storePendingTask("task-dup", first)
	decision := w.storePendingTask("task-dup", duplicate)
	if decision != OfferDuplicate {
		t.Fatalf("expected OfferDuplicate, got %d", decision)
	}

	// The existing entry must be preserved (first, not duplicate).
	got := w.takePendingTask("task-dup")
	if got == nil {
		t.Fatal("expected non-nil after duplicate")
	}
	if got != first {
		t.Errorf("expected original pointer preserved, got different pointer")
	}
	if got.ExecutorVersion != 1 {
		t.Errorf("expected original ExecutorVersion=1, got %d", got.ExecutorVersion)
	}
}

// TestPendingTasks_StaleOfferReturnsStale ensures a TaskOffer with a lower
// attempt_number is rejected as OfferStale without modifying the map.
func TestPendingTasks_StaleOfferReturnsStale(t *testing.T) {
	w := &Worker{
		pendingTasks: make(map[string]*PendingTaskExecution),
	}
	current := &PendingTaskExecution{
		TaskID:        "task-stale",
		JobID:         "job-1",
		AttemptID:     "attempt-2",
		AttemptNumber: 2,
		LeaseID:       "lease-2",
		Revision:      1,
	}
	stale := &PendingTaskExecution{
		TaskID:        "task-stale",
		JobID:         "job-1",
		AttemptID:     "attempt-1",
		AttemptNumber: 1,
		LeaseID:       "lease-1",
		Revision:      1,
	}

	w.storePendingTask("task-stale", current)
	decision := w.storePendingTask("task-stale", stale)
	if decision != OfferStale {
		t.Fatalf("expected OfferStale, got %d", decision)
	}

	// The existing entry must be preserved (current, not stale).
	got := w.takePendingTask("task-stale")
	if got == nil {
		t.Fatal("expected non-nil after stale offer")
	}
	if got != current {
		t.Errorf("expected original pointer preserved, got different pointer")
	}
	if got.AttemptNumber != 2 {
		t.Errorf("expected original AttemptNumber=2, got %d", got.AttemptNumber)
	}
}

// TestPendingTasks_ReplacedOfferReturnsReplaced ensures a TaskOffer with a
// higher attempt_number replaces the existing entry.
func TestPendingTasks_ReplacedOfferReturnsReplaced(t *testing.T) {
	w := &Worker{
		pendingTasks: make(map[string]*PendingTaskExecution),
	}
	old := &PendingTaskExecution{
		TaskID:        "task-replace",
		JobID:         "job-1",
		AttemptID:     "attempt-1",
		AttemptNumber: 1,
		LeaseID:       "lease-1",
		Revision:      1,
	}
	new := &PendingTaskExecution{
		TaskID:        "task-replace",
		JobID:         "job-1",
		AttemptID:     "attempt-2",
		AttemptNumber: 2,
		LeaseID:       "lease-2",
		Revision:      2,
	}

	w.storePendingTask("task-replace", old)
	decision := w.storePendingTask("task-replace", new)
	if decision != OfferReplaced {
		t.Fatalf("expected OfferReplaced, got %d", decision)
	}

	// The new entry must replace the old.
	got := w.takePendingTask("task-replace")
	if got == nil {
		t.Fatal("expected non-nil after replaced offer")
	}
	if got != new {
		t.Errorf("expected new pointer, got different pointer")
	}
	if got.AttemptNumber != 2 {
		t.Errorf("expected new AttemptNumber=2, got %d", got.AttemptNumber)
	}
}

// TestPendingTasks_IdentityConflictReturnsConflict ensures a TaskOffer with
// the same attempt_number but mismatched lease_id or revision is classified
// as OfferIdentityConflict.
func TestPendingTasks_IdentityConflictReturnsConflict(t *testing.T) {
	w := &Worker{
		pendingTasks: make(map[string]*PendingTaskExecution),
	}
	existing := &PendingTaskExecution{
		TaskID:        "task-conflict",
		JobID:         "job-1",
		AttemptID:     "attempt-1",
		AttemptNumber: 1,
		LeaseID:       "lease-1",
		Revision:      1,
	}
	// Same attempt_number, different lease_id.
	conflicting := &PendingTaskExecution{
		TaskID:        "task-conflict",
		JobID:         "job-1",
		AttemptID:     "attempt-1",
		AttemptNumber: 1,
		LeaseID:       "lease-X", // Different lease
		Revision:      1,
	}

	w.storePendingTask("task-conflict", existing)
	decision := w.storePendingTask("task-conflict", conflicting)
	if decision != OfferIdentityConflict {
		t.Fatalf("expected OfferIdentityConflict, got %d", decision)
	}

	// The existing entry must be preserved.
	got := w.takePendingTask("task-conflict")
	if got == nil {
		t.Fatal("expected non-nil after identity conflict")
	}
	if got != existing {
		t.Errorf("expected original pointer preserved, got different pointer")
	}
	if got.LeaseID != "lease-1" {
		t.Errorf("expected original LeaseID=lease-1, got %q", got.LeaseID)
	}
}

// TestPendingTasks_IdentityConflictDifferentRevision ensures a TaskOffer with
// the same attempt_number but mismatched revision is also an identity conflict.
func TestPendingTasks_IdentityConflictDifferentRevision(t *testing.T) {
	w := &Worker{
		pendingTasks: make(map[string]*PendingTaskExecution),
	}
	existing := &PendingTaskExecution{
		TaskID:        "task-conflict-rev",
		JobID:         "job-1",
		AttemptID:     "attempt-1",
		AttemptNumber: 1,
		LeaseID:       "lease-1",
		Revision:      1,
	}
	// Same attempt_number and lease_id, different revision.
	conflicting := &PendingTaskExecution{
		TaskID:        "task-conflict-rev",
		JobID:         "job-1",
		AttemptID:     "attempt-1",
		AttemptNumber: 1,
		LeaseID:       "lease-1",
		Revision:      99, // Different revision
	}

	w.storePendingTask("task-conflict-rev", existing)
	decision := w.storePendingTask("task-conflict-rev", conflicting)
	if decision != OfferIdentityConflict {
		t.Fatalf("expected OfferIdentityConflict, got %d", decision)
	}
}

// TestComparePendingOffer testifies the comparison function directly.
func TestComparePendingOffer(t *testing.T) {
	tests := []struct {
		name     string
		existing *PendingTaskExecution
		incoming *PendingTaskExecution
		want     PendingOfferDecision
	}{
		{
			name:     "identical identity → Duplicate",
			existing: &PendingTaskExecution{AttemptID: "a1", AttemptNumber: 1, LeaseID: "l1", Revision: 1},
			incoming: &PendingTaskExecution{AttemptID: "a1", AttemptNumber: 1, LeaseID: "l1", Revision: 1},
			want:     OfferDuplicate,
		},
		{
			name:     "higher attempt_number → Replaced",
			existing: &PendingTaskExecution{AttemptID: "a1", AttemptNumber: 1, LeaseID: "l1", Revision: 1},
			incoming: &PendingTaskExecution{AttemptID: "a2", AttemptNumber: 2, LeaseID: "l2", Revision: 2},
			want:     OfferReplaced,
		},
		{
			name:     "lower attempt_number → Stale",
			existing: &PendingTaskExecution{AttemptID: "a2", AttemptNumber: 2, LeaseID: "l2", Revision: 2},
			incoming: &PendingTaskExecution{AttemptID: "a1", AttemptNumber: 1, LeaseID: "l1", Revision: 1},
			want:     OfferStale,
		},
		{
			name:     "same attempt, different lease → IdentityConflict",
			existing: &PendingTaskExecution{AttemptID: "a1", AttemptNumber: 1, LeaseID: "l1", Revision: 1},
			incoming: &PendingTaskExecution{AttemptID: "a1", AttemptNumber: 1, LeaseID: "lX", Revision: 1},
			want:     OfferIdentityConflict,
		},
		{
			name:     "same attempt, different revision → IdentityConflict",
			existing: &PendingTaskExecution{AttemptID: "a1", AttemptNumber: 1, LeaseID: "l1", Revision: 1},
			incoming: &PendingTaskExecution{AttemptID: "a1", AttemptNumber: 1, LeaseID: "l1", Revision: 99},
			want:     OfferIdentityConflict,
		},
		{
			name:     "same attempt, different lease AND revision → IdentityConflict",
			existing: &PendingTaskExecution{AttemptID: "a1", AttemptNumber: 1, LeaseID: "l1", Revision: 1},
			incoming: &PendingTaskExecution{AttemptID: "a1", AttemptNumber: 1, LeaseID: "lX", Revision: 99},
			want:     OfferIdentityConflict,
		},
		{
			name:     "higher attempt with different lease → Replaced",
			existing: &PendingTaskExecution{AttemptID: "a1", AttemptNumber: 1, LeaseID: "l1", Revision: 1},
			incoming: &PendingTaskExecution{AttemptID: "a2", AttemptNumber: 2, LeaseID: "lX", Revision: 2},
			want:     OfferReplaced,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := comparePendingOffer(tt.existing, tt.incoming)
			if got != tt.want {
				t.Errorf("comparePendingOffer() = %d, want %d", got, tt.want)
			}
		})
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
