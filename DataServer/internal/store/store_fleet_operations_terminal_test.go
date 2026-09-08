// Package store — Step 4/15 fleet-operator surface tests for
// the fleet_operations repository.
//
// Coverage rationale (each test exists for a specific
// invariant the dashboard or tick path relies on):
//
//   - InsertAndGet               — canonical GET round-trip
//   - AcceptsAllOperationKinds   — schema CHECK covers every
//     canonical kind (debug fix
//     when a new kind lands outside
//     sqlite/104)
//   - RejectsUnknownKind         — schema CHECK rejects typo'd
//     kinds (e.g. "drainning"); the
//     audit surface stays clean
//   - InFlightDedup              — partial UNIQUE INDEX fires on
//     duplicate (worker_id, op) while
//     a prior row is QUEUED/RUNNING
//   - AllowsReissueAfterTerminal — partial UNIQUE INDEX does NOT
//     fire on a re-issue after the
//     prior run terminates (Monday +
//     Tuesday reboots are both legit)
//   - LifecycleTransitions       — QUEUED → RUNNING → SUCCEEDED
//     chain writing started_at and
//     finished_at
//   - FailedCapturesErrorMessage — FAILED with non-empty
//     error_message so the dashboard
//     renders a cause
//   - NotFound                   — ErrOperationNotFound sentinel
//     for unknown operation_id
//   - ListsOrdered               — DESC by queued_at on the audit
//     surface
//   - ListFilters                — worker_id + status filters
//     combine cleanly
//   - QueuedListOrdered          — FIFO by queued_at ASC on the
//     tick dispatch path
//   - ListLimit                  — limit > 0 caps the rows
package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func TestFleetStore_CannotJumpStraightToTerminal(t *testing.T) {
	s := newFleetTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	insertQueuedOp(t, s, "op-jump-1", "restart")
	if err := s.MarkSucceeded(ctx, "op-jump-1", now); !errors.Is(err, ErrIllegalOperationTransition) {
		t.Fatalf("QUEUED -> SUCCEEDED error = %v, want ErrIllegalOperationTransition", err)
	}
	insertQueuedOp(t, s, "op-jump-2", "smoke")
	if err := s.MarkFailed(ctx, "op-jump-2", now, "premature"); !errors.Is(err, ErrIllegalOperationTransition) {
		t.Fatalf("QUEUED -> FAILED error = %v, want ErrIllegalOperationTransition", err)
	}

	for _, id := range []string{"op-jump-1", "op-jump-2"} {
		got, err := s.GetOperation(ctx, id)
		if err != nil {
			t.Fatalf("get %s: %v", id, err)
		}
		if got.Status != OperationStatusQueued {
			t.Errorf("Status(%s) = %q, want QUEUED (rejected transition must not move the row)", id, got.Status)
		}
	}
}

// TestFleetStore_TerminalMarkIsIdempotent pins the double-call safety the
// controller's terminal-persist retry loop depends on: marking a row that
// is ALREADY in the requested terminal state is a nil no-op, never an
// error.
func TestFleetStore_TerminalMarkIsIdempotent(t *testing.T) {
	s := newFleetTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	insertQueuedOp(t, s, "op-idem-1", "drain")
	if _, err := s.MarkRunning(ctx, "op-idem-1", now); err != nil {
		t.Fatalf("mark running: %v", err)
	}
	first := now.Add(time.Second).Truncate(time.Second)
	if err := s.MarkSucceeded(ctx, "op-idem-1", first); err != nil {
		t.Fatalf("mark succeeded: %v", err)
	}
	// Second call: already SUCCEEDED → idempotent no-op.
	if err := s.MarkSucceeded(ctx, "op-idem-1", now.Add(2*time.Second)); err != nil {
		t.Fatalf("second MarkSucceeded error = %v, want idempotent nil", err)
	}
	got, err := s.GetOperation(ctx, "op-idem-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != OperationStatusSucceeded {
		t.Errorf("Status = %q, want SUCCEEDED", got.Status)
	}
	if got.FinishedAt == nil || !got.FinishedAt.Equal(first) {
		t.Errorf("FinishedAt = %v, want %v (idempotent no-op must not restamp)", got.FinishedAt, first)
	}
}

// TestFleetStore_TerminalMarkOnMissingFailsClosed pins that a terminal
// transition against a row that does not exist surfaces ErrOperationNotFound
// instead of a silent no-op — the old WHERE-guard behaviour that swallowed
// missing rows.
func TestFleetStore_TerminalMarkOnMissingFailsClosed(t *testing.T) {
	s := newFleetTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	if err := s.MarkSucceeded(ctx, "op-ghost-term", now); !errors.Is(err, ErrOperationNotFound) {
		t.Fatalf("MarkSucceeded(missing) error = %v, want ErrOperationNotFound", err)
	}
	if err := s.MarkFailed(ctx, "op-ghost-term", now, "x"); !errors.Is(err, ErrOperationNotFound) {
		t.Fatalf("MarkFailed(missing) error = %v, want ErrOperationNotFound", err)
	}
}

// TestFleetStore_MarkRunningGuardedNoopOnTerminal pins the claim contract:
// a claim attempt against an already-terminal row is a guarded (false, nil)
// no-op — never a replay of the external executor and never an error.
func TestFleetStore_MarkRunningGuardedNoopOnTerminal(t *testing.T) {
	s := newFleetTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	insertQueuedOp(t, s, "op-claim-term", "restart")
	if _, err := s.MarkRunning(ctx, "op-claim-term", now); err != nil {
		t.Fatalf("mark running: %v", err)
	}
	if err := s.MarkSucceeded(ctx, "op-claim-term", now); err != nil {
		t.Fatalf("mark succeeded: %v", err)
	}

	claimed, err := s.MarkRunning(ctx, "op-claim-term", now)
	if err != nil {
		t.Fatalf("MarkRunning(terminal) error = %v, want guarded no-op nil", err)
	}
	if claimed {
		t.Errorf("MarkRunning(terminal) claimed = true, want false (never replay the executor)")
	}
	got, err := s.GetOperation(ctx, "op-claim-term")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != OperationStatusSucceeded {
		t.Errorf("Status = %q, want SUCCEEDED (no-op claim must not move the row)", got.Status)
	}
}

// TestFleetStore_MarkRunningIdempotent pins the duplicate-claim case: a
// second claim on an already-RUNNING row is (false, nil) and must not
// restamp started_at.
func TestFleetStore_MarkRunningIdempotent(t *testing.T) {
	s := newFleetTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	insertQueuedOp(t, s, "op-claim-dup", "drain")
	now = now.Truncate(time.Second)
	if _, err := s.MarkRunning(ctx, "op-claim-dup", now); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	claimed, err := s.MarkRunning(ctx, "op-claim-dup", now.Add(time.Minute))
	if err != nil {
		t.Fatalf("second claim error = %v, want idempotent nil", err)
	}
	if claimed {
		t.Errorf("second claim = true, want false")
	}
	got, err := s.GetOperation(ctx, "op-claim-dup")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.StartedAt == nil || !got.StartedAt.Equal(now) {
		t.Errorf("StartedAt = %v, want %v (idempotent claim must not restamp)", got.StartedAt, now)
	}
}

// TestFleetStore_ListLimit caps the audit-endpoint enumeration
// at the configured limit. `limit <= 0` must mean "no cap" —
// tested by the no-arg call above; this test pins the cap.
func TestFleetStore_ListLimit(t *testing.T) {
	s := newFleetTestStore(t)
	ctx := context.Background()

	// Each row uses a UNIQUE worker_id so the (worker_id, op)
	// in-flight UNIQUE constraint does not reject later inserts.
	// The limit test demonstrates ListOperations' LIMIT cap; the
	// in-flight de-dup contract is exercised separately in
	// TestFleetStore_InFlightDedup.
	for i := 0; i < 5; i++ {
		op := &Operation{
			OperationID: fmt.Sprintf("op-lim-%d", i),
			WorkerID:    fmt.Sprintf("wicket-%d", i),
			Op:          "drain",
			RequestedBy: "ops",
			Reason:      "limit",
			Status:      OperationStatusQueued,
			QueuedAt:    time.Now().UTC().Add(time.Duration(i) * time.Second),
		}
		if err := s.InsertOperation(ctx, op); err != nil {
			t.Fatal(err)
		}
	}

	list, err := s.ListOperations(ctx, "", "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 {
		t.Errorf("len = %d, want 2", len(list))
	}
}
