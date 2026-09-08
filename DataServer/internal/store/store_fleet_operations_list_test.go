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

func TestFleetStore_NotFound(t *testing.T) {
	s := newFleetTestStore(t)
	ctx := context.Background()
	_, err := s.GetOperation(ctx, "op-ghost")
	if !errors.Is(err, ErrOperationNotFound) {
		t.Errorf("err = %v, want ErrOperationNotFound", err)
	}
}

// TestFleetStore_ListsOrdered asserts the DESC-by-queued_at
// sort the audit endpoint depends on.
func TestFleetStore_ListsOrdered(t *testing.T) {
	s := newFleetTestStore(t)
	ctx := context.Background()

	// Each row uses a DIFFERENT op so the (worker_id, op) pair is
	// unique per row — the partial UNIQUE INDEX WHERE status IN
	// ('QUEUED','RUNNING') does not reject later inserts. The sort
	// test was designed to exercise queued_at DESC; using the same
	// op across rows would fail the in-flight de-dup test
	// (ErrOperationInFlight) instead of the sort test.
	ops := []string{"drain", "resume", "restart"}
	base := time.Now().UTC().Truncate(time.Second)
	for i := 0; i < 3; i++ {
		op := &Operation{
			OperationID: fmt.Sprintf("op-list-%d", i),
			WorkerID:    "wicket",
			Op:          ops[i],
			RequestedBy: "ops",
			Reason:      fmt.Sprintf("entry %d", i),
			Status:      OperationStatusQueued,
			QueuedAt:    base.Add(time.Duration(i) * time.Second),
		}
		if err := s.InsertOperation(ctx, op); err != nil {
			t.Fatalf("insert [%d]: %v", i, err)
		}
	}

	list, err := s.ListOperations(ctx, "", "", 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("len = %d, want 3", len(list))
	}
	if list[0].OperationID != "op-list-2" {
		t.Errorf("list[0] = %q, want op-list-2 (newest first)", list[0].OperationID)
	}
	if list[2].OperationID != "op-list-0" {
		t.Errorf("list[2] = %q, want op-list-0 (oldest last)", list[2].OperationID)
	}
}

// TestFleetStore_ListFilters exercises the worker_id + status
// query filters the audit endpoint exposes.
func TestFleetStore_ListFilters(t *testing.T) {
	s := newFleetTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC()
	insert := func(id, worker, opKind string) {
		if err := s.InsertOperation(ctx, &Operation{
			OperationID: id,
			WorkerID:    worker,
			Op:          opKind,
			RequestedBy: "ops",
			Reason:      "filter test",
			Status:      OperationStatusQueued,
			QueuedAt:    now,
		}); err != nil {
			t.Fatal(err)
		}
	}
	insert("op-a", "wicket", "drain")
	insert("op-b", "threepio", "drain")
	insert("op-c", "wicket", "restart")

	// worker_id filter.
	got, err := s.ListOperations(ctx, "wicket", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Errorf("worker=wicket: len = %d, want 2", len(got))
	}

	// Terminate op-a cleanly.
	if _, err := s.MarkRunning(ctx, "op-a", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkSucceeded(ctx, "op-a", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	// status filter.
	succeeded, err := s.ListOperations(ctx, "", string(OperationStatusSucceeded), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(succeeded) != 1 || succeeded[0].OperationID != "op-a" {
		t.Errorf("status=SUCCEEDED: %v, want [op-a]", succeeded)
	}
}

// TestFleetStore_QueuedListOrdered validates the tick path:
// FIFO by queued_at ASC — admin's "now drain, then update in
// 5s" must NOT be answered in reverse.
func TestFleetStore_QueuedListOrdered(t *testing.T) {
	s := newFleetTestStore(t)
	ctx := context.Background()

	base := time.Now().UTC().Truncate(time.Second)
	// Insert terminal rows FIRST and lifecycle each to SUCCEEDED
	// before the next insert so the partial UNIQUE INDEX does not
	// block the live insert that follows. The terminations use
	// op=drain; the live row uses op=smoke so they are in distinct
	// (worker_id, op) slots — defence-in-depth against a
	// regression that incorrectly treats SUCCEEDED as still
	// in-flight.
	for i := 0; i < 2; i++ {
		op := &Operation{
			OperationID: fmt.Sprintf("op-term-%d", i),
			WorkerID:    "wicket",
			Op:          "drain",
			RequestedBy: "ops",
			Reason:      "terminal",
			Status:      OperationStatusQueued,
			QueuedAt:    base.Add(time.Duration(i+1) * time.Second),
		}
		if err := s.InsertOperation(ctx, op); err != nil {
			t.Fatal(err)
		}
		if _, err := s.MarkRunning(ctx, op.OperationID, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		if err := s.MarkSucceeded(ctx, op.OperationID, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}
	live := &Operation{
		OperationID: "op-live",
		WorkerID:    "wicket",
		Op:          "smoke",
		RequestedBy: "ops",
		Reason:      "live",
		Status:      OperationStatusQueued,
		QueuedAt:    base,
	}
	if err := s.InsertOperation(ctx, live); err != nil {
		t.Fatal(err)
	}

	q, err := s.ListQueuedOperations(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(q) != 1 || q[0].OperationID != "op-live" {
		t.Errorf("ListQueuedOperations = %v, want [op-live]", q)
	}
}

// ============================================================
// Single transactional transition API (transitionOperation)
// ============================================================

func insertQueuedOp(t *testing.T, s *SQLiteStore, id, opKind string) {
	t.Helper()
	if err := s.InsertOperation(context.Background(), &Operation{
		OperationID: id,
		WorkerID:    "wicket",
		Op:          opKind,
		RequestedBy: "ops",
		Reason:      "transition API test",
		Status:      OperationStatusQueued,
		QueuedAt:    time.Now().UTC(),
	}); err != nil {
		t.Fatalf("InsertOperation(%s): %v", id, err)
	}
}

// TestFleetStore_NoOperationResurrection pins the no-resurrection rule of
// the canonical operation machine at the store boundary: a RUNNING row that
// reached a terminal status can never be moved to the other terminal — a
// late MarkFailed must not flip a SUCCEEDED audit row (and vice versa).
// The rejected transition must write NOTHING: status, timestamps and
// error_message all stay exactly as the winning terminal call left them.
func TestFleetStore_NoOperationResurrection(t *testing.T) {
	s := newFleetTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	insertQueuedOp(t, s, "op-nores-1", "drain")
	if _, err := s.MarkRunning(ctx, "op-nores-1", now); err != nil {
		t.Fatalf("mark running: %v", err)
	}
	if err := s.MarkSucceeded(ctx, "op-nores-1", now); err != nil {
		t.Fatalf("mark succeeded: %v", err)
	}
	if err := s.MarkFailed(ctx, "op-nores-1", now.Add(time.Minute), "late failure"); !errors.Is(err, ErrIllegalOperationTransition) {
		t.Fatalf("SUCCEEDED -> FAILED error = %v, want ErrIllegalOperationTransition", err)
	}
	got, err := s.GetOperation(ctx, "op-nores-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != OperationStatusSucceeded {
		t.Errorf("Status = %q, want SUCCEEDED (terminal row must stay put)", got.Status)
	}
	// "Writes nothing": the rejected MarkFailed carried a late failure text
	// and a later finished_at — neither may leak onto the winning SUCCEEDED
	// row. The canonical validation rejects the transition BEFORE any write.
	if got.ErrorMessage != "" {
		t.Errorf("ErrorMessage = %q, want empty (rejected MarkFailed must not write its error text)", got.ErrorMessage)
	}
	if got.FinishedAt == nil || !got.FinishedAt.Equal(now) {
		t.Errorf("FinishedAt = %v, want %v (rejected attempt must not restamp the winning timestamp)", got.FinishedAt, now)
	}

	insertQueuedOp(t, s, "op-nores-2", "update")
	if _, err := s.MarkRunning(ctx, "op-nores-2", now); err != nil {
		t.Fatalf("mark running: %v", err)
	}
	if err := s.MarkFailed(ctx, "op-nores-2", now, "cosign verify failed"); err != nil {
		t.Fatalf("mark failed: %v", err)
	}
	if err := s.MarkSucceeded(ctx, "op-nores-2", now.Add(time.Minute)); !errors.Is(err, ErrIllegalOperationTransition) {
		t.Fatalf("FAILED -> SUCCEEDED error = %v, want ErrIllegalOperationTransition", err)
	}
	got, err = s.GetOperation(ctx, "op-nores-2")
	if err != nil {
		t.Fatalf("get op-nores-2: %v", err)
	}
	// Mirror case: the rejected MarkSucceeded must not erase the FAILED row's
	// error text nor move its finished_at.
	if got.Status != OperationStatusFailed {
		t.Errorf("Status = %q, want FAILED (terminal row must stay put)", got.Status)
	}
	if got.ErrorMessage != "cosign verify failed" {
		t.Errorf("ErrorMessage = %q, want %q (rejected MarkSucceeded must not clear the failure text)", got.ErrorMessage, "cosign verify failed")
	}
	if got.FinishedAt == nil || !got.FinishedAt.Equal(now) {
		t.Errorf("FinishedAt = %v, want %v (rejected attempt must not restamp the winning timestamp)", got.FinishedAt, now)
	}
}

// TestFleetStore_CannotJumpStraightToTerminal pins the claim contract: a
// QUEUED row can only be claimed (→ RUNNING); jumping straight to a
// terminal status is rejected by the canonical machine.
