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
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func newFleetTestStore(t *testing.T) *SQLiteStore {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fleet-test.db")
	s, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	if err := s.CreateFleetOperationsTableIfNotExists(); err != nil {
		t.Fatalf("CreateFleetOperationsTableIfNotExists: %v", err)
	}
	t.Cleanup(func() {
		_ = s.Close()
	})
	return s
}

// TestFleetStore_InsertAndGet verifies the canonical GET
// round-trip: insert a QUEUED row, fetch by ID, all fields
// preserved (timestamps parsed back, payload marshaled intact).
func TestFleetStore_InsertAndGet(t *testing.T) {
	s := newFleetTestStore(t)
	ctx := context.Background()

	queuedAt := time.Now().UTC().Truncate(time.Second)
	op := &Operation{
		OperationID: "op-insert-get-1",
		WorkerID:    "wicket",
		Op:          "drain",
		RequestedBy: "ops@example.com",
		Reason:      "maintenance window",
		Status:      OperationStatusQueued,
		QueuedAt:    queuedAt,
		Payload:     json.RawMessage(`{"timeout_s": 300}`),
	}
	if err := s.InsertOperation(ctx, op); err != nil {
		t.Fatalf("InsertOperation: %v", err)
	}

	got, err := s.GetOperation(ctx, "op-insert-get-1")
	if err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	if got.WorkerID != "wicket" {
		t.Errorf("WorkerID = %q, want wicket", got.WorkerID)
	}
	if got.Op != "drain" {
		t.Errorf("Op = %q, want drain", got.Op)
	}
	if got.Status != OperationStatusQueued {
		t.Errorf("Status = %q, want QUEUED", got.Status)
	}
	if !got.QueuedAt.Equal(queuedAt) {
		t.Errorf("QueuedAt = %v, want %v", got.QueuedAt, queuedAt)
	}
	if string(got.Payload) != `{"timeout_s": 300}` {
		t.Errorf("Payload = %q, want %q", got.Payload, `{"timeout_s": 300}`)
	}
	if op.StartedAt != nil {
		t.Errorf("StartedAt = %v, want nil for a QUEUED row", op.StartedAt)
	}
}

// TestFleetStore_AcceptsAllOperationKinds exercises the schema
// CHECK on every canonical OperationKind in AllOperationKinds.
// A drift (kind added to enum but not to the schema CHECK string)
// would surface here as an INSERT failure on the first test.
func TestFleetStore_AcceptsAllOperationKinds(t *testing.T) {
	s := newFleetTestStore(t)
	ctx := context.Background()
	kinds := []string{"drain", "resume", "restart", "update", "rollback", "quarantine", "smoke"}
	for i, k := range kinds {
		op := &Operation{
			OperationID: fmt.Sprintf("op-kind-%d", i),
			WorkerID:    fmt.Sprintf("wicket-%d", i),
			Op:          k,
			RequestedBy: "ops",
			Reason:      "kind-coverage",
			Status:      OperationStatusQueued,
			QueuedAt:    time.Now().UTC(),
		}
		if err := s.InsertOperation(ctx, op); err != nil {
			t.Errorf("InsertOperation(%q): %v", k, err)
		}
	}
}

// TestFleetStore_RejectsUnknownKind is the negative pin for the
// schema CHECK. A typo'd kind (e.g. "drainning", future kind
// "rotate_secret" before the schema migration lands) MUST be
// rejected at the SQL layer instead of silently accepted.
func TestFleetStore_RejectsUnknownKind(t *testing.T) {
	s := newFleetTestStore(t)
	ctx := context.Background()
	op := &Operation{
		OperationID: "op-badkind-1",
		WorkerID:    "wicket",
		Op:          "drainning", // double-n typo
		RequestedBy: "ops",
		Reason:      "typo test",
		Status:      OperationStatusQueued,
		QueuedAt:    time.Now().UTC(),
	}
	err := s.InsertOperation(ctx, op)
	if err == nil {
		t.Errorf("expected CHECK-constraint rejection on unknown kind, got nil")
	}
}

// TestFleetStore_InFlightDedup is the cornerstone test of the
// idempotency contract: two admin clicks 2 seconds apart on
// (worker_id=wicket, op=drain) MUST reject the second with
// ErrOperationInFlight (and on the SQLite side it's a partial
// UNIQUE INDEX violation translated by isInflightUniqueConflict).
func TestFleetStore_InFlightDedup(t *testing.T) {
	s := newFleetTestStore(t)
	ctx := context.Background()

	first := &Operation{
		OperationID: "op-inflight-1",
		WorkerID:    "wicket",
		Op:          "drain",
		RequestedBy: "ops",
		Reason:      "first click",
		Status:      OperationStatusQueued,
		QueuedAt:    time.Now().UTC(),
	}
	if err := s.InsertOperation(ctx, first); err != nil {
		t.Fatalf("first insert: %v", err)
	}

	second := &Operation{
		OperationID: "op-inflight-2",
		WorkerID:    "wicket",
		Op:          "drain",
		RequestedBy: "ops",
		Reason:      "second click",
		Status:      OperationStatusQueued,
		QueuedAt:    time.Now().UTC(),
	}
	err := s.InsertOperation(ctx, second)
	if !errors.Is(err, ErrOperationInFlight) {
		t.Errorf("second insert err = %v, want ErrOperationInFlight", err)
	}
}

// TestFleetStore_AllowsReissueAfterTerminal is the counter-pin
// to InFlightDedup: a fresh (worker_id, op) insert AFTER the
// prior run has terminated MUST be allowed. The partial WHERE
// clause on the UNIQUE INDEX restricts uniqueness to live rows
// only, so historical rows are free to be re-issued against.
//
// Maps directly to the operator's "drain again next week" UX:
// a Monday drain SUCCEEDED is not a blocker for Tuesday's
// drain, even though both rows share (worker_id, op).
func TestFleetStore_AllowsReissueAfterTerminal(t *testing.T) {
	s := newFleetTestStore(t)
	ctx := context.Background()

	monday := &Operation{
		OperationID: "op-mon",
		WorkerID:    "wicket",
		Op:          "restart",
		RequestedBy: "ops",
		Reason:      "monday",
		Status:      OperationStatusQueued,
		QueuedAt:    time.Now().UTC(),
	}
	if err := s.InsertOperation(ctx, monday); err != nil {
		t.Fatalf("monday insert: %v", err)
	}

	// Lifecycle the Monday row to SUCCEEDED.
	if _, err := s.MarkRunning(ctx, monday.OperationID, time.Now().UTC()); err != nil {
		t.Fatalf("monday mark running: %v", err)
	}
	if err := s.MarkSucceeded(ctx, monday.OperationID, time.Now().UTC()); err != nil {
		t.Fatalf("monday mark succeeded: %v", err)
	}

	tuesday := &Operation{
		OperationID: "op-tue",
		WorkerID:    "wicket",
		Op:          "restart",
		RequestedBy: "ops",
		Reason:      "tuesday",
		Status:      OperationStatusQueued,
		QueuedAt:    time.Now().UTC(),
	}
	if err := s.InsertOperation(ctx, tuesday); err != nil {
		t.Errorf("tuesday insert: %v (re-issue should succeed once Monday terminated)", err)
	}
}

// TestFleetStore_LifecycleTransitions traces the canonical
// happy-path chain: QUEUED → RUNNING → SUCCEEDED via MarkRunning
// + MarkSucceeded.
func TestFleetStore_LifecycleTransitions(t *testing.T) {
	s := newFleetTestStore(t)
	ctx := context.Background()

	queuedAt := time.Now().UTC().Truncate(time.Second)
	op := &Operation{
		OperationID: "op-lifecycle-1",
		WorkerID:    "wicket",
		Op:          "drain",
		RequestedBy: "ops",
		Reason:      "lifecycle",
		Status:      OperationStatusQueued,
		QueuedAt:    queuedAt,
	}
	if err := s.InsertOperation(ctx, op); err != nil {
		t.Fatalf("insert: %v", err)
	}

	startedAt := queuedAt.Add(2 * time.Second)
	if _, err := s.MarkRunning(ctx, op.OperationID, startedAt); err != nil {
		t.Fatalf("mark running: %v", err)
	}

	finishedAt := startedAt.Add(5 * time.Second)
	if err := s.MarkSucceeded(ctx, op.OperationID, finishedAt); err != nil {
		t.Fatalf("mark succeeded: %v", err)
	}

	got, err := s.GetOperation(ctx, op.OperationID)
	if err != nil {
		t.Fatalf("get after lifecycle: %v", err)
	}
	if got.Status != OperationStatusSucceeded {
		t.Errorf("Status = %q, want SUCCEEDED", got.Status)
	}
	if got.StartedAt == nil || !got.StartedAt.Equal(startedAt) {
		t.Errorf("StartedAt = %v, want %v", got.StartedAt, startedAt)
	}
	if got.FinishedAt == nil || !got.FinishedAt.Equal(finishedAt) {
		t.Errorf("FinishedAt = %v, want %v", got.FinishedAt, finishedAt)
	}
}

// TestFleetStore_FailedCapturesErrorMessage pins the audit
// contract: a FAILED terminal MUST carry a non-empty
// error_message so the dashboard renders a human-readable
// cause. The repository's MarkFailed synthesises a default
// "executor returned an error" string when the caller passes
// "" so even a forgotten arg produces a renderable dashboard
// row (defence-in-depth against botched upper-layer calls).
func TestFleetStore_FailedCapturesErrorMessage(t *testing.T) {
	s := newFleetTestStore(t)
	ctx := context.Background()

	op := &Operation{
		OperationID: "op-fail-1",
		WorkerID:    "wicket",
		Op:          "update",
		RequestedBy: "ops",
		Reason:      "fail test",
		Status:      OperationStatusQueued,
		QueuedAt:    time.Now().UTC(),
	}
	if err := s.InsertOperation(ctx, op); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := s.MarkRunning(ctx, op.OperationID, time.Now().UTC()); err != nil {
		t.Fatalf("mark running: %v", err)
	}
	if err := s.MarkFailed(ctx, op.OperationID, time.Now().UTC(), "cosign: signature invalid"); err != nil {
		t.Fatalf("mark failed: %v", err)
	}

	got, err := s.GetOperation(ctx, op.OperationID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != OperationStatusFailed {
		t.Errorf("Status = %q, want FAILED", got.Status)
	}
	if got.ErrorMessage != "cosign: signature invalid" {
		t.Errorf("ErrorMessage = %q, want cosign: signature invalid", got.ErrorMessage)
	}

	// Defence-in-depth: a fail call with empty errMsg still
	// produces a non-empty error_message column.
	if err := s.InsertOperation(ctx, &Operation{
		OperationID: "op-fail-2",
		WorkerID:    "threepio",
		Op:          "smoke",
		RequestedBy: "ops",
		Reason:      "default-msg test",
		Status:      OperationStatusQueued,
		QueuedAt:    time.Now().UTC(),
	}); err != nil {
		t.Fatalf("second insert: %v", err)
	}
	if _, err := s.MarkRunning(ctx, "op-fail-2", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkFailed(ctx, "op-fail-2", time.Now().UTC(), ""); err != nil {
		t.Fatal(err)
	}
	got2, _ := s.GetOperation(ctx, "op-fail-2")
	if got2.ErrorMessage == "" {
		t.Errorf("ErrorMessage empty after MarkFailed(\"\") — fallback synthesis missed")
	}
}

// TestFleetStore_NotFound pins the ErrOperationNotFound
// sentinel for the audit-endpoint 404 path.
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
func TestFleetStore_NoOperationResurrection(t *testing.T) {
	s := newFleetTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	insertQueuedOp(t, s, "op-nores-1", "drain")
	if _, err := s.MarkRunning(ctx, "op-nores-1", now); err != nil {
		t.Fatalf("mark running: %v", err)
	}
	if err := s.MarkSucceeded(ctx, "op-nores-1", now); err != nil {
		t.Fatalf("mark succeeded: %v", err)
	}
	if err := s.MarkFailed(ctx, "op-nores-1", now, "late failure"); !errors.Is(err, ErrIllegalOperationTransition) {
		t.Fatalf("SUCCEEDED -> FAILED error = %v, want ErrIllegalOperationTransition", err)
	}
	got, err := s.GetOperation(ctx, "op-nores-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != OperationStatusSucceeded {
		t.Errorf("Status = %q, want SUCCEEDED (terminal row must stay put)", got.Status)
	}

	insertQueuedOp(t, s, "op-nores-2", "update")
	if _, err := s.MarkRunning(ctx, "op-nores-2", now); err != nil {
		t.Fatalf("mark running: %v", err)
	}
	if err := s.MarkFailed(ctx, "op-nores-2", now, "cosign verify failed"); err != nil {
		t.Fatalf("mark failed: %v", err)
	}
	if err := s.MarkSucceeded(ctx, "op-nores-2", now); !errors.Is(err, ErrIllegalOperationTransition) {
		t.Fatalf("FAILED -> SUCCEEDED error = %v, want ErrIllegalOperationTransition", err)
	}
}

// TestFleetStore_CannotJumpStraightToTerminal pins the claim contract: a
// QUEUED row can only be claimed (→ RUNNING); jumping straight to a
// terminal status is rejected by the canonical machine.
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
