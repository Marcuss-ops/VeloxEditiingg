package completion

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"
)

// seedAttemptCommit inserts a controlled attempt_commits row so the
// gate tests can exercise the Read / ReadOrMissing paths without
// going through DeclareOutputs. Returns the canonical commit_id so
// tests can assert against the gate's response.
func seedAttemptCommit(t *testing.T, db *sql.DB, fence FenceTuple, status string, requiredCount int) string {
	t.Helper()
	commitIDRaw, err := newUUIDLowerHex()
	if err != nil {
		t.Fatalf("seedAttemptCommit: mint commit_id: %v", err)
	}
	commitID := commitIDRaw + "_test"
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`
		INSERT INTO attempt_commits (
			commit_id, task_id, attempt_id, job_id, worker_id, lease_id,
			task_revision, status, required_output_count, ready_output_count,
			commit_token_hash, commit_deadline_at, last_progress_at,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?)`,
		commitID, fence.TaskID, fence.AttemptID, "job-test", fence.WorkerID, fence.LeaseID,
		fence.Revision, status, requiredCount,
		"hash-"+commitID, now, now, now, now,
	); err != nil {
		t.Fatalf("seed attempt_commits: %v", err)
	}
	return commitID
}

// ────────────────────────────────────────────────────────────────────────
// FenceTuple.Read tests — the central gate
// ────────────────────────────────────────────────────────────────────────

// TestFenceTuple_Read_HappyPath: a fence that matches the canonical
// attempt_commits row on every field returns the canonical state
// (commit_id, status, task_revision). The state.CommitID is what
// downstream CAS predicates must use.
func TestFenceTuple_Read_HappyPath(t *testing.T) {
	db := openCoordinatorTestDB(t)
	fence := validFence("task-read-happy", "attempt-read-happy")
	commitID := seedAttemptCommit(t, db, fence, "DECLARED", 1)

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	state, err := fence.Read(context.Background(), tx)
	if err != nil {
		t.Fatalf("Read happy: %v", err)
	}
	if state.CommitID != commitID {
		t.Errorf("state.CommitID: got %q, want %q", state.CommitID, commitID)
	}
	if state.Status != "DECLARED" {
		t.Errorf("state.Status: got %q, want DECLARED", state.Status)
	}
	if state.TaskRevision != fence.Revision {
		t.Errorf("state.TaskRevision: got %d, want %d", state.TaskRevision, fence.Revision)
	}
}

// TestFenceTuple_Read_NoRow: a fence pointing at a (task_id,
// attempt_id) with no attempt_commits row returns
// ErrAttemptCommitNotFound. This is the canonical "stale worker on
// a reaped lease before any declare" case.
func TestFenceTuple_Read_NoRow(t *testing.T) {
	db := openCoordinatorTestDB(t)
	fence := validFence("task-read-norow", "attempt-read-norow")
	// No seed: attempt_commits is empty for this tuple.

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	state, err := fence.Read(context.Background(), tx)
	if err == nil {
		t.Fatal("Read on missing row: expected error, got nil")
	}
	if state != nil {
		t.Errorf("Read on missing row: state should be nil on error, got %+v", state)
	}
	if !errors.Is(err, ErrAttemptCommitNotFound) {
		t.Errorf("Read on missing row: error should wrap ErrAttemptCommitNotFound, got: %v", err)
	}
}

// TestFenceTuple_Read_StaleWorker: a fence whose worker_id differs
// from the canonical row returns ErrTransitionConflict. The
// diagnostic message includes both the supplied and the stored
// worker_id so an operator can quickly identify the impersonation.
func TestFenceTuple_Read_StaleWorker(t *testing.T) {
	db := openCoordinatorTestDB(t)
	original := validFence("task-stale-w", "attempt-stale-w")
	seedAttemptCommit(t, db, original, "DECLARED", 1)

	stale := original
	stale.WorkerID = "impostor-worker"

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	_, err = stale.Read(context.Background(), tx)
	if err == nil {
		t.Fatal("Read with stale worker_id: expected error, got nil")
	}
	if !errors.Is(err, ErrTransitionConflict) {
		t.Errorf("Read with stale worker_id: error should wrap ErrTransitionConflict, got: %v", err)
	}
	if !strings.Contains(err.Error(), "impostor-worker") {
		t.Errorf("error should mention supplied worker_id, got: %v", err)
	}
	if !strings.Contains(err.Error(), original.WorkerID) {
		t.Errorf("error should mention stored worker_id, got: %v", err)
	}
	if !strings.Contains(err.Error(), "DECLARED") {
		t.Errorf("error should mention stored status, got: %v", err)
	}
}

// TestFenceTuple_Read_StaleLease: a fence whose lease_id differs
// from the canonical row returns ErrTransitionConflict. This is
// the canonical "reaped-lease stale-worker retry" closure.
func TestFenceTuple_Read_StaleLease(t *testing.T) {
	db := openCoordinatorTestDB(t)
	original := validFence("task-stale-l", "attempt-stale-l")
	seedAttemptCommit(t, db, original, "DECLARED", 1)

	stale := original
	stale.LeaseID = "reaped-lease"

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	_, err = stale.Read(context.Background(), tx)
	if err == nil {
		t.Fatal("Read with stale lease_id: expected error, got nil")
	}
	if !errors.Is(err, ErrTransitionConflict) {
		t.Errorf("Read with stale lease_id: error should wrap ErrTransitionConflict, got: %v", err)
	}
	if !strings.Contains(err.Error(), "reaped-lease") {
		t.Errorf("error should mention supplied lease_id, got: %v", err)
	}
	if !strings.Contains(err.Error(), "DECLARED") {
		t.Errorf("error should mention stored status, got: %v", err)
	}
}

// TestFenceTuple_Read_StaleRevision: a fence whose task_revision
// is older than the canonical row's task_revision (e.g. the master
// has bumped the revision via a CAS the worker has not yet observed)
// returns ErrTransitionConflict. Closes the "stale-revision replay"
// window that would otherwise allow a reaped-lease worker to
// race past the master-side advancement.
func TestFenceTuple_Read_StaleRevision(t *testing.T) {
	db := openCoordinatorTestDB(t)
	original := validFence("task-stale-r", "attempt-stale-r")
	seedAttemptCommit(t, db, original, "DECLARED", 1)

	// Bump the fence revision by 5 to simulate a master-side CAS
	// chain that the worker has not yet observed.
	stale := original
	stale.Revision = original.Revision + 5

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	_, err = stale.Read(context.Background(), tx)
	if err == nil {
		t.Fatal("Read with stale revision: expected error, got nil")
	}
	if !errors.Is(err, ErrTransitionConflict) {
		t.Errorf("Read with stale revision: error should wrap ErrTransitionConflict, got: %v", err)
	}
	if !strings.Contains(err.Error(), "DECLARED") {
		t.Errorf("error should mention stored status, got: %v", err)
	}
}

// TestFenceTuple_Read_InvalidFence: a fence with any empty identity
// field (TaskID, AttemptID, WorkerID, LeaseID) or a negative
// Revision is rejected at the entry of Read with ErrFenceMismatch.
// No DB I/O happens on the invalid-input path (the gate short-
// circuits before tx.QueryRowContext).
func TestFenceTuple_Read_InvalidFence(t *testing.T) {
	db := openCoordinatorTestDB(t)
	bad := FenceTuple{TaskID: "t"} // missing AttemptID, WorkerID, LeaseID

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	_, err = bad.Read(context.Background(), tx)
	if err == nil {
		t.Fatal("Read on invalid fence: expected error, got nil")
	}
	if !errors.Is(err, ErrFenceMismatch) {
		t.Errorf("Read on invalid fence: error should wrap ErrFenceMismatch, got: %v", err)
	}
}

// ────────────────────────────────────────────────────────────────────────
// FenceTuple.ReadOrMissing tests — the DeclareOutputs variant
// ────────────────────────────────────────────────────────────────────────

// TestFenceTuple_ReadOrMissing_NoRow: the no-row path returns
// (nil, nil) so DeclareOutputs can perform the canonical
// INSERT-OR-IGNORE. A non-nil error here would block the first
// declare on a fresh Attempt.
func TestFenceTuple_ReadOrMissing_NoRow(t *testing.T) {
	db := openCoordinatorTestDB(t)
	fence := validFence("task-rom-norow", "attempt-rom-norow")
	// No seed.

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	state, err := fence.ReadOrMissing(context.Background(), tx)
	if err != nil {
		t.Fatalf("ReadOrMissing on no-row: %v", err)
	}
	if state != nil {
		t.Errorf("ReadOrMissing on no-row: state should be nil, got %+v", state)
	}
}

// TestFenceTuple_ReadOrMissing_StaleWorker: when an existing row's
// worker_id does not match the fence, ReadOrMissing surfaces
// ErrTransitionConflict — same as Read. DeclareOutputs cannot
// insert a duplicate attempt_commits row for an existing identity
// that belongs to a different worker.
func TestFenceTuple_ReadOrMissing_StaleWorker(t *testing.T) {
	db := openCoordinatorTestDB(t)
	original := validFence("task-rom-stale", "attempt-rom-stale")
	seedAttemptCommit(t, db, original, "DECLARED", 1)

	stale := original
	stale.WorkerID = "different-worker"

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	_, err = stale.ReadOrMissing(context.Background(), tx)
	if err == nil {
		t.Fatal("ReadOrMissing stale worker: expected error, got nil")
	}
	if !errors.Is(err, ErrTransitionConflict) {
		t.Errorf("ReadOrMissing stale worker: error should wrap ErrTransitionConflict, got: %v", err)
	}
}

// TestFenceTuple_ReadOrMissing_HappyPath: an existing row whose
// fence matches returns the canonical state (commit_id + status).
// DeclareOutputs uses state.CommitID to skip the INSERT-OR-IGNORE
// and reuse the canonical row.
func TestFenceTuple_ReadOrMissing_HappyPath(t *testing.T) {
	db := openCoordinatorTestDB(t)
	fence := validFence("task-rom-happy", "attempt-rom-happy")
	commitID := seedAttemptCommit(t, db, fence, "DECLARED", 1)

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	state, err := fence.ReadOrMissing(context.Background(), tx)
	if err != nil {
		t.Fatalf("ReadOrMissing happy: %v", err)
	}
	if state == nil {
		t.Fatal("ReadOrMissing happy: state should not be nil on match")
	}
	if state.CommitID != commitID {
		t.Errorf("state.CommitID: got %q, want %q", state.CommitID, commitID)
	}
	if state.Status != "DECLARED" {
		t.Errorf("state.Status: got %q, want DECLARED", state.Status)
	}
}
