package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

// =====================================================================
// ClaimNextWithAttemptAtomic — atomicity test (PR-2 / fix/canonical-attempt-identity).
//
// The 4-sub-op claim tx (SELECT READY → UPDATE CAS stamp
//   attempt_id + attempt_number + revision + lease_expires_at
// → INSERT task_attempts → SELECT task_specs.payload_json → COMMIT)
// MUST commit-or-rollback atomically. A middle-step failure (e.g.
// UNIQUE collision on task_attempts(task_id, attempt_number)) must NOT
// leave the Task in a half-claimed state with attempt_id stamped but
// no Attempt row minted — that would violate the §9.5 invariant.
//
// Failure-injection mechanism: drop task_specs so the final
// `SELECT payload_json FROM task_specs ...` fails AFTER the tx has
// already done the READY→LEASED CAS and inserted the PENDING attempt.
// The tx must roll back fully. The test then asserts:
//   1. tasks.status remained 'READY' (step-3 UPDATE rolled back)
//   2. tasks.attempt_id is NULL/'' (NOT stamped with uuid from step-2)
//   3. tasks.attempt_number is 0 (NOT stamped with computed value)
//   4. tasks.revision is unchanged (NOT bumped)
//   5. tasks.lease_expires_at is empty (NOT stamped with TTL date)
//   6. task_attempts has zero rows (the inserted pending attempt rolled back)
//   7. The function returned (nil, nil, err) — no phantom claim contract
//
// migration 052 added lease_expires_at to the production schema (mirrored
// at the top of this file) so ClaimNextWithAttemptAtomic's UPDATE can
// write it under -race; the new column is purely additive and unused by
// pre-existing test seeds.
// =====================================================================

func TestClaimNextWithAttemptAtomic_Atomicity(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	const (
		taskID      = "T-claim-atomicity-1"
		newWorkerID = "w-1"
		newLeaseID  = "L-1"
	)
	seedReadyTask(t, s.db, taskID, 0)

	if _, err := s.db.ExecContext(ctx, `DROP TABLE task_specs`); err != nil {
		t.Fatalf("drop task_specs for rollback injection: %v", err)
	}

	// Claim — the final task_specs read must fail, forcing full rollback.
	tws, att, err := r.ClaimNextWithAttemptAtomic(ctx, newWorkerID, newLeaseID)
	if err == nil {
		t.Fatalf("ClaimNextWithAttemptAtomic: expected spec-read failure, got nil (tws=%v att=%v)", tws, att)
	}
	if tws != nil {
		t.Errorf("tws=%v; want nil (rollback prevented claim contract)", tws)
	}
	if att != nil {
		t.Errorf("att=%v; want nil (rollback prevented attempt mint)", att)
	}

	// Atomicity assertion 1–4: tasks row preserved at READY + pre-state.
	var (
		taskStatus                                    string
		workerCol, leaseCol                           string
		attemptIDCol                                  sql.NullString
		attemptNumCol                                 sql.NullInt64
		revision                                      int
		leaseExpiresCol, startedAtCol, completedAtCol sql.NullString
	)
	err = s.db.QueryRowContext(ctx,
		`SELECT status, COALESCE(worker_id, ''), COALESCE(lease_id, ''),
		        attempt_id, attempt_number, revision,
		        COALESCE(lease_expires_at, ''), COALESCE(started_at, ''), COALESCE(completed_at, '')
		 FROM tasks WHERE task_id = ?`,
		taskID,
	).Scan(&taskStatus, &workerCol, &leaseCol, &attemptIDCol, &attemptNumCol, &revision,
		&leaseExpiresCol, &startedAtCol, &completedAtCol)
	if err != nil {
		t.Fatalf("post-rollback SELECT tasks: %v", err)
	}
	if taskStatus != "READY" {
		t.Errorf("tasks.status = %q; want READY (rollback preserved pre-state)", taskStatus)
	}
	if workerCol != "" || leaseCol != "" {
		t.Errorf("tasks worker_id=%q lease_id=%q want both empty (rollback preserved pre-state)", workerCol, leaseCol)
	}
	if attemptIDCol.Valid && attemptIDCol.String != "" {
		t.Errorf("tasks.attempt_id = %q; want NULL/empty (rollback did NOT clear canonical stamp)", attemptIDCol.String)
	}
	if attemptNumCol.Valid && attemptNumCol.Int64 != 0 {
		t.Errorf("tasks.attempt_number = %d; want 0 (rollback did NOT clear canonical stamp)", attemptNumCol.Int64)
	}
	if revision != 0 {
		t.Errorf("tasks.revision = %d; want 0 (rollback did NOT bump CAS counter)", revision)
	}
	if leaseExpiresCol.Valid && leaseExpiresCol.String != "" {
		t.Errorf("tasks.lease_expires_at = %q; want empty (rollback did NOT write lease_expires_at)", leaseExpiresCol.String)
	}
	if startedAtCol.Valid && startedAtCol.String != "" {
		t.Errorf("tasks.started_at = %q; want empty", startedAtCol.String)
	}
	if completedAtCol.Valid && completedAtCol.String != "" {
		t.Errorf("tasks.completed_at = %q; want empty", completedAtCol.String)
	}

	// Atomicity assertion 5: the inserted pending attempt rolled back.
	var count int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM task_attempts WHERE task_id = ?`,
		taskID,
	).Scan(&count); err != nil {
		t.Fatalf("COUNT task_attempts: %v", err)
	}
	if count != 0 {
		t.Errorf("task_attempts count = %d; want 0 after rollback", count)
	}
}

func TestReleaseLease_DeletesPendingAttemptAndClearsClaimIdentity(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	const (
		taskID      = "T-release-1"
		workerID    = "w-release-1"
		leaseID     = "L-release-1"
		attemptID   = "A-release-1"
		attemptNum  = 1
		taskRevSeed = 0
	)
	seedLeasedTask(t, s.db, taskID, workerID, leaseID, attemptID, attemptNum, taskRevSeed)

	if err := r.ReleaseLease(ctx, taskID, workerID, leaseID); err != nil {
		t.Fatalf("ReleaseLease: %v", err)
	}

	var (
		taskStatus      string
		clearedWorkerID string
		clearedLeaseID  string
		revision        int
		attemptIDCol    sql.NullString
		attemptNumCol   int
		leaseExpCol     sql.NullString
	)
	if err := s.db.QueryRowContext(ctx,
		`SELECT status, COALESCE(worker_id, ''), COALESCE(lease_id, ''),
		        revision, attempt_id, attempt_number, lease_expires_at
		 FROM tasks WHERE task_id = ?`,
		taskID,
	).Scan(&taskStatus, &clearedWorkerID, &clearedLeaseID,
		&revision, &attemptIDCol, &attemptNumCol, &leaseExpCol); err != nil {
		t.Fatalf("post-release SELECT tasks: %v", err)
	}
	if taskStatus != "READY" {
		t.Errorf("tasks.status = %q; want READY", taskStatus)
	}
	if clearedWorkerID != "" || clearedLeaseID != "" {
		t.Errorf("tasks worker_id=%q lease_id=%q; want both empty", clearedWorkerID, clearedLeaseID)
	}
	if attemptIDCol.Valid && attemptIDCol.String != "" {
		t.Errorf("tasks.attempt_id = %q; want NULL/empty after release", attemptIDCol.String)
	}
	if attemptNumCol != 0 {
		t.Errorf("tasks.attempt_number = %d; want 0 after release", attemptNumCol)
	}
	if leaseExpCol.Valid && leaseExpCol.String != "" {
		t.Errorf("tasks.lease_expires_at = %q; want NULL/empty after release", leaseExpCol.String)
	}
	if revision != 1 {
		t.Errorf("tasks.revision = %d; want 1 after release CAS", revision)
	}

	var attempts int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM task_attempts WHERE task_id = ?`,
		taskID,
	).Scan(&attempts); err != nil {
		t.Fatalf("count task_attempts after release: %v", err)
	}
	if attempts != 0 {
		t.Errorf("task_attempts rows = %d; want 0 after release deletes pending attempt", attempts)
	}
}

func TestClaimNextWithAttemptAtomic_UsesHistoricalMaxAttemptNumber(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	const (
		taskID          = "T-claim-historical-max"
		workerID        = "w-history-1"
		leaseID         = "L-history-1"
		previousAttempt = "A-history-1"
	)

	seedReadyTask(t, s.db, taskID, 0)

	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO task_attempts
		 (id, task_id, job_id, attempt_number, worker_id, lease_id, status,
		  report_version, created_at, updated_at)
		 VALUES (?, ?, ?, 1, ?, ?, 'TIMED_OUT', 0, ?, ?)`,
		previousAttempt, taskID, "job-"+taskID, workerID, leaseID, now, now,
	); err != nil {
		t.Fatalf("seed historical attempt: %v", err)
	}

	tws, att, err := r.ClaimNextWithAttemptAtomic(ctx, workerID, "L-history-2")
	if err != nil {
		t.Fatalf("ClaimNextWithAttemptAtomic drift-repair: %v", err)
	}
	if tws == nil || att == nil {
		t.Fatalf("claim returned nil tws/att: tws=%v att=%v", tws, att)
	}
	if att.AttemptNumber != 2 {
		t.Fatalf("attempt.AttemptNumber = %d; want 2", att.AttemptNumber)
	}

	var attemptCount, attemptNumber int
	if err := s.db.QueryRowContext(ctx,
		`SELECT attempt_count, attempt_number FROM tasks WHERE task_id = ?`,
		taskID,
	).Scan(&attemptCount, &attemptNumber); err != nil {
		t.Fatalf("SELECT repaired task: %v", err)
	}
	if attemptCount != 2 {
		t.Errorf("tasks.attempt_count = %d; want newly-minted attempt ordinal 2", attemptCount)
	}
	if attemptNumber != 2 {
		t.Errorf("tasks.attempt_number = %d; want claimed attempt 2", attemptNumber)
	}

	var invariantBreaches int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*)
		   FROM (
		     SELECT t.task_id
		     FROM tasks t
		     JOIN task_attempts ta ON ta.task_id = t.task_id
		     GROUP BY t.task_id
		     HAVING t.attempt_count < MAX(ta.attempt_number)
		   )`,
	).Scan(&invariantBreaches); err != nil {
		t.Fatalf("invariant query: %v", err)
	}
	if invariantBreaches != 0 {
		t.Errorf("attempt-count invariant breaches = %d; want 0", invariantBreaches)
	}
}

// =====================================================================
// ClaimTaskForWorkerAtomic — placement-driven exact-task claim tests.
//
// The placement matcher produces a (task_id, executor_id, executor_version)
// tuple. ClaimTaskForWorkerAtomic CAS-gates on all three plus revision
// so a concurrent dispatcher cannot claim the same task between
// ListReadyCandidates and Claim. The tests below assert:
//   - Happy path: matching revision + executor → LEASED + PENDING attempt
//   - Stale revision: wrong revision → ErrTransitionConflict
//   - Executor mismatch: wrong executor_id or executor_version → conflict
//   - Status mismatch: task not READY → conflict
//   - Concurrent claim: task already claimed by another dispatcher → conflict
//   - Atomicity: mid-tx failure (dropped task_specs) → full rollback
// =====================================================================

// TestClaimTaskForWorkerAtomic_HappyPath: READY task with matching
// revision + executor_id + executor_version → successful claim.
func TestClaimTaskForWorkerAtomic_HappyPath(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	const (
		taskID          = "T-claim-1"
		executorID      = "blender"
		executorVersion = 4
		taskRevision    = 0
		workerID        = "w-claim-1"
		leaseID         = "L-claim-1"
	)
	seedReadyTaskWithExecutor(t, s.db, taskID, executorID, executorVersion, taskRevision)

	cmd := taskgraph.ClaimTaskForWorkerCommand{
		TaskID:               taskID,
		ExpectedTaskRevision: taskRevision,
		WorkerID:             workerID,
		SessionID:            "sess-1",
		LeaseID:              leaseID,
		ExecutorID:           executorID,
		ExecutorVersion:      executorVersion,
		CapabilityRevision:   1,
	}
	tws, att, err := r.ClaimTaskForWorkerAtomic(ctx, cmd)
	if err != nil {
		t.Fatalf("ClaimTaskForWorkerAtomic happy path: %v", err)
	}
	if tws == nil || att == nil {
		t.Fatalf("expected non-nil TaskWithSpec + TaskAttempt; got tws=%v att=%v", tws, att)
	}

	// Verify Task row: LEASED + worker/lease stamped + revision bumped.
	var status, storedWorker, storedLease string
	var revision int
	if err := s.db.QueryRowContext(ctx,
		`SELECT status, worker_id, lease_id, revision FROM tasks WHERE task_id = ?`,
		taskID,
	).Scan(&status, &storedWorker, &storedLease, &revision); err != nil {
		t.Fatalf("post-claim SELECT tasks: %v", err)
	}
	if status != "LEASED" {
		t.Errorf("tasks.status = %q; want LEASED", status)
	}
	if storedWorker != workerID {
		t.Errorf("tasks.worker_id = %q; want %q", storedWorker, workerID)
	}
	if storedLease != leaseID {
		t.Errorf("tasks.lease_id = %q; want %q", storedLease, leaseID)
	}
	if revision != taskRevision+1 {
		t.Errorf("tasks.revision = %d; want %d", revision, taskRevision+1)
	}

	// Verify PENDING attempt exists.
	attFromDB := attemptForTask(t, s.db, taskID, workerID, leaseID)
	if attFromDB == nil {
		t.Fatal("PENDING attempt missing after claim")
	}
	if attFromDB.Status != taskattempts.AttemptStatusPending {
		t.Errorf("attempt.status = %s; want PENDING", attFromDB.Status)
	}
	if attFromDB.AttemptNumber != 1 {
		t.Errorf("attempt.attempt_number = %d; want 1", attFromDB.AttemptNumber)
	}

	// Verify returned TaskWithSpec carries the spec payload.
	if tws.Task.ID != taskID {
		t.Errorf("tws.Task.ID = %q; want %q", tws.Task.ID, taskID)
	}
	if tws.Task.WorkerID != workerID {
		t.Errorf("tws.Task.WorkerID = %q; want %q", tws.Task.WorkerID, workerID)
	}

	// Verify returned attempt matches the DB.
	if att.ID != attFromDB.ID {
		t.Errorf("returned attempt.ID = %q; DB attempt.ID = %q", att.ID, attFromDB.ID)
	}
}

// TestClaimTaskForWorkerAtomic_RevisionMismatch: task exists READY but
// the expected revision doesn't match → ErrTransitionConflict.
func TestClaimTaskForWorkerAtomic_RevisionMismatch(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	const (
		taskID          = "T-claim-rev"
		executorID      = "blender"
		executorVersion = 4
		taskRevision    = 3
	)
	seedReadyTaskWithExecutor(t, s.db, taskID, executorID, executorVersion, taskRevision)

	cmd := taskgraph.ClaimTaskForWorkerCommand{
		TaskID:               taskID,
		ExpectedTaskRevision: 99, // stale revision
		WorkerID:             "w-rev",
		SessionID:            "sess-rev",
		LeaseID:              "L-rev",
		ExecutorID:           executorID,
		ExecutorVersion:      executorVersion,
		CapabilityRevision:   1,
	}
	tws, att, err := r.ClaimTaskForWorkerAtomic(ctx, cmd)
	if err == nil {
		t.Fatalf("expected ErrTransitionConflict on stale revision; got nil (tws=%v att=%v)", tws, att)
	}
	if !errors.Is(err, taskgraph.ErrTransitionConflict) {
		t.Errorf("expected taskgraph.ErrTransitionConflict; got %v", err)
	}
	if tws != nil || att != nil {
		t.Errorf("expected nil return values on conflict; got tws=%v att=%v", tws, att)
	}

	// Verify task stayed READY (rollback preserved pre-claim state).
	var status string
	if err := s.db.QueryRowContext(ctx,
		`SELECT status FROM tasks WHERE task_id = ?`, taskID,
	).Scan(&status); err != nil {
		t.Fatalf("post-conflict SELECT: %v", err)
	}
	if status != "READY" {
		t.Errorf("tasks.status = %q; want READY (rollback)", status)
	}

	// Verify no attempt row was created.
	var count int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM task_attempts WHERE task_id = ?`, taskID,
	).Scan(&count); err != nil {
		t.Fatalf("count attempts: %v", err)
	}
	if count != 0 {
		t.Errorf("task_attempts rows = %d; want 0", count)
	}
}

// TestClaimTaskForWorkerAtomic_ExecutorIDMismatch: task READY but
// executor_id doesn't match → ErrTransitionConflict.
func TestClaimTaskForWorkerAtomic_ExecutorIDMismatch(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	seedReadyTaskWithExecutor(t, s.db, "T-claim-eid", "blender", 4, 0)

	cmd := taskgraph.ClaimTaskForWorkerCommand{
		TaskID:               "T-claim-eid",
		ExpectedTaskRevision: 0,
		WorkerID:             "w-eid",
		SessionID:            "sess-eid",
		LeaseID:              "L-eid",
		ExecutorID:           "nuke", // wrong executor
		ExecutorVersion:      4,
		CapabilityRevision:   1,
	}
	_, _, err := r.ClaimTaskForWorkerAtomic(ctx, cmd)
	if err == nil {
		t.Fatal("expected ErrTransitionConflict on executor_id mismatch")
	}
	if !errors.Is(err, taskgraph.ErrTransitionConflict) {
		t.Errorf("expected taskgraph.ErrTransitionConflict; got %v", err)
	}

	// Verify task stayed READY.
	var status string
	if err := s.db.QueryRowContext(ctx,
		`SELECT status FROM tasks WHERE task_id = 'T-claim-eid'`,
	).Scan(&status); err != nil {
		t.Fatalf("post-conflict SELECT: %v", err)
	}
	if status != "READY" {
		t.Errorf("tasks.status = %q; want READY", status)
	}
}

// TestClaimTaskForWorkerAtomic_ExecutorVersionMismatch: task READY but
// executor_version doesn't match → ErrTransitionConflict.
func TestClaimTaskForWorkerAtomic_ExecutorVersionMismatch(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	seedReadyTaskWithExecutor(t, s.db, "T-claim-ever", "blender", 4, 0)

	cmd := taskgraph.ClaimTaskForWorkerCommand{
		TaskID:               "T-claim-ever",
		ExpectedTaskRevision: 0,
		WorkerID:             "w-ever",
		SessionID:            "sess-ever",
		LeaseID:              "L-ever",
		ExecutorID:           "blender",
		ExecutorVersion:      99, // wrong version
		CapabilityRevision:   1,
	}
	_, _, err := r.ClaimTaskForWorkerAtomic(ctx, cmd)
	if err == nil {
		t.Fatal("expected ErrTransitionConflict on executor_version mismatch")
	}
	if !errors.Is(err, taskgraph.ErrTransitionConflict) {
		t.Errorf("expected taskgraph.ErrTransitionConflict; got %v", err)
	}

	var status string
	if err := s.db.QueryRowContext(ctx,
		`SELECT status FROM tasks WHERE task_id = 'T-claim-ever'`,
	).Scan(&status); err != nil {
		t.Fatalf("post-conflict SELECT: %v", err)
	}
	if status != "READY" {
		t.Errorf("tasks.status = %q; want READY", status)
	}
}

// TestClaimTaskForWorkerAtomic_NotReady: task exists but is not READY
// (e.g. already LEASED by another worker) → ErrTransitionConflict.
func TestClaimTaskForWorkerAtomic_NotReady(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	const taskID = "T-claim-notready"
	// Seed as LEASED (already claimed by someone else).
	seedLeasedTask(t, s.db, taskID, "w-other", "L-other", "A-other", 1, 0)

	cmd := taskgraph.ClaimTaskForWorkerCommand{
		TaskID:               taskID,
		ExpectedTaskRevision: 0,
		WorkerID:             "w-me",
		SessionID:            "sess-me",
		LeaseID:              "L-me",
		ExecutorID:           "blender",
		ExecutorVersion:      4,
		CapabilityRevision:   1,
	}
	_, _, err := r.ClaimTaskForWorkerAtomic(ctx, cmd)
	if err == nil {
		t.Fatal("expected ErrTransitionConflict when task not READY")
	}
	if !errors.Is(err, taskgraph.ErrTransitionConflict) {
		t.Errorf("expected taskgraph.ErrTransitionConflict; got %v", err)
	}

	// Task must still be LEASED with the original worker (no drift).
	var status, storedWorker, storedLease string
	if err := s.db.QueryRowContext(ctx,
		`SELECT status, worker_id, lease_id FROM tasks WHERE task_id = ?`, taskID,
	).Scan(&status, &storedWorker, &storedLease); err != nil {
		t.Fatalf("post-conflict SELECT: %v", err)
	}
	if status != "LEASED" {
		t.Errorf("tasks.status = %q; want LEASED", status)
	}
	if storedWorker != "w-other" || storedLease != "L-other" {
		t.Errorf("tasks worker/lease = %q/%q; want w-other/L-other", storedWorker, storedLease)
	}
}

// TestClaimTaskForWorkerAtomic_AlreadyClaimed: two concurrent claims
// race on the same task → one wins, the other gets ErrTransitionConflict.
func TestClaimTaskForWorkerAtomic_AlreadyClaimed(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	seedReadyTaskWithExecutor(t, s.db, "T-claim-race", "blender", 4, 0)

	claim := func(workerID, leaseID string) error {
		cmd := taskgraph.ClaimTaskForWorkerCommand{
			TaskID:               "T-claim-race",
			ExpectedTaskRevision: 0,
			WorkerID:             workerID,
			SessionID:            "sess-" + workerID,
			LeaseID:              leaseID,
			ExecutorID:           "blender",
			ExecutorVersion:      4,
			CapabilityRevision:   1,
		}
		_, _, err := r.ClaimTaskForWorkerAtomic(ctx, cmd)
		return err
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs <- claim(
				fmt.Sprintf("w-race-%d", idx),
				fmt.Sprintf("L-race-%d", idx),
			)
		}(i)
	}
	wg.Wait()
	close(errs)

	successes := 0
	conflicts := 0
	for err := range errs {
		if err == nil {
			successes++
		} else if errors.Is(err, taskgraph.ErrTransitionConflict) ||
			strings.Contains(err.Error(), "database table is locked") {
			conflicts++
		} else {
			t.Errorf("unexpected error: %v", err)
		}
	}
	if successes != 1 {
		t.Errorf("concurrent claims: successes=%d; want exactly 1", successes)
	}
	if conflicts != 1 {
		t.Errorf("concurrent claims: conflicts=%d; want exactly 1", conflicts)
	}

	// Verify exactly one LEASED row exists.
	var count int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM tasks WHERE task_id = 'T-claim-race' AND status = 'LEASED'`,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("LEASED tasks = %d; want 1", count)
	}

	// Verify exactly one PENDING attempt.
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM task_attempts WHERE task_id = 'T-claim-race'`,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("task_attempts rows = %d; want 1", count)
	}
}

// TestClaimTaskForWorkerAtomic_Atomicity: inject a mid-tx failure
// (DROP TABLE task_specs) and verify the entire tx rolls back —
// task stays READY, no attempt row is created, no lease stamp leaked.
func TestClaimTaskForWorkerAtomic_Atomicity(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	const (
		taskID          = "T-claim-atom"
		executorID      = "blender"
		executorVersion = 4
		taskRevision    = 0
		workerID        = "w-atom"
		leaseID         = "L-atom"
	)
	seedReadyTaskWithExecutor(t, s.db, taskID, executorID, executorVersion, taskRevision)

	// Drop task_specs so the final SELECT payload_json fails mid-tx.
	if _, err := s.db.ExecContext(ctx, `DROP TABLE task_specs`); err != nil {
		t.Fatalf("drop task_specs: %v", err)
	}

	cmd := taskgraph.ClaimTaskForWorkerCommand{
		TaskID:               taskID,
		ExpectedTaskRevision: taskRevision,
		WorkerID:             workerID,
		SessionID:            "sess-atom",
		LeaseID:              leaseID,
		ExecutorID:           executorID,
		ExecutorVersion:      executorVersion,
		CapabilityRevision:   1,
	}
	tws, att, err := r.ClaimTaskForWorkerAtomic(ctx, cmd)
	if err == nil {
		t.Fatalf("expected spec-read failure (dropped task_specs); got nil (tws=%v att=%v)", tws, att)
	}
	if tws != nil {
		t.Errorf("tws=%v; want nil (rollback prevented claim)", tws)
	}
	if att != nil {
		t.Errorf("att=%v; want nil (rollback prevented attempt mint)", att)
	}

	// Atomicity: task row preserved at READY + pre-state.
	var (
		status        string
		storedWorker  string
		storedLease   string
		revision      int
		attemptIDCol  sql.NullString
		attemptNumCol sql.NullInt64
		leaseExpCol   sql.NullString
	)
	err = s.db.QueryRowContext(ctx,
		`SELECT status, COALESCE(worker_id, ''), COALESCE(lease_id, ''),
		        revision, attempt_id, attempt_number,
		        COALESCE(lease_expires_at, '')
		 FROM tasks WHERE task_id = ?`,
		taskID,
	).Scan(&status, &storedWorker, &storedLease, &revision,
		&attemptIDCol, &attemptNumCol, &leaseExpCol)
	if err != nil {
		t.Fatalf("post-rollback SELECT: %v", err)
	}
	if status != "READY" {
		t.Errorf("tasks.status = %q; want READY (rollback)", status)
	}
	if storedWorker != "" || storedLease != "" {
		t.Errorf("tasks worker=%q lease=%q; want both empty", storedWorker, storedLease)
	}
	if attemptIDCol.Valid && attemptIDCol.String != "" {
		t.Errorf("tasks.attempt_id = %q; want NULL/empty", attemptIDCol.String)
	}
	if attemptNumCol.Valid && attemptNumCol.Int64 != 0 {
		t.Errorf("tasks.attempt_number = %d; want 0", attemptNumCol.Int64)
	}
	if revision != taskRevision {
		t.Errorf("tasks.revision = %d; want %d (no CAS bump)", revision, taskRevision)
	}
	if leaseExpCol.Valid && leaseExpCol.String != "" {
		t.Errorf("tasks.lease_expires_at = %q; want empty", leaseExpCol.String)
	}

	// No attempt row created.
	var count int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM task_attempts WHERE task_id = ?`, taskID,
	).Scan(&count); err != nil {
		t.Fatalf("count attempts: %v", err)
	}
	if count != 0 {
		t.Errorf("task_attempts rows = %d; want 0", count)
	}
}
