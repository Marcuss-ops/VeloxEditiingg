package store

import (
	"context"
	"errors"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/taskgraph"
)

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
