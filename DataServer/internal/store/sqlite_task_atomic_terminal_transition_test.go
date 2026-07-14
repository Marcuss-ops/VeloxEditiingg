package store

import (
	"context"
	"errors"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

// =====================================================================
// TransitionTaskToTerminalAtomic tests
// =====================================================================
//
// TransitionTaskToTerminalAtomic marks a Task AND its matching active
// TaskAttempt as terminal (SUCCEEDED / FAILED / CANCELLED) in a SINGLE
// transaction. Idempotency at the Task-CAS layer (not row layer): the
// second call after a successful terminal returns ErrTransitionConflict.
// The §9.5 invariant guard below rejects an attempted terminal on a
// Task whose attempt row is missing — refusing to deepen the desync
// from "Task RUNNING + no attempt" into "Task terminal + no attempt".

// TestTransitionTaskToTerminalAtomic_HappyPath: Task RUNNING + active
// attempt RUNNING ⇒ both terminal in one commit.
func TestTransitionTaskToTerminalAtomic_HappyPath(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	seedLeasedTask(t, s.db, "T-term-1", "w-1", "L-1", "A-term-1", 1, 0)
	attempt := &taskattempts.TaskAttempt{
		ID:            "A-term-1",
		TaskID:        "T-term-1",
		JobID:         "job-T-term-1",
		WorkerID:      "w-1",
		LeaseID:       "L-1",
		AttemptNumber: 1,
		Status:        taskattempts.AttemptStatusRunning,
	}
	if err := r.AcceptTaskAtomic(ctx, attempt, 0); err != nil {
		t.Fatalf("seed AcceptTaskAtomic: %v", err)
	}

	if err := r.TransitionTaskToTerminalAtomic(ctx, "T-term-1", "w-1", "L-1",
		taskgraph.StatusSucceeded, taskattempts.AttemptStatusSucceeded, "", ""); err != nil {
		t.Fatalf("TransitionTaskToTerminalAtomic happy path: %v", err)
	}

	var taskStatus, taskCompletedAt string
	if err := s.db.QueryRowContext(ctx,
		`SELECT status, COALESCE(completed_at, '') FROM tasks WHERE task_id = ?`,
		"T-term-1").Scan(&taskStatus, &taskCompletedAt); err != nil {
		t.Fatalf("post-terminal SELECT tasks: %v", err)
	}
	if taskStatus != "SUCCEEDED" {
		t.Errorf("tasks.status = %s; want SUCCEEDED", taskStatus)
	}
	if taskCompletedAt == "" {
		t.Errorf("tasks.completed_at empty; want RFC3339 timestamp")
	}

	// §9.5 invariant (positive case): Task SUCCEEDED AND attempt SUCCEEDED.
	att := attemptForTask(t, s.db, "T-term-1", "w-1", "L-1")
	if att == nil {
		t.Fatal("attempt missing after TransitionTaskToTerminalAtomic")
	}
	if att.Status != taskattempts.AttemptStatusSucceeded {
		t.Errorf("task_attempts.status = %s; want SUCCEEDED", att.Status)
	}
}

// TestTransitionTaskToTerminalAtomic_IdempotentRetry: a second call
// after the first succeeds (Task terminal) must return
// ErrTransitionConflict (Task CAS did not match), not silently
// double-promote. The replay-safe short-circuit on attempt-already-
// terminal only fires if the Task CAS still succeeded; here Task is
// already SUCCEEDED so the second call is a clean refusal.
func TestTransitionTaskToTerminalAtomic_IdempotentRetry(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	seedLeasedTask(t, s.db, "T-term-2", "w-2", "L-2", "A-term-2", 1, 0)
	attempt := &taskattempts.TaskAttempt{
		ID: "A-term-2", TaskID: "T-term-2", JobID: "job-T-term-2",
		WorkerID: "w-2", LeaseID: "L-2",
		AttemptNumber: 1, Status: taskattempts.AttemptStatusRunning,
	}
	if err := r.AcceptTaskAtomic(ctx, attempt, 0); err != nil {
		t.Fatalf("seed AcceptTaskAtomic: %v", err)
	}

	// First terminal transition.
	if err := r.TransitionTaskToTerminalAtomic(ctx, "T-term-2", "w-2", "L-2",
		taskgraph.StatusSucceeded, taskattempts.AttemptStatusSucceeded, "", ""); err != nil {
		t.Fatalf("first TransitionTaskToTerminalAtomic: %v", err)
	}
	// Second transition (worker retry): Task CAS fails (already SUCCEEDED)
	// ⇒ ErrTransitionConflict. This is the correct semantic: idempotency
	// at the Task-CAS layer, not at the row layer.
	err := r.TransitionTaskToTerminalAtomic(ctx, "T-term-2", "w-2", "L-2",
		taskgraph.StatusSucceeded, taskattempts.AttemptStatusSucceeded, "", "")
	if err == nil {
		t.Fatal("second call expected ErrTransitionConflict (Task already terminal)")
	}
	if !errors.Is(err, taskgraph.ErrTransitionConflict) {
		t.Errorf("second call expected ErrTransitionConflict; got %v", err)
	}

	// Verify state unchanged.
	var taskStatus string
	if err := s.db.QueryRowContext(ctx,
		`SELECT status FROM tasks WHERE task_id = ?`,
		"T-term-2").Scan(&taskStatus); err != nil {
		t.Fatal(err)
	}
	if taskStatus != "SUCCEEDED" {
		t.Errorf("tasks.status = %s; want SUCCEEDED (no drift)", taskStatus)
	}
}

// TestTransitionTaskToTerminalAtomic_RollsBackOnMissingAttempt is THE
// §9.5 invariant guard: a Task RUNNING row that has no matching attempt
// (out-of-band desync, e.g. handleTaskAccepted attempted Create but
// crashed) MUST NOT be allowed to deepen the breach by also transitioning
// the Task to terminal. The method rolls back with ErrStaleReport and
// leaves the Task row at RUNNING so an operator can inspect it.
func TestTransitionTaskToTerminalAtomic_RollsBackOnMissingAttempt(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	seedRunningTask(t, s.db, "T-orphan-no-attempt", "w-orph", "L-orph")

	err := r.TransitionTaskToTerminalAtomic(ctx, "T-orphan-no-attempt", "w-orph", "L-orph",
		taskgraph.StatusSucceeded, taskattempts.AttemptStatusSucceeded, "", "")
	if err == nil {
		t.Fatal("expected ErrStaleReport when attempt row missing; got nil")
	}
	if !errors.Is(err, taskattempts.ErrStaleReport) {
		t.Errorf("expected taskattempts.ErrStaleReport; got %v", err)
	}
	if !strings.Contains(err.Error(), "§9.5 invariant") {
		t.Errorf("error message should reference §9.5 invariant guard; got %q", err.Error())
	}

	// §9.5 preservation: Task stayed at RUNNING even though caller asked
	// for SUCCEEDED. The desync was already present, but the guard
	// refuses to deepen it.
	var taskStatus string
	if err := s.db.QueryRowContext(ctx,
		`SELECT status FROM tasks WHERE task_id = ?`,
		"T-orphan-no-attempt").Scan(&taskStatus); err != nil {
		t.Fatal(err)
	}
	if taskStatus != "RUNNING" {
		t.Errorf("tasks.status = %s; want RUNNING (rollback preserved §9.5 state)", taskStatus)
	}
}

// TestTransitionTaskToTerminalAtomic_RejectsTerminalStatusInput guards
// the precondition that callers must supply a terminal target status.
func TestTransitionTaskToTerminalAtomic_RejectsTerminalStatusInput(t *testing.T) {
	_, r := openTaskAtomicTestDB(t)
	ctx := context.Background()
	err := r.TransitionTaskToTerminalAtomic(ctx, "T-bad-input", "w", "L",
		taskgraph.StatusRunning, taskattempts.AttemptStatusSucceeded, "", "")
	if err == nil {
		t.Fatal("expected validation error for non-terminal task status; got nil")
	}
	if !strings.Contains(err.Error(), "terminal task status") {
		t.Errorf("error should mention terminal task status; got %q", err.Error())
	}
}
