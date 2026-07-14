package store

import (
	"context"
	"fmt"
	"sync"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

// =====================================================================
// §9.5 invariant property-style scan for the full happy path.
// =====================================================================
//
// TestSqliteTaskAtomic_Invariant_TaskRunningInvActiveAttempt sweeps
// 8 (worker, lease) pairs through the canonical accept → terminate
// lifecycle and asserts §9.5 holds at every step: at no point may
// tasks have status RUNNING without a matching active attempt.

func TestSqliteTaskAtomic_Invariant_TaskRunningInvActiveAttempt(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	for i := 0; i < 8; i++ {
		taskID := fmt.Sprintf("T-sweep-%d", i)
		workerID := fmt.Sprintf("w-sweep-%d", i)
		leaseID := fmt.Sprintf("L-sweep-%d", i)

		seedLeasedTask(t, s.db, taskID, workerID, leaseID, "A-sweep-"+fmt.Sprint(i), 1, 0)
		attempt := &taskattempts.TaskAttempt{
			ID:     "A-sweep-" + fmt.Sprint(i),
			TaskID: taskID, JobID: "job-" + taskID,
			WorkerID: workerID, LeaseID: leaseID,
			AttemptNumber: 1, Status: taskattempts.AttemptStatusRunning,
		}
		if err := r.AcceptTaskAtomic(ctx, attempt, 0); err != nil {
			t.Fatalf("iteration %d: AcceptTaskAtomic: %v", i, err)
		}

		// Invariant check mid-flight: Task RUNNING ⇒ Attempt RUNNING.
		var taskStatus string
		if err := s.db.QueryRowContext(ctx,
			`SELECT status FROM tasks WHERE task_id = ?`,
			taskID).Scan(&taskStatus); err != nil {
			t.Fatalf("iteration %d: SELECT tasks.status: %v", i, err)
		}
		if taskStatus != "RUNNING" {
			t.Fatalf("iteration %d: tasks.status = %s; want RUNNING", i, taskStatus)
		}
		att := attemptForTask(t, s.db, taskID, workerID, leaseID)
		if att == nil {
			t.Fatalf("iteration %d: §9.5 BREACH — Task RUNNING but no active Attempt", i)
		}
		if att.Status != taskattempts.AttemptStatusRunning {
			t.Fatalf("iteration %d: §9.5 BREACH — Task RUNNING but Attempt status=%s", i, att.Status)
		}

		// Now terminate atomically.
		if err := r.TransitionTaskToTerminalAtomic(ctx, taskID, workerID, leaseID,
			taskgraph.StatusSucceeded, taskattempts.AttemptStatusSucceeded, "", ""); err != nil {
			t.Fatalf("iteration %d: TransitionTaskToTerminalAtomic: %v", i, err)
		}

		// Final invariant check: Task SUCCEEDED AND Attempt SUCCEEDED.
		if err := s.db.QueryRowContext(ctx,
			`SELECT status FROM tasks WHERE task_id = ?`,
			taskID).Scan(&taskStatus); err != nil {
			t.Fatalf("iteration %d: final SELECT tasks.status: %v", i, err)
		}
		if taskStatus != "SUCCEEDED" {
			t.Errorf("iteration %d: tasks.status = %s; want SUCCEEDED", i, taskStatus)
		}
		att = attemptForTask(t, s.db, taskID, workerID, leaseID)
		if att == nil || att.Status != taskattempts.AttemptStatusSucceeded {
			t.Errorf("iteration %d: final attempt wrong: %+v", i, att)
		}
	}
}

// TestSqliteTaskAtomic_Invariant_ConcurrentTerminalTransitionsIsSafe:
// 2 goroutines racing TransitionTaskToTerminalAtomic on the same task
// produce EXACTLY one SUCCEEDED Task, EXACTLY one SUCCEEDED attempt.
// The loser must NOT leave the attempt stranded in RUNNING.
func TestSqliteTaskAtomic_Invariant_ConcurrentTerminalTransitionsIsSafe(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	seedLeasedTask(t, s.db, "T-race", "w-race", "L-race", "A-race", 1, 0)
	attempt := &taskattempts.TaskAttempt{
		ID: "A-race", TaskID: "T-race", JobID: "job-T-race",
		WorkerID: "w-race", LeaseID: "L-race",
		AttemptNumber: 1, Status: taskattempts.AttemptStatusRunning,
	}
	if err := r.AcceptTaskAtomic(ctx, attempt, 0); err != nil {
		t.Fatalf("seed AcceptTaskAtomic: %v", err)
	}

	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := r.TransitionTaskToTerminalAtomic(ctx, "T-race", "w-race", "L-race",
				taskgraph.StatusSucceeded, taskattempts.AttemptStatusSucceeded, "", "")
			results <- err
		}()
	}
	wg.Wait()
	close(results)

	successes := 0
	var firstErr error
	for err := range results {
		if err == nil {
			successes++
		} else if firstErr == nil {
			firstErr = err
		}
	}
	if successes != 1 {
		t.Errorf("concurrent terminal transitions: successes=%d; want exactly 1 (first err=%v)",
			successes, firstErr)
	}
	// §9.5 invariant under contention: exactly one SUCCEEDED on tasks and
	// exactly one SUCCEEDED on attempts. Anything else is a §9.5 breach.
	var taskSucc int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM tasks WHERE task_id = ? AND status = 'SUCCEEDED'`,
		"T-race").Scan(&taskSucc); err != nil {
		t.Fatal(err)
	}
	if taskSucc != 1 {
		t.Errorf("SUCCEEDED tasks = %d; want 1 (§9.5)", taskSucc)
	}
	var attSucc int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM task_attempts
		 WHERE task_id = ? AND status = 'SUCCEEDED'`,
		"T-race").Scan(&attSucc); err != nil {
		t.Fatal(err)
	}
	if attSucc != 1 {
		t.Errorf("SUCCEEDED attempts = %d; want 1 (§9.5)", attSucc)
	}
}
