// Package workflow — tests covering 12 PR 9 DoD scenarios.
//
//  1. Linear A→B→C → run SUCCEEDED on terminal step.
//  2. Branching A→B/C → both activated in one transaction.
//  3. Join B+C→D → D flips READY only after both parents succeed.
//  4. Duplicate step_key → CreateRun errors.
//  5. Completion duplicata → second CompleteStep errors with "not in RUNNING".
//  6. Crash dopo step completion → MarkStepRunning then CompleteStep recovers.
//  7. Retry: Requeue=true and attempt < max_attempts flips back to READY.
//  8. Cancellazione: CancelRun flips run + remaining steps CANCELLED.
//  9. Due dispatcher concorrenti: parallel CompleteStepAndReleaseDependents.
// 10. Restart: same DB → GetRun/ListSteps restore prior state.
// 11. Job-failed → step retry path.
// 12. Invariant: CompleteStep when a non-terminal dep exists does NOT activate
//     dependents and does NOT flip run SUCCEEDED.
package workflow_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/workflow"
)

// ── Fixtures ────────────────────────────────────────────────────────────────

// newTestDB opens an in-memory SQLite DB and applies the workflow v2 schema
// (mirrors migration 027). SetMaxOpenConns(1) to avoid SQLITE_BUSY on
// concurrent writers.
func newTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open in-memory sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	for _, stmt := range workflowSchema {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("apply schema: %v\nstatement:\n%s", err, stmt)
		}
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// workflowSchema mirrors migration 027. Kept inline (vs. importing the
// migrations package) so the workflow package stays a leaf with no
// store dependency.
var workflowSchema = []string{
	`CREATE TABLE IF NOT EXISTS workflow_runs (
	    run_id             TEXT PRIMARY KEY,
	    workflow_type      TEXT NOT NULL,
	    status             TEXT NOT NULL,
	    input_json         TEXT NOT NULL DEFAULT '{}',
	    output_json        TEXT NOT NULL DEFAULT '{}',
	    revision           INTEGER NOT NULL DEFAULT 0,
	    created_at         TEXT NOT NULL,
	    updated_at         TEXT NOT NULL,
	    started_at         TEXT,
	    completed_at       TEXT,
	    last_error_code    TEXT,
	    last_error_message TEXT
	)`,
	`CREATE TABLE IF NOT EXISTS workflow_steps (
	    step_id        TEXT PRIMARY KEY,
	    run_id         TEXT NOT NULL,
	    step_key       TEXT NOT NULL,
	    job_id         TEXT,
	    status         TEXT NOT NULL,
	    attempt        INTEGER NOT NULL DEFAULT 0,
	    max_attempts   INTEGER NOT NULL DEFAULT 3,
	    input_json     TEXT NOT NULL DEFAULT '{}',
	    output_json    TEXT NOT NULL DEFAULT '{}',
	    revision       INTEGER NOT NULL DEFAULT 0,
	    created_at     TEXT NOT NULL,
	    updated_at     TEXT NOT NULL,
	    started_at     TEXT,
	    completed_at   TEXT,
	    error_code     TEXT,
	    error_message  TEXT,
	    UNIQUE(run_id, step_key),
	    UNIQUE(job_id)
	)`,
	`CREATE TABLE IF NOT EXISTS workflow_dependencies (
	    run_id              TEXT NOT NULL,
	    step_id             TEXT NOT NULL,
	    depends_on_step_id  TEXT NOT NULL,
	    PRIMARY KEY(run_id, step_id, depends_on_step_id)
	)`,
	`CREATE TABLE IF NOT EXISTS workflow_events (
	    event_id      TEXT PRIMARY KEY,
	    run_id        TEXT NOT NULL,
	    step_id       TEXT,
	    event_type    TEXT NOT NULL,
	    payload_json  TEXT NOT NULL DEFAULT '{}',
	    created_at    TEXT NOT NULL
	)`,
}

func newRepo(t *testing.T) *workflow.SQLiteRepository {
	t.Helper()
	return workflow.NewSQLiteRepository(newTestDB(t))
}

// stubOutboxWriter records every Enqueue call synchronously; tests assert
// the exact sequence + payloads produced by Repository methods.
type stubOutboxWriter struct {
	mu     sync.Mutex
	events []workflow.WorkflowOutboxEvent
}

func (s *stubOutboxWriter) Enqueue(ctx context.Context, ev workflow.WorkflowOutboxEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, ev)
	return nil
}

func (s *stubOutboxWriter) snapshot() []workflow.WorkflowOutboxEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]workflow.WorkflowOutboxEvent, len(s.events))
	copy(out, s.events)
	return out
}

// ── Topology helpers ────────────────────────────────────────────────────────

// stepOf returns the Step in run with the given StepKey (helper to look up
// steps after CreateRun since IDs are server-assigned).
func stepOf(t *testing.T, run *workflow.Run, steps []workflow.Step, key string) workflow.Step {
	t.Helper()
	if run == nil {
		t.Fatalf("run is nil")
	}
	for _, s := range steps {
		if s.StepKey == key {
			return s
		}
	}
	t.Fatalf("step %q not found in run %q (have %d steps)", key, run.RunID, len(steps))
	return workflow.Step{}
}

// markRunning is shorthand: takes a step to RUNNING with the given jobID.
func markRunning(t *testing.T, repo workflow.Repository, runID string, s workflow.Step, jobID string, attempt int) {
	t.Helper()
	if err := repo.MarkStepRunning(context.Background(), workflow.StartStep{
		RunID: runID, StepID: s.StepID, JobID: jobID, Attempt: attempt,
	}); err != nil {
		t.Fatalf("MarkStepRunning %s: %v", s.StepKey, err)
	}
}

// ── Scenario 1: Linear A → B → C ──────────────────────────────────────────

func TestWorkflow_LinearABC_RunSucceeds(t *testing.T) {
	repo := newRepo(t)
	ctx := context.Background()

	run, err := repo.CreateRun(ctx, workflow.WorkflowSpec{
		RunID:        "run-abc",
		WorkflowType: "linear",
		Input:        map[string]any{"n": 3},
		Steps: []workflow.WorkflowStepSpec{
			{StepKey: "A", Input: map[string]any{"task": "a"}},
			{StepKey: "B", Input: map[string]any{"task": "b"}, DependsOnKeys: []string{"A"}},
			{StepKey: "C", Input: map[string]any{"task": "c"}, DependsOnKeys: []string{"B"}},
		},
	})
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	steps, _ := repo.ListSteps(ctx, run.RunID)
	if steps[0].Status != workflow.StepStatusReady {
		t.Fatalf("A status = %q, want READY", steps[0].Status)
	}
	if steps[1].Status != workflow.StepStatusBlocked || steps[2].Status != workflow.StepStatusBlocked {
		t.Fatalf("B/C should start BLOCKED, got %q/%q", steps[1].Status, steps[2].Status)
	}

	// Drive A → B → C → run SUCCEEDED.
	markRunning(t, repo, run.RunID, stepOf(t, run, steps, "A"), "job-a", 1)
	if rp, err := repo.CompleteStepAndReleaseDependents(ctx, workflow.CompleteStep{
		RunID: run.RunID, StepID: stepOf(t, run, steps, "A").StepID,
		Output: map[string]any{"a": "ok"},
	}); err != nil || !containsKey(rp.Activated, "B") {
		t.Fatalf("Complete A: err=%v activated=%v", err, rp.Activated)
	}
	steps, _ = repo.ListSteps(ctx, run.RunID)

	markRunning(t, repo, run.RunID, stepOf(t, run, steps, "B"), "job-b", 1)
	rp, err := repo.CompleteStepAndReleaseDependents(ctx, workflow.CompleteStep{
		RunID: run.RunID, StepID: stepOf(t, run, steps, "B").StepID,
		Output: map[string]any{"b": "ok"},
	})
	if err != nil {
		t.Fatalf("Complete B: %v", err)
	}
	if !containsKey(rp.Activated, "C") {
		t.Fatalf("expected C activated after B, got %v", rp.Activated)
	}
	steps, _ = repo.ListSteps(ctx, run.RunID)

	markRunning(t, repo, run.RunID, stepOf(t, run, steps, "C"), "job-c", 1)
	rp, err = repo.CompleteStepAndReleaseDependents(ctx, workflow.CompleteStep{
		RunID: run.RunID, StepID: stepOf(t, run, steps, "C").StepID,
		Output: map[string]any{"c": "ok"},
	})
	if err != nil {
		t.Fatalf("Complete C: %v", err)
	}
	if !rp.Completed {
		t.Fatalf("Final CompleteStep did not set rp.Completed=true: %+v", rp.Run)
	}
	if rp.Run.Status != workflow.RunStatusSucceeded {
		t.Fatalf("run status = %q, want SUCCEEDED", rp.Run.Status)
	}
}

// ── Scenario 2: Branching A → B / C ────────────────────────────────────────

func TestWorkflow_BranchingAtoBC_BothActivatedAtomically(t *testing.T) {
	repo := newRepo(t)
	ctx := context.Background()

	run, err := repo.CreateRun(ctx, workflow.WorkflowSpec{
		RunID:        "run-branch",
		WorkflowType: "branch",
		Input:        map[string]any{},
		Steps: []workflow.WorkflowStepSpec{
			{StepKey: "A", Input: map[string]any{}},
			{StepKey: "B", DependsOnKeys: []string{"A"}},
			{StepKey: "C", DependsOnKeys: []string{"A"}},
		},
	})
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	steps, _ := repo.ListSteps(ctx, run.RunID)
	markRunning(t, repo, run.RunID, stepOf(t, run, steps, "A"), "jA", 1)
	rp, err := repo.CompleteStepAndReleaseDependents(ctx, workflow.CompleteStep{
		RunID: run.RunID, StepID: stepOf(t, run, steps, "A").StepID,
		Output: map[string]any{},
	})
	if err != nil {
		t.Fatalf("Complete A: %v", err)
	}
	if !containsKey(rp.Activated, "B") || !containsKey(rp.Activated, "C") {
		t.Fatalf("expected B+C activated, got %v", rp.Activated)
	}
	if rp.Completed {
		t.Fatalf("run should not be SUCCEEDED while B+C are READY (invariant #12)")
	}
}

// ── Scenario 3: Join B + C → D ─────────────────────────────────────────────

func TestWorkflow_JoinBCtoD_DependsOnBothParents(t *testing.T) {
	repo := newRepo(t)
	ctx := context.Background()

	run, err := repo.CreateRun(ctx, workflow.WorkflowSpec{
		RunID:        "run-join",
		WorkflowType: "diamond",
		Input:        map[string]any{},
		Steps: []workflow.WorkflowStepSpec{
			{StepKey: "A"},
			{StepKey: "B", DependsOnKeys: []string{"A"}},
			{StepKey: "C", DependsOnKeys: []string{"A"}},
			{StepKey: "D", DependsOnKeys: []string{"B", "C"}},
		},
	})
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	steps, _ := repo.ListSteps(ctx, run.RunID)
	a := stepOf(t, run, steps, "A")
	b := stepOf(t, run, steps, "B")
	c := stepOf(t, run, steps, "C")
	_ = stepOf(t, run, steps, "D") // D is asserted below by re-listing steps

	// Complete A → B,C READY. D stays BLOCKED.
	markRunning(t, repo, run.RunID, a, "jA", 1)
	if _, err := repo.CompleteStepAndReleaseDependents(ctx, workflow.CompleteStep{
		RunID: run.RunID, StepID: a.StepID,
	}); err != nil {
		t.Fatalf("Complete A: %v", err)
	}
	steps, _ = repo.ListSteps(ctx, run.RunID)
	if stepOf(t, run, steps, "D").Status != workflow.StepStatusBlocked {
		t.Fatalf("after A, D should still be BLOCKED")
	}

	// Complete B → C still READY, D still BLOCKED (B done but C not yet).
	markRunning(t, repo, run.RunID, b, "jB", 1)
	if _, err := repo.CompleteStepAndReleaseDependents(ctx, workflow.CompleteStep{
		RunID: run.RunID, StepID: b.StepID,
	}); err != nil {
		t.Fatalf("Complete B: %v", err)
	}
	steps, _ = repo.ListSteps(ctx, run.RunID)
	if stepOf(t, run, steps, "D").Status != workflow.StepStatusBlocked {
		t.Fatalf("after B only, D must still be BLOCKED")
	}

	// Complete C → D flips to READY.
	markRunning(t, repo, run.RunID, c, "jC", 1)
	rp, err := repo.CompleteStepAndReleaseDependents(ctx, workflow.CompleteStep{
		RunID: run.RunID, StepID: c.StepID,
	})
	if err != nil {
		t.Fatalf("Complete C: %v", err)
	}
	if !containsKey(rp.Activated, "D") {
		t.Fatalf("expected D activated after C, got %v", rp.Activated)
	}
}

// ── Scenario 4: Duplicate step_key → error ─────────────────────────────────

func TestWorkflow_DuplicateStep_Error(t *testing.T) {
	repo := newRepo(t)
	_, err := repo.CreateRun(context.Background(), workflow.WorkflowSpec{
		RunID:        "run-dup",
		WorkflowType: "dup",
		Steps: []workflow.WorkflowStepSpec{
			{StepKey: "A"},
			{StepKey: "A"},
		},
	})
	if err == nil {
		t.Fatalf("expected duplicate step_key error")
	}
	if !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("expected 'duplicate' error, got: %v", err)
	}
}

// ── Scenario 5: Completion duplicata ───────────────────────────────────────

func TestWorkflow_CompletionDuplicata_SecondCompletesError(t *testing.T) {
	repo := newRepo(t)
	ctx := context.Background()
	run, _ := repo.CreateRun(ctx, workflow.WorkflowSpec{
		RunID: "run-double", WorkflowType: "double", Steps: []workflow.WorkflowStepSpec{
			{StepKey: "A"}, {StepKey: "B", DependsOnKeys: []string{"A"}},
		},
	})
	steps, _ := repo.ListSteps(ctx, run.RunID)
	a := stepOf(t, run, steps, "A")
	markRunning(t, repo, run.RunID, a, "jA", 1)

	if _, err := repo.CompleteStepAndReleaseDependents(ctx, workflow.CompleteStep{
		RunID: run.RunID, StepID: a.StepID, Output: map[string]any{},
	}); err != nil {
		t.Fatalf("first Complete A: %v", err)
	}

	// Second complete on the same step: SQL update has WHERE status='RUNNING'
	// so it matches zero rows and the helper returns an error.
	_, err := repo.CompleteStepAndReleaseDependents(ctx, workflow.CompleteStep{
		RunID: run.RunID, StepID: a.StepID, Output: map[string]any{},
	})
	if err == nil {
		t.Fatalf("second Complete A should error")
	}
	if !strings.Contains(err.Error(), "not in RUNNING") {
		t.Fatalf("got error %v, want one containing 'not in RUNNING'", err)
	}
}

// ── Scenario 6: Crash dopo step completion (recovery) ──────────────────────

func TestWorkflow_CrashMidCompletion_Recovery(t *testing.T) {
	// We can't actually crash inside the repo, but we test the recovery
	// invariant: a step that is left RUNNING because the handler crashed
	// after writing the SUCCESS response is re-completable on retry.
	repo := newRepo(t)
	ctx := context.Background()
	run, _ := repo.CreateRun(ctx, workflow.WorkflowSpec{
		RunID: "run-crash", WorkflowType: "crash",
		Steps: []workflow.WorkflowStepSpec{
			{StepKey: "A"}, {StepKey: "B", DependsOnKeys: []string{"A"}},
		},
	})
	steps, _ := repo.ListSteps(ctx, run.RunID)
	a := stepOf(t, run, steps, "A")
	markRunning(t, repo, run.RunID, a, "jA", 1)

	// The "crash" point: handler returned success but the response didn't
	// reach the master (or the commit failed). The step is still RUNNING.
	steps, _ = repo.ListSteps(ctx, run.RunID)
	if stepOf(t, run, steps, "A").Status != workflow.StepStatusRunning {
		t.Fatalf("pre-crash A status = %q, want RUNNING", stepOf(t, run, steps, "A").Status)
	}

	// Recovery: re-call CompleteStep. The SQL UPDATE flips RUNNING →
	// SUCCEEDED with the new output.
	rp, err := repo.CompleteStepAndReleaseDependents(ctx, workflow.CompleteStep{
		RunID: run.RunID, StepID: a.StepID, Output: map[string]any{"recovered": true},
	})
	if err != nil {
		t.Fatalf("recovery Complete A: %v", err)
	}
	if !containsKey(rp.Activated, "B") {
		t.Fatalf("expected B activated after recovery")
	}
}

// ── Scenario 7: Retry via FailStep{Requeue: true} ──────────────────────────

func TestWorkflow_RetryRequeue_FlipsToReady(t *testing.T) {
	repo := newRepo(t)
	ctx := context.Background()
	run, _ := repo.CreateRun(ctx, workflow.WorkflowSpec{
		RunID: "run-retry", WorkflowType: "retry",
		Steps: []workflow.WorkflowStepSpec{
			{StepKey: "A", MaxAttempts: 3},
		},
	})
	steps, _ := repo.ListSteps(ctx, run.RunID)
	a := stepOf(t, run, steps, "A")

	markRunning(t, repo, run.RunID, a, "jA-1", 1)
	rp, err := repo.FailStep(ctx, workflow.FailStep{
		RunID: run.RunID, StepID: a.StepID,
		ErrorCode: "transient", ErrorMessage: "boom",
		Attempt: 1, Requeue: true,
	})
	if err != nil {
		t.Fatalf("FailStep requeue: %v", err)
	}
	steps, _ = repo.ListSteps(ctx, run.RunID)
	if stepOf(t, run, steps, "A").Status != workflow.StepStatusReady {
		t.Fatalf("after Requeue, A status = %q, want READY", stepOf(t, run, steps, "A").Status)
	}
	// Run must NOT be FAILED — only one step is retrying.
	if rp.Run.Status == workflow.RunStatusFailed {
		t.Fatalf("run flipped to FAILED on retryable failure: %+v", rp.Run)
	}

	// Second attempt succeeds.
	markRunning(t, repo, run.RunID, stepOf(t, run, steps, "A"), "jA-2", 2)
	rp, err = repo.CompleteStepAndReleaseDependents(ctx, workflow.CompleteStep{
		RunID: run.RunID, StepID: stepOf(t, run, steps, "A").StepID,
		Output: map[string]any{"done": true}, Attempt: 2,
	})
	if err != nil {
		t.Fatalf("CompleteStep 2nd attempt: %v", err)
	}
	if !rp.Completed {
		t.Fatalf("run should be SUCCEEDED after A retries")
	}
}

// ── Scenario 8: Cancellazione ───────────────────────────────────────────────

func TestWorkflow_Cancellazione_FlipsAllStepsTerminal(t *testing.T) {
	repo := newRepo(t)
	ctx := context.Background()
	run, _ := repo.CreateRun(ctx, workflow.WorkflowSpec{
		RunID: "run-cancel", WorkflowType: "cancel",
		Steps: []workflow.WorkflowStepSpec{
			{StepKey: "A"}, {StepKey: "B", DependsOnKeys: []string{"A"}},
		},
	})
	steps, _ := repo.ListSteps(ctx, run.RunID)
	a := stepOf(t, run, steps, "A")
	markRunning(t, repo, run.RunID, a, "jA", 1)

	if err := repo.CancelRun(ctx, run.RunID); err != nil {
		t.Fatalf("CancelRun: %v", err)
	}
	r, _ := repo.GetRun(ctx, run.RunID)
	if r.Status != workflow.RunStatusCancelled {
		t.Fatalf("run status = %q, want CANCELLED", r.Status)
	}
	steps, _ = repo.ListSteps(ctx, run.RunID)
	for _, s := range steps {
		if string(s.Status) != "CANCELLED" {
			t.Fatalf("step %q status = %q, want CANCELLED", s.StepKey, s.Status)
		}
	}

	// Idempotent.
	if err := repo.CancelRun(ctx, run.RunID); err != nil {
		t.Fatalf("CancelRun idempotent should not error: %v", err)
	}
}

// ── Scenario 9: Due dispatcher concorrenti ──────────────────────────────────

func TestWorkflow_TwoDispatchers_FinalStateConsistent(t *testing.T) {
	repo := newRepo(t)
	ctx := context.Background()
	run, _ := repo.CreateRun(ctx, workflow.WorkflowSpec{
		RunID: "run-concurrent", WorkflowType: "concurrent",
		Steps: []workflow.WorkflowStepSpec{
			{StepKey: "A"},
			{StepKey: "B", DependsOnKeys: []string{"A"}},
			{StepKey: "C", DependsOnKeys: []string{"A"}},
			{StepKey: "D", DependsOnKeys: []string{"B", "C"}},
		},
	})
	steps, _ := repo.ListSteps(ctx, run.RunID)

	a := stepOf(t, run, steps, "A")
	markRunning(t, repo, run.RunID, a, "jA", 1)
	if _, err := repo.CompleteStepAndReleaseDependents(ctx, workflow.CompleteStep{
		RunID: run.RunID, StepID: a.StepID,
	}); err != nil {
		t.Fatalf("Complete A: %v", err)
	}
	steps, _ = repo.ListSteps(ctx, run.RunID)
	b := stepOf(t, run, steps, "B")
	c := stepOf(t, run, steps, "C")
	_ = stepOf(t, run, steps, "D") // D is asserted below by re-listing steps
	markRunning(t, repo, run.RunID, b, "jB", 1)
	markRunning(t, repo, run.RunID, c, "jC", 1)

	// Two dispatchers race to complete B and C. SetMaxOpenConns(1) on the
	// shared in-memory DB serialises writes — both succeed in some order.
	var wg sync.WaitGroup
	var errB, errC error
	wg.Add(2)
	go func() { defer wg.Done(); _, errB = repo.CompleteStepAndReleaseDependents(ctx, workflow.CompleteStep{RunID: run.RunID, StepID: b.StepID}) }()
	go func() { defer wg.Done(); _, errC = repo.CompleteStepAndReleaseDependents(ctx, workflow.CompleteStep{RunID: run.RunID, StepID: c.StepID}) }()
	wg.Wait()
	if errB != nil {
		t.Fatalf("Complete B: %v", errB)
	}
	if errC != nil {
		t.Fatalf("Complete C: %v", errC)
	}

	// Final state: A,B,C SUCCEEDED; D READY (only one of B/C completed
	// will activate D, but the other has already SUCCEEDED so on the
	// second CompleteStep's invariant check, D is still BLOCKED until both
	// are SUCCEEDED. So after both goroutines, D MUST be READY.)
	steps, _ = repo.ListSteps(ctx, run.RunID)
	for _, s := range steps {
		switch s.StepKey {
		case "A", "B", "C":
			if s.Status != workflow.StepStatusSucceeded {
				t.Fatalf("step %q status = %q, want SUCCEEDED", s.StepKey, s.Status)
			}
		case "D":
			if s.Status != workflow.StepStatusReady {
				t.Fatalf("step D status = %q, want READY", s.Status)
			}
		}
	}
}

// ── Scenario 10: Restart (re-create Repository from same DB) ───────────────

func TestWorkflow_Restart_StatePersists(t *testing.T) {
	db := newTestDB(t)
	repo1 := workflow.NewSQLiteRepository(db)
	ctx := context.Background()
	run, err := repo1.CreateRun(ctx, workflow.WorkflowSpec{
		RunID: "run-restart", WorkflowType: "restart",
		Steps: []workflow.WorkflowStepSpec{
			{StepKey: "A"},
		},
	})
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	// Simulate process restart: drop repo1, build repo2 from same DB.
	repo2 := workflow.NewSQLiteRepository(db)

	got, _ := repo2.GetRun(ctx, run.RunID)
	if got == nil || got.RunID != run.RunID {
		t.Fatalf("post-restart GetRun = %v, want %q", got, run.RunID)
	}
	steps, _ := repo2.ListSteps(ctx, run.RunID)
	if len(steps) != 1 || steps[0].StepKey != "A" {
		t.Fatalf("post-restart ListSteps wrong: %+v", steps)
	}
}

// ── Scenario 11: Job-failed → step retry → run SUCCEEDED ───────────────────

func TestWorkflow_JobFailedStepRetry_FinalRunSucceeded(t *testing.T) {
	repo := newRepo(t)
	ctx := context.Background()
	run, _ := repo.CreateRun(ctx, workflow.WorkflowSpec{
		RunID: "run-job-fail", WorkflowType: "job-fail",
		Steps: []workflow.WorkflowStepSpec{
			{StepKey: "A", MaxAttempts: 3},
			{StepKey: "B", DependsOnKeys: []string{"A"}},
		},
	})
	steps, _ := repo.ListSteps(ctx, run.RunID)
	a := stepOf(t, run, steps, "A")

	// Job fails on its first attempt → FailStep{Requeue: true}.
	markRunning(t, repo, run.RunID, a, "job-a-att-1", 1)
	if _, err := repo.FailStep(ctx, workflow.FailStep{
		RunID: run.RunID, StepID: a.StepID,
		ErrorCode: "JobFail", ErrorMessage: "worker crashed",
		Attempt: 1, Requeue: true,
	}); err != nil {
		t.Fatalf("FailStep: %v", err)
	}
	steps, _ = repo.ListSteps(ctx, run.RunID)
	if stepOf(t, run, steps, "A").Status != workflow.StepStatusReady {
		t.Fatalf("after requeue, A status = %q, want READY", stepOf(t, run, steps, "A").Status)
	}

	// Second attempt completes.
	markRunning(t, repo, run.RunID, stepOf(t, run, steps, "A"), "job-a-att-2", 2)
	rp, err := repo.CompleteStepAndReleaseDependents(ctx, workflow.CompleteStep{
		RunID: run.RunID, StepID: stepOf(t, run, steps, "A").StepID,
		Attempt: 2, Output: map[string]any{"ok": true},
	})
	if err != nil {
		t.Fatalf("CompleteStep 2nd attempt: %v", err)
	}
	if !rp.Completed {
		t.Fatalf("run should be SUCCEEDED after retry")
	}
}

// ── Scenario 12: Invariant — non-terminal step prevents run SUCCEEDED ─────

func TestWorkflow_Invariant_NonTerminalStepPreventsRunSucceeded(t *testing.T) {
	// A run with two unrelated chains: A→B (will succeed) and Z (left in
	// READY forever, never dispatched). Completing B must NOT flip the
	// run to SUCCEEDED while Z remains non-terminal.
	repo := newRepo(t)
	ctx := context.Background()
	run, _ := repo.CreateRun(ctx, workflow.WorkflowSpec{
		RunID: "run-invariant", WorkflowType: "invariant",
		Steps: []workflow.WorkflowStepSpec{
			{StepKey: "A"},
			{StepKey: "B", DependsOnKeys: []string{"A"}},
			{StepKey: "Z"}, // independent chain, must remain non-terminal
		},
	})
	steps, _ := repo.ListSteps(ctx, run.RunID)
	a := stepOf(t, run, steps, "A")
	b := stepOf(t, run, steps, "B")

	markRunning(t, repo, run.RunID, a, "jA", 1)
	if _, err := repo.CompleteStepAndReleaseDependents(ctx, workflow.CompleteStep{
		RunID: run.RunID, StepID: a.StepID,
	}); err != nil {
		t.Fatalf("Complete A: %v", err)
	}
	markRunning(t, repo, run.RunID, b, "jB", 1)
	rp, err := repo.CompleteStepAndReleaseDependents(ctx, workflow.CompleteStep{
		RunID: run.RunID, StepID: b.StepID,
	})
	if err != nil {
		t.Fatalf("Complete B: %v", err)
	}
	if rp.Completed {
		t.Fatalf("run should NOT be SUCCEEDED while Z is non-terminal (READY)")
	}
	if rp.Run.Status == workflow.RunStatusSucceeded {
		t.Fatalf("run status = %q, want non-SUCCEEDED", rp.Run.Status)
	}
}

// ── Bonus: outbox writer integration ───────────────────────────────────────

func TestWorkflow_OutboxWriter_EmitsEvents(t *testing.T) {
	db := newTestDB(t)
	repo := workflow.NewSQLiteRepository(db)
	w := &stubOutboxWriter{}
	repo.SetOutbox(w)

	ctx := context.Background()
	run, _ := repo.CreateRun(ctx, workflow.WorkflowSpec{
		RunID:        "run-outbox",
		WorkflowType: "outbox",
		Steps: []workflow.WorkflowStepSpec{
			{StepKey: "A"}, {StepKey: "B", DependsOnKeys: []string{"A"}},
		},
	})
	steps, _ := repo.ListSteps(ctx, run.RunID)
	a := stepOf(t, run, steps, "A")

	evs := w.snapshot()
	if len(evs) != 1 || evs[0].EventType != "WORKFLOW_STEP_READY" {
		t.Fatalf("after CreateRun expected one WORKFLOW_STEP_READY, got %v", evs)
	}

	markRunning(t, repo, run.RunID, a, "jA", 1)
	_, _ = repo.CompleteStepAndReleaseDependents(ctx, workflow.CompleteStep{
		RunID: run.RunID, StepID: a.StepID, Attempt: 1,
	})
	evs = w.snapshot()
	// One WORKFLOW_STEP_SUCCEEDED + one WORKFLOW_STEP_READY for B.
	var types []string
	for _, e := range evs {
		types = append(types, e.EventType)
	}
	if !containsKey(types, "WORKFLOW_STEP_SUCCEEDED") {
		t.Fatalf("expected WORKFLOW_STEP_SUCCEEDED, got %v", types)
	}
	count := 0
	for _, e := range types {
		if e == "WORKFLOW_STEP_READY" {
			count++
		}
	}
	if count != 2 {
		t.Fatalf("expected 2 WORKFLOW_STEP_READY (A at start + B after A completes), got %d (%v)", count, types)
	}
}

// ── Bonus: GetStepByJobID inverse lookup ──────────────────────────────────

func TestWorkflow_GetStepByJobID_OK(t *testing.T) {
	repo := newRepo(t)
	ctx := context.Background()
	run, _ := repo.CreateRun(ctx, workflow.WorkflowSpec{
		RunID: "run-getbj", WorkflowType: "getbj",
		Steps: []workflow.WorkflowStepSpec{{StepKey: "A"}},
	})
	steps, _ := repo.ListSteps(ctx, run.RunID)
	a := stepOf(t, run, steps, "A")
	markRunning(t, repo, run.RunID, a, "job-xyz-123", 1)

	got, runID, err := repo.GetStepByJobID(ctx, "job-xyz-123")
	if err != nil {
		t.Fatalf("GetStepByJobID: %v", err)
	}
	if got == nil {
		t.Fatalf("expected step, got nil")
	}
	if runID != run.RunID {
		t.Fatalf("runID = %q, want %q", runID, run.RunID)
	}
	if got.JobID == nil || *got.JobID != "job-xyz-123" {
		t.Fatalf("step.JobID = %v, want %q", got.JobID, "job-xyz-123")
	}

	// Unknown job_id → (nil, "", nil)
	_, runID2, err := repo.GetStepByJobID(ctx, "missing-job")
	if err != nil {
		t.Fatalf("missing: %v", err)
	}
	if runID2 != "" {
		t.Fatalf("missing runID = %q, want empty", runID2)
	}
}

// ── Bonus: Migrate legacy MultiStepJob → WorkflowSpec ──────────────────────

func TestWorkflow_Migrate_LegacyMultiStepJob_RoundTrip(t *testing.T) {
	repo := newRepo(t)
	raw := []byte(`{
		"job_id": "leg-1",
		"pipeline_type": "yt",
		"status": "RUNNING",
		"steps": [
			{"step_id":"s1","step_name":"script","step_order":0,"status":"SUCCEEDED","job_type":"generate_script","payload":{"topic":"X"},"dependencies":[],"retry_count":0,"max_retries":2},
			{"step_id":"s2","step_name":"render","step_order":1,"status":"READY","job_type":"render_video","payload":{},"dependencies":["s1"],"retry_count":0,"max_retries":2}
		]
	}`)

	res, err := workflow.Apply(context.Background(), repo, [][]byte{raw})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if res.RunsMigrated != 1 {
		t.Fatalf("RunsMigrated = %d, want 1 (res=%+v)", res.RunsMigrated, res)
	}
	if res.StepsMigrated != 2 {
		t.Fatalf("StepsMigrated = %d, want 2", res.StepsMigrated)
	}
	if res.InvalidRuns != 0 || res.InvalidSteps != 0 {
		t.Fatalf("unexpected invalid counts: %+v", res)
	}

	run, _ := repo.GetRun(context.Background(), "leg-1")
	if run == nil {
		t.Fatalf("GetRun after Apply: nil")
	}
	if run.WorkflowType != "yt" {
		t.Fatalf("WorkflowType = %q, want yt", run.WorkflowType)
	}

	steps, _ := repo.ListSteps(context.Background(), "leg-1")
	if len(steps) != 2 {
		t.Fatalf("ListSteps len = %d, want 2", len(steps))
	}
	// The v2 migrator drops the legacy step status field (it only
	// records DAG topology + retry metadata). Steps with no deps start
	// as READY regardless of what the legacy blob claimed.
	if stepOf(t, run, steps, "script").Status != workflow.StepStatusReady {
		t.Fatalf("script status = %q, want READY (no deps, v2 initial)", stepOf(t, run, steps, "script").Status)
	}
	if stepOf(t, run, steps, "render").Status != workflow.StepStatusBlocked {
		t.Fatalf("render status = %q, want BLOCKED (depends on script)", stepOf(t, run, steps, "render").Status)
	}
}

// ── Helpers ─────────────────────────────────────────────────────────────────

func containsKey(keys []string, want string) bool {
	for _, k := range keys {
		if k == want {
			return true
		}
	}
	return false
}
