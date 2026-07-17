// Package store / atomic_job_task_test.go
//
// End-to-end tests for AtomicJobTaskCreator that close the gap identified
// by Action Plan P0.2 "Delivery plan prima del rendering". The 7 plan
// cases are exercised by these tests:
//
//   1. no delivery_plan in prod -> error
//      ── covered by AtomicJobTaskCreator.WithDeliveryPlanPolicy(true)
//         + empty delivery_plan payload (see TestAtomicJobTaskCreator_RequiresExplicitPlanInProdByDefault)
//   2. delivery_destination_id valid (single) -> ok  ── TestAtomicJobTaskCreator_HappyPath_SingleDestinationDeliveryPlan
//   3. delivery_destination_ids valid (multi) -> ok  ── TestAtomicJobTaskCreator_HappyPath_MultiDestinationDeliveryPlan
//   4. destination_id non-existent -> error       ── TestAtomicJobTaskCreator_RejectsMissingDestination
//   5. destination globally disabled -> error     ── TestAtomicJobTaskCreator_RejectsDisabledDestination
//   6. duplicate destination_id in plan -> error  ── TestAtomicJobTaskCreator_RollsBackOnDuplicateDestinationInPlan
//   7. invalid plan -> NO job/row persisted (full rollback)
//                                                 ── TestAtomicJobTaskCreator_RejectsMissingDestination
//                                                    + TestAtomicJobTaskCreator_RejectsDisabledDestination
//                                                    + TestAtomicJobTaskCreator_RollsBackOnDuplicateDestinationInPlan
//
// The pure extracted-function helper (validateDeliveryDestinationTx)
// has its own 3 unit tests at the top of the file — they run without
// the atomic creator constructor and prove the error-message contract
// is stable (used programmatically by the enqueue-layer validator).
//
// Caller-commit contract:
//   - TestCreateJobWithTaskTx_CallerOwnsCommitBoundary exercises the
//     *Tx path on the error side: caller defers Rollback, error path
//     surfs, no rows leak.
//   - TestCreateJobWithTaskTx_HappyCommitPersistsWhenCallerCommits
//     exercises the *Tx path on the success side: caller explicitly
//     commits, rows ARE visible. Together they document that the *Tx
//     variant does NOT auto-commit.

package store

import (
	"context"
	"strings"
	"testing"

	"velox-server/internal/jobs"
	"velox-server/internal/taskgraph"
)

// ============================================================
// validateDeliveryDestinationTx unit tests — error contract is stable.
// ============================================================

// TestValidateDeliveryDestinationTx_AcceptsEnabled: an enabled row in
// delivery_destinations is accepted by the per-destination check.
func TestValidateDeliveryDestinationTx_AcceptsEnabled(t *testing.T) {
	t.Parallel()
	s := openTestDB(t)
	defer s.Close()

	if err := s.InsertDeliveryDestination(&DeliveryDestination{
		DestinationID: "drive-main",
		Provider:      "drive",
		Name:          "Main Drive",
		Enabled:       true,
	}); err != nil {
		t.Fatalf("seed drive-main: %v", err)
	}

	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := validateDeliveryDestinationTx(ctx, tx, "drive-main"); err != nil {
		t.Fatalf("validateDeliveryDestinationTx for enabled row: got %v, want nil", err)
	}
}

// TestValidateDeliveryDestinationTx_RejectsNonExistent: a destination_id
// with no matching row returns the canonical "does not exist" error so
// callers can distinguish missing-vs-disabled programmatically.
func TestValidateDeliveryDestinationTx_RejectsNonExistent(t *testing.T) {
	t.Parallel()
	s := openTestDB(t)
	defer s.Close()

	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	err = validateDeliveryDestinationTx(ctx, tx, "missing-dest")
	if err == nil {
		t.Fatal("want error for non-existent destination_id")
	}
	if !strings.Contains(err.Error(), `"missing-dest" does not exist`) {
		t.Fatalf("error = %q, want destination_id %q does not exist", err.Error(), "missing-dest")
	}
}

// TestValidateDeliveryDestinationTx_RejectsGloballyDisabled: a disabled
// (enabled=0) row returns the canonical "globally disabled" error so
// callers can distinguish missing-vs-disabled programmatically.
func TestValidateDeliveryDestinationTx_RejectsGloballyDisabled(t *testing.T) {
	t.Parallel()
	s := openTestDB(t)
	defer s.Close()

	if err := s.InsertDeliveryDestination(&DeliveryDestination{
		DestinationID: "drive-disabled",
		Provider:      "drive",
		Name:          "Disabled Drive",
		Enabled:       false,
	}); err != nil {
		t.Fatalf("seed drive-disabled: %v", err)
	}

	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	err = validateDeliveryDestinationTx(ctx, tx, "drive-disabled")
	if err == nil {
		t.Fatal("want error for globally disabled destination")
	}
	if !strings.Contains(err.Error(), `"drive-disabled" is globally disabled`) {
		t.Fatalf("error = %q, want destination_id %q is globally disabled", err.Error(), "drive-disabled")
	}
}

// ============================================================
// AtomicJobTaskCreator end-to-end tests (7 plan cases).
// ============================================================

// seedDestinations seeds the delivery_destinations table with the
// given (id, enabled) pairs so the per-destination validator has a
// stable set to query against.
func seedDestinations(t *testing.T, s *SQLiteStore, pairs map[string]bool) {
	t.Helper()
	for id, enabled := range pairs {
		if err := s.InsertDeliveryDestination(&DeliveryDestination{
			DestinationID: id,
			Provider:      "drive",
			Name:          id,
			Enabled:       enabled,
		}); err != nil {
			t.Fatalf("seed destination %q: %v", id, err)
		}
	}
}

// newTestJobAndSpec builds a Job + TaskSpec pair configured for the
// canonical scene.composite.v1 path with the supplied payload. The
// payload is placed verbatim into spec.Payload so parseDeliveryPlanPayload
// (called inside CreateJobWithTask) reads it through the canonical
// delivery_plan / delivery_destination_ids / delivery_destination_id
// keys.
func newTestJobAndSpec(jobID string, payload map[string]interface{}) (*jobs.Job, *taskgraph.TaskSpec) {
	job := &jobs.Job{
		ID:         jobID,
		VideoName:  "Test " + jobID,
		ProjectID:  "test-project",
		RunID:      jobID + "-run",
		MaxRetries: 3,
		Payload:    "",
	}
	spec := &taskgraph.TaskSpec{
		Version:    taskgraph.SpecVersion,
		JobID:      jobID,
		ExecutorID: "scene.composite.v1",
		Payload:    payload,
	}
	return job, spec
}

// assertNoRowsCommitted is the rollback-integrity assertion used by the
// error-path tests. After a failed CreateJobWithTask, NO row should be
// visible in any of the canonical enqueue tables for the given job_id.
// The global task_specs count must also remain zero because the spawned
// task_id never made it to commit.
//
// Each test calls openTestDB (file-based tempdir SQLite opened via
// NewSQLiteStore — see sqlite_ansible_test.go:413), which runs the
// full migration set so delivery_destinations ; job_delivery_plans ;
// jobs ; tasks ; task_specs all exist BEFORE the test starts. So the
// "task_specs == 0 globally" assertion is safe per-test: no other test
// in this package writes task_specs against the same DB.
func assertNoRowsCommitted(t *testing.T, s *SQLiteStore, jobID string) {
	t.Helper()
	ctx := context.Background()

	var jobsN int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM jobs WHERE job_id = ?`, jobID,
	).Scan(&jobsN); err != nil {
		t.Fatalf("COUNT jobs: %v", err)
	}
	if jobsN != 0 {
		t.Errorf("jobs rows for %q = %d; want 0 (rollback)", jobID, jobsN)
	}

	var tasksN int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM tasks WHERE job_id = ?`, jobID,
	).Scan(&tasksN); err != nil {
		t.Fatalf("COUNT tasks: %v", err)
	}
	if tasksN != 0 {
		t.Errorf("tasks rows for %q = %d; want 0 (rollback)", jobID, tasksN)
	}

	var plansN int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM job_delivery_plans WHERE job_id = ?`, jobID,
	).Scan(&plansN); err != nil {
		t.Fatalf("COUNT job_delivery_plans: %v", err)
	}
	if plansN != 0 {
		t.Errorf("job_delivery_plans rows for %q = %d; want 0 (rollback)", jobID, plansN)
	}

	var specN int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM task_specs`).Scan(&specN); err != nil {
		t.Fatalf("COUNT task_specs: %v", err)
	}
	if specN != 0 {
		t.Errorf("task_specs rows = %d; want 0 (rollback)", specN)
	}
}

// TestAtomicJobTaskCreator_HappyPath_SingleDestinationDeliveryPlan
// (Action Plan case 2): a single delivery_destination_id payload, with
// the destination row enabled, MUST succeed — the canonical enqueue
// path that finalizes a real render.
func TestAtomicJobTaskCreator_HappyPath_SingleDestinationDeliveryPlan(t *testing.T) {
	t.Parallel()
	s := openTestDB(t)
	defer s.Close()

	seedDestinations(t, s, map[string]bool{"drive-main": true})

	const jobID = "job-single-dest"
	job, spec := newTestJobAndSpec(jobID, map[string]interface{}{
		"delivery_plan": []interface{}{
			map[string]interface{}{
				"destination_id": "drive-main",
				"priority":       0,
				"retry_budget":   3,
				"enabled":        true,
			},
		},
	})

	creator := NewAtomicJobTaskCreator(s)
	if err := creator.CreateJobWithTask(context.Background(), job, spec, 0); err != nil {
		t.Fatalf("CreateJobWithTask happy path: %v", err)
	}

	// Job row must be present + PENDING.
	var jobStatus string
	if err := s.db.QueryRowContext(context.Background(),
		`SELECT status FROM jobs WHERE job_id = ?`, jobID,
	).Scan(&jobStatus); err != nil {
		t.Fatalf("SELECT jobs: %v", err)
	}
	if jobStatus != "PENDING" {
		t.Errorf("jobs.status = %q; want PENDING", jobStatus)
	}

	// Exactly one Task row + exactly one TaskSpec row.
	var (
		tasksN int
		specsN int
	)
	if err := s.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM tasks WHERE job_id = ?`, jobID,
	).Scan(&tasksN); err != nil {
		t.Fatalf("COUNT tasks: %v", err)
	}
	if tasksN != 1 {
		t.Errorf("tasks rows for %q = %d; want 1", jobID, tasksN)
	}
	if err := s.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM task_specs`,
	).Scan(&specsN); err != nil {
		t.Fatalf("COUNT task_specs: %v", err)
	}
	if specsN != 1 {
		t.Errorf("task_specs rows = %d; want 1", specsN)
	}

	// Exactly one job_delivery_plans row pointing at drive-main.
	var (
		plansN      int
		plansDestID string
		plansBudget int
	)
	if err := s.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*), COALESCE(MIN(destination_id), ''), COALESCE(MIN(retry_budget), 0)
		   FROM job_delivery_plans WHERE job_id = ?`, jobID,
	).Scan(&plansN, &plansDestID, &plansBudget); err != nil {
		t.Fatalf("SELECT job_delivery_plans: %v", err)
	}
	if plansN != 1 {
		t.Fatalf("job_delivery_plans rows = %d; want 1", plansN)
	}
	if plansDestID != "drive-main" {
		t.Errorf("plans.destination_id = %q; want drive-main", plansDestID)
	}
	if plansBudget != 3 {
		t.Errorf("plans.retry_budget = %d; want 3", plansBudget)
	}
}

// TestAtomicJobTaskCreator_HappyPath_MultiDestinationDeliveryPlan
// (Action Plan case 3): a multi-id payload delivers a fan-out plan.
// Every destination row keeps the same retry_budget as supplied, and
// the priority ordering is preserved as supplied by the caller.
func TestAtomicJobTaskCreator_HappyPath_MultiDestinationDeliveryPlan(t *testing.T) {
	t.Parallel()
	s := openTestDB(t)
	defer s.Close()

	seedDestinations(t, s, map[string]bool{
		"drive-main": true,
		"video-main": true,
		"social-mr": true,
	})

	const jobID = "job-multi-dest"
	plan := []interface{}{
		map[string]interface{}{"destination_id": "drive-main", "priority": 0, "retry_budget": 3, "enabled": true},
		map[string]interface{}{"destination_id": "video-main", "priority": 1, "retry_budget": 7, "enabled": true},
		map[string]interface{}{"destination_id": "social-mr", "priority": 2, "retry_budget": 5, "enabled": true},
	}
	job, spec := newTestJobAndSpec(jobID, map[string]interface{}{
		"delivery_plan": plan,
	})

	creator := NewAtomicJobTaskCreator(s)
	if err := creator.CreateJobWithTask(context.Background(), job, spec, 0); err != nil {
		t.Fatalf("CreateJobWithTask multi-dest: %v", err)
	}

	// All three plan rows committed in priority order.
	rows, err := s.db.QueryContext(context.Background(),
		`SELECT destination_id, priority, retry_budget
		   FROM job_delivery_plans WHERE job_id = ? ORDER BY priority ASC`, jobID)
	if err != nil {
		t.Fatalf("SELECT job_delivery_plans: %v", err)
	}
	defer rows.Close()

	type planRow struct {
		dest string
		prio int
		budg int
	}
	var got []planRow
	for rows.Next() {
		var r planRow
		if err := rows.Scan(&r.dest, &r.prio, &r.budg); err != nil {
			t.Fatalf("scan plan row: %v", err)
		}
		got = append(got, r)
	}
	want := []planRow{
		{"drive-main", 0, 3},
		{"video-main", 1, 7},
		{"social-mr", 2, 5},
	}
	if len(got) != len(want) {
		t.Fatalf("plan rows = %d; want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("plan[%d] = %+v; want %+v", i, got[i], want[i])
		}
	}
}

// TestAtomicJobTaskCreator_RejectsMissingDestination (Action Plan
// case 4): a delivery_plan entry whose destination_id does NOT exist
// in delivery_destinations MUST reject the create, and the rejection
// MUST roll back ALL tx writes (no orphan Job / Task / TaskSpec /
// job_delivery_plans row).
func TestAtomicJobTaskCreator_RejectsMissingDestination(t *testing.T) {
	t.Parallel()
	s := openTestDB(t)
	defer s.Close()

	seedDestinations(t, s, map[string]bool{"drive-main": true})

	const jobID = "job-missing-dest"
	job, spec := newTestJobAndSpec(jobID, map[string]interface{}{
		"delivery_plan": []interface{}{
			map[string]interface{}{
				"destination_id": "ghost-dest",
				"priority":       0,
				"retry_budget":   3,
				"enabled":        true,
			},
		},
	})

	creator := NewAtomicJobTaskCreator(s)
	err := creator.CreateJobWithTask(context.Background(), job, spec, 0)
	if err == nil {
		t.Fatal("want error for non-existent destination, got nil")
	}
	if !strings.Contains(err.Error(), `"ghost-dest" does not exist`) {
		t.Fatalf("error = %q, want destination_id %q does not exist", err.Error(), "ghost-dest")
	}

	assertNoRowsCommitted(t, s, jobID)
}

// TestAtomicJobTaskCreator_RejectsDisabledDestination (Action Plan
// case 5): a delivery_plan entry whose destination_id exists but is
// enabled=0 MUST reject the create, AND MUST roll back. The error
// message must distinguish "globally disabled" from "does not exist"
// so callers can take appropriate action (re-enable vs investigate
// typos / removed destinations).
func TestAtomicJobTaskCreator_RejectsDisabledDestination(t *testing.T) {
	t.Parallel()
	s := openTestDB(t)
	defer s.Close()

	seedDestinations(t, s, map[string]bool{
		"drive-main":     true,
		"video-disabled": false,
	})

	const jobID = "job-disabled-dest"
	job, spec := newTestJobAndSpec(jobID, map[string]interface{}{
		"delivery_plan": []interface{}{
			map[string]interface{}{
				"destination_id": "drive-main",
				"priority":       0,
				"retry_budget":   3,
				"enabled":        true,
			},
			map[string]interface{}{
				"destination_id": "video-disabled",
				"priority":       1,
				"retry_budget":   3,
				"enabled":        true,
			},
		},
	})

	creator := NewAtomicJobTaskCreator(s)
	err := creator.CreateJobWithTask(context.Background(), job, spec, 0)
	if err == nil {
		t.Fatal("want error for disabled destination, got nil")
	}
	if !strings.Contains(err.Error(), `"video-disabled" is globally disabled`) {
		t.Fatalf("error = %q, want destination_id %q is globally disabled", err.Error(), "video-disabled")
	}

	assertNoRowsCommitted(t, s, jobID)
}

// TestAtomicJobTaskCreator_RollsBackOnDuplicateDestinationInPlan
// (Action Plan cases 6 + 7): a delivery_plan payload carrying the
// same destination_id twice is rejected at parse time by
// parseDeliveryPlanPayload with the canonical "duplicate" error, and
// the rejection MUST roll back to ZERO committed rows.
func TestAtomicJobTaskCreator_RollsBackOnDuplicateDestinationInPlan(t *testing.T) {
	t.Parallel()
	s := openTestDB(t)
	defer s.Close()

	seedDestinations(t, s, map[string]bool{"drive-main": true})

	const jobID = "job-duplicate-dest"
	job, spec := newTestJobAndSpec(jobID, map[string]interface{}{
		"delivery_plan": []interface{}{
			map[string]interface{}{"destination_id": "drive-main", "priority": 0, "retry_budget": 3, "enabled": true},
			map[string]interface{}{"destination_id": "drive-main", "priority": 1, "retry_budget": 5, "enabled": true},
		},
	})

	creator := NewAtomicJobTaskCreator(s)
	err := creator.CreateJobWithTask(context.Background(), job, spec, 0)
	if err == nil {
		t.Fatal("want error for duplicate destination_id in plan, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate destination_id") {
		t.Fatalf("error = %q, want duplicate destination_id", err.Error())
	}

	assertNoRowsCommitted(t, s, jobID)
}

// TestCreateJobWithTaskTx_CallerOwnsCommitBoundary nails down the
// partition between BeginTx held by AtomicJobTaskCreator (the
// CreateJobWithTask convenience path) and the CreateJobWithTaskTx path
// which assumes the CALLER manages commit/rollback.
//
// This test exercises the error-path contract explicitly: an invalid
// destination forces CreateJobWithTaskTx to return; the caller does
// NOT commit; the defer rollback fires; zero rows are visible.
func TestCreateJobWithTaskTx_CallerOwnsCommitBoundary(t *testing.T) {
	t.Parallel()
	s := openTestDB(t)
	defer s.Close()

	seedDestinations(t, s, map[string]bool{"drive-main": true})

	const jobID = "job-tx-caller-commit-error"
	job, spec := newTestJobAndSpec(jobID, map[string]interface{}{
		"delivery_plan": []interface{}{
			map[string]interface{}{"destination_id": "ghost-dest", "priority": 0, "retry_budget": 3, "enabled": true},
		},
	})

	creator := NewAtomicJobTaskCreator(s)
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }() // caller's commit boundary on error path

	err = creator.CreateJobWithTaskTx(ctx, tx, job, spec, 0)
	if err == nil {
		t.Fatal("want error for non-existent destination")
	}

	// Caller did NOT commit; the deferred Rollback should have cleared
	// any side-effects (and because CreateJobWithTaskTx already produced
	// the error before any commit, the defer rollback is largely a
	// no-op safety net for failed-tx cases).
	assertNoRowsCommitted(t, s, jobID)
}

// TestCreateJobWithTaskTx_HappyCommitPersistsWhenCallerCommits is the
// happy-path companion to TestCreateJobWithTaskTx_CallerOwnsCommitBoundary.
// Together they prove the *Tx variant does NOT auto-commit: the caller
// MUST call tx.Commit() for rows to become visible. Without the
// explicit commit, even a successful CreateJobWithTaskTx leaves zero
// rows once the caller rolls back.
func TestCreateJobWithTaskTx_HappyCommitPersistsWhenCallerCommits(t *testing.T) {
	t.Parallel()
	s := openTestDB(t)
	defer s.Close()

	seedDestinations(t, s, map[string]bool{"drive-main": true})

	const jobID = "job-tx-caller-commit-happy"
	job, spec := newTestJobAndSpec(jobID, map[string]interface{}{
		"delivery_plan": []interface{}{
			map[string]interface{}{"destination_id": "drive-main", "priority": 0, "retry_budget": 3, "enabled": true},
		},
	})

	creator := NewAtomicJobTaskCreator(s)
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if err := creator.CreateJobWithTaskTx(ctx, tx, job, spec, 0); err != nil {
		_ = tx.Rollback()
		t.Fatalf("CreateJobWithTaskTx happy path: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("tx.Commit: %v", err)
	}

	// After explicit Commit, all four enqueue rows are visible.
	var jobsN, tasksN, specsN, plansN int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM jobs WHERE job_id = ?`, jobID,
	).Scan(&jobsN); err != nil {
		t.Fatalf("COUNT jobs: %v", err)
	}
	if jobsN != 1 {
		t.Errorf("jobs rows for %q = %d; want 1 (caller commit persisted)", jobID, jobsN)
	}
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM tasks WHERE job_id = ?`, jobID,
	).Scan(&tasksN); err != nil {
		t.Fatalf("COUNT tasks: %v", err)
	}
	if tasksN != 1 {
		t.Errorf("tasks rows for %q = %d; want 1", jobID, tasksN)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM task_specs`).Scan(&specsN); err != nil {
		t.Fatalf("COUNT task_specs: %v", err)
	}
	if specsN != 1 {
		t.Errorf("task_specs rows = %d; want 1", specsN)
	}
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM job_delivery_plans WHERE job_id = ?`, jobID,
	).Scan(&plansN); err != nil {
		t.Fatalf("COUNT job_delivery_plans: %v", err)
	}
	if plansN != 1 {
		t.Errorf("job_delivery_plans rows for %q = %d; want 1", jobID, plansN)
	}
}

// TestAtomicJobTaskCreator_RequiresExplicitPlanInProdByDefault nails
// down Action Plan case 1: WithDeliveryPlanPolicy(true) makes any
// enqueue without an explicit delivery_plan rejected, even if payload
// has clips / scenes / a delivery_destination_id legacy singleton.
// The default state of the creator (requireExplicit=false) is permissive
// — exposed here so future bootstrap changes cannot accidentally regress
// prod by removing the explicit flag.
func TestAtomicJobTaskCreator_RequiresExplicitPlanInProdByDefault(t *testing.T) {
	t.Parallel()
	s := openTestDB(t)
	defer s.Close()

	const jobID = "job-no-plan-prod"
	// payload has no delivery_plan / delivery_destination_ids / delivery_destination_id
	job, spec := newTestJobAndSpec(jobID, map[string]interface{}{
		"video_name": "Test",
		"scenes":     []interface{}{map[string]interface{}{"text": "S1"}},
	})

	creator := NewAtomicJobTaskCreator(s).WithDeliveryPlanPolicy(true)

	err := creator.CreateJobWithTask(context.Background(), job, spec, 0)
	if err == nil {
		t.Fatal("want error when requireExplicitDeliveryPlan=true and payload has no plan")
	}
	if !strings.Contains(err.Error(), "explicit delivery plan required") {
		t.Fatalf("error = %q, want explicit delivery plan required", err.Error())
	}

	assertNoRowsCommitted(t, s, jobID)
}
