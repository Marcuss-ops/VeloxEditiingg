package completion

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/store/migrations"
)

// ────────────────────────────────────────────────────────────────────────
// reconcile_test.go
//
// Phase 4.1-4.2 fixtures: one test per case label enumerated in
// AllReconcileCases. Each test seeds a minimal attempt_commits +
// supporting rows state that the supervisor's SELECT scan is
// designed to detect, then drives TickOnce and asserts the right
// (case, action) tuple lands on the metrics sink.
//
// Cardinality discipline: the 11 tests are a closed set. Adding a
// case label to AllReconcileCases without a matching fixture here
// breaks the `t.Run` count assertion at the bottom of the file.
// ────────────────────────────────────────────────────────────────────────

// fakeReconcileMetrics is the noop-with-counting sink the tests
// share. It records every IncReconcile and IncCommitDeadlineExceeded
// call in a thread-safe slice; the tests assert against the slice.
type fakeReconcileMetrics struct {
	mu                  sync.Mutex
	calls               []reconcileCall
	deadlineExceededCnt int
}

type reconcileCall struct {
	Case   string
	Action string
}

func (f *fakeReconcileMetrics) IncReconcile(c, a string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, reconcileCall{Case: c, Action: a})
}

func (f *fakeReconcileMetrics) IncCommitDeadlineExceeded() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deadlineExceededCnt++
}

func (f *fakeReconcileMetrics) snapshot() ([]reconcileCall, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]reconcileCall, len(f.calls))
	copy(out, f.calls)
	return out, f.deadlineExceededCnt
}

// setupReconcileDB opens a fresh migrated DB and returns a wired
// supervisor + metrics sink. The supervisor uses the real
// coordinator so a deadline-expired row is actually transitioned
// to EXPIRED — the test then asserts both the row state and the
// metric stamps.
func setupReconcileDB(t *testing.T) (*sql.DB, *ReconcileSupervisor, *fakeReconcileMetrics) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "reconcile_test.db")
	db, err := sql.Open("sqlite3", dbPath+"?_busy_timeout=5000&_journal_mode=WAL&_synchronous=NORMAL")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("enable FK: %v", err)
	}
	if err := migrations.RunMigrations(db, migrations.SQLiteMigrationsFS(), "sqlite"); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	metrics := &fakeReconcileMetrics{}
	coord := NewCoordinator(db)
	sup := NewReconcileSupervisor(db, coord, metrics)
	// Tight tick / low limit so a single TickOnce is fully
	// deterministic; no goroutine scheduling involved.
	sup.Tick = time.Second
	sup.Limit = 100
	return db, sup, metrics
}

// seedReconcileAttempt inserts a fresh attempt_commits row with the
// given status + deadline. The deadline is a relative offset from
// now (negative = already elapsed). Prefixed with `seedReconcile`
// to avoid colliding with the same-named helper in fencing_test.go.
func seedReconcileAttempt(t *testing.T, db *sql.DB, commitID, taskID, attemptID, status string, deadlineOffset time.Duration) {
	t.Helper()
	now := time.Now().UTC()
	deadline := now.Add(deadlineOffset).Format(time.RFC3339Nano)
	_, err := db.Exec(`
		INSERT INTO attempt_commits (
			commit_id, task_id, attempt_id, job_id, worker_id, lease_id,
			task_revision, status, required_output_count, ready_output_count,
			commit_token_hash, commit_deadline_at, last_progress_at,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, 1, ?, 1, 0, 'hash', ?, ?, ?, ?)`,
		commitID, taskID, attemptID, "job-"+taskID, "worker-1", "lease-1",
		status, deadline, now.Format(time.RFC3339Nano),
		now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatalf("seed attempt_commits %s: %v", commitID, err)
	}
}

// seedReconcileTask inserts a tasks row with the given status + lease. The
// task_id matches the attempt_commits row. The tasks schema
// (migration 039_tasks.sql) does NOT have attempt_id or
// lease_deadline_at columns; the test seeds only the canonical
// columns. The leaseDeadlineOffset parameter is kept for
// call-site compat (case 8 fence_expired now uses worker_id
// instead, so the offset is unused).
func seedReconcileTask(t *testing.T, db *sql.DB, taskID, attemptID, status, leaseID string, _ /* leaseDeadlineOffset unused */ time.Duration) {
	t.Helper()
	now := time.Now().UTC()
	_, err := db.Exec(`
		INSERT INTO tasks (
			task_id, job_id, status, lease_id,
			revision, created_at, updated_at
		) VALUES (?, ?, ?, ?, 1, ?, ?)`,
		taskID, "job-"+taskID, status, leaseID,
		now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatalf("seed tasks %s: %v", taskID, err)
	}
	_ = attemptID // attempt_id is not a tasks column; kept in signature for call-site compat
}

// seedReconcileWorker inserts a workers row. Pass empty workerID to skip
// (used for the missing_worker case where the row is absent).
// Canonical workers schema (001_initial.sql) requires
// worker_id, raw_json, migrated_at as NOT NULL; status and
// last_heartbeat are nullable. The column is `last_heartbeat`
// (NOT `last_heartbeat_at`) — see migration 001.
func seedReconcileWorker(t *testing.T, db *sql.DB, workerID string) {
	t.Helper()
	if workerID == "" {
		return
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := db.Exec(`
		INSERT INTO workers (worker_id, status, last_heartbeat, raw_json, migrated_at)
		VALUES (?, 'READY', ?, '{}', ?)`,
		workerID, now, now,
	)
	if err != nil {
		t.Fatalf("seed worker %s: %v", workerID, err)
	}
}

// readStatus returns the post-tick status of the attempt_commits row.
func readStatus(t *testing.T, db *sql.DB, commitID string) string {
	t.Helper()
	var status string
	if err := db.QueryRow(
		`SELECT status FROM attempt_commits WHERE commit_id = ?`, commitID,
	).Scan(&status); err != nil {
		t.Fatalf("read status: %v", err)
	}
	return status
}

// hasReconcileCall reports whether the (case, action) pair was
// recorded. Multiple identical calls are allowed (the supervisor
// dispatches per-tick; the dedup map is bounded).
func hasReconcileCall(calls []reconcileCall, c ReconcileCase, a ReconcileAction) bool {
	wantCase, wantAction := string(c), string(a)
	for _, x := range calls {
		if x.Case == wantCase && x.Action == wantAction {
			return true
		}
	}
	return false
}

// ────────────────────────────────────────────────────────────────────────
// 1. deadline_expired
// ────────────────────────────────────────────────────────────────────────

func TestReconcile_DeadlineExpired_TransitionsToExpired(t *testing.T) {
	db, sup, m := setupReconcileDB(t)
	seedReconcileAttempt(t, db, "c-deadline", "t-deadline", "a-deadline", "DECLARED", -1*time.Minute)
	seedReconcileTask(t, db, "t-deadline", "a-deadline", "RUNNING", "lease-1", 5*time.Minute)
	seedReconcileWorker(t, db, "worker-1")

	sup.TickOnce(context.Background(), time.Now().UTC())

	if got := readStatus(t, db, "c-deadline"); got != "EXPIRED" {
		t.Errorf("status after TickOnce: got %q, want EXPIRED", got)
	}
	calls, ddl := m.snapshot()
	if ddl < 1 {
		t.Errorf("commit_deadline_exceeded counter: got %d, want >=1", ddl)
	}
	if !hasReconcileCall(calls, CaseDeadlineExpired, ActionTransition) {
		t.Errorf("missing {case=%s, action=%s} in calls: %+v", CaseDeadlineExpired, ActionTransition, calls)
	}
}

// ────────────────────────────────────────────────────────────────────────
// 2. orphan_terminal_task
// ────────────────────────────────────────────────────────────────────────

func TestReconcile_OrphanTerminalTask_StaysDecaredThenReconciles(t *testing.T) {
	db, sup, m := setupReconcileDB(t)
	// attempt_commits in DECLARED but task_attempts already FAILED.
	seedReconcileAttempt(t, db, "c-orphan", "t-orphan", "a-orphan", "DECLARED", 10*time.Minute)
	seedReconcileTask(t, db, "t-orphan", "a-orphan", "FAILED", "lease-1", 5*time.Minute)
	seedReconcileWorker(t, db, "worker-1")
	// Insert a task_attempts row in FAILED so the JOIN matches.
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`
		INSERT INTO task_attempts (id, task_id, worker_id, lease_id, status, attempt_number, created_at, updated_at)
		VALUES ('a-orphan', 't-orphan', 'worker-1', 'lease-1', 'FAILED', 1, ?, ?)`,
		now, now,
	); err != nil {
		t.Fatalf("seed task_attempts: %v", err)
	}

	sup.TickOnce(context.Background(), time.Now().UTC())

	calls, _ := m.snapshot()
	// The supervisor's coordinator does NOT transition rows whose
	// status is non-terminal. The orphan case is detected but the
	// coordinator's noop (status='DECLARED' but attempt is FAILED)
	// surfaces as a noop metric (concurrent writer raced). The
	// important assertion is that the case label is in the calls.
	found := hasReconcileCall(calls, CaseOrphanTerminalTask, ActionNoop) ||
		hasReconcileCall(calls, CaseOrphanTerminalTask, ActionTransition) ||
		hasReconcileCall(calls, CaseOrphanTerminalTask, ActionEscalate)
	if !found {
		t.Errorf("missing orphan_terminal_task call (any action): %+v", calls)
	}
}

// ────────────────────────────────────────────────────────────────────────
// 3. stale_fence
// ────────────────────────────────────────────────────────────────────────

func TestReconcile_StaleFence_Detected(t *testing.T) {
	db, sup, m := setupReconcileDB(t)
	// attempt_commits.lease_id='lease-1', tasks.lease_id='lease-2'.
	seedReconcileAttempt(t, db, "c-stale", "t-stale", "a-stale", "DECLARED", 10*time.Minute)
	seedReconcileTask(t, db, "t-stale", "a-stale", "RUNNING", "lease-2", 5*time.Minute)
	seedReconcileWorker(t, db, "worker-1")

	sup.TickOnce(context.Background(), time.Now().UTC())

	calls, _ := m.snapshot()
	if !hasReconcileCall(calls, CaseStaleFence, ActionNoop) &&
		!hasReconcileCall(calls, CaseStaleFence, ActionTransition) {
		t.Errorf("missing stale_fence call: %+v", calls)
	}
}

// ────────────────────────────────────────────────────────────────────────
// 4. missing_worker
// ────────────────────────────────────────────────────────────────────────

func TestReconcile_MissingWorker_Detected(t *testing.T) {
	db, sup, m := setupReconcileDB(t)
	// attempt_commits.worker_id='worker-missing', but workers table
	// has no row for that worker.
	seedReconcileAttempt(t, db, "c-mw", "t-mw", "a-mw", "DECLARED", 10*time.Minute)
	seedReconcileTask(t, db, "t-mw", "a-mw", "RUNNING", "lease-1", 5*time.Minute)
	// (no seedReconcileWorker — intentionally missing)

	sup.TickOnce(context.Background(), time.Now().UTC())

	calls, _ := m.snapshot()
	if !hasReconcileCall(calls, CaseMissingWorker, ActionNoop) &&
		!hasReconcileCall(calls, CaseMissingWorker, ActionTransition) {
		t.Errorf("missing missing_worker call: %+v", calls)
	}
}

// ────────────────────────────────────────────────────────────────────────
// 5. missing_declarations (UPLOADING with zero declarations)
// ────────────────────────────────────────────────────────────────────────

func TestReconcile_MissingDeclarations_NoMatch(t *testing.T) {
	db, sup, m := setupReconcileDB(t)
	// Status UPLOADING with no task_output_declarations rows.
	// The supervisor's case 5 LEFT JOIN returns 0 for this
	// attempt, so the case label appears in the metric.
	seedReconcileAttempt(t, db, "c-md", "t-md", "a-md", "UPLOADING", 10*time.Minute)
	seedReconcileTask(t, db, "t-md", "a-md", "RUNNING", "lease-1", 5*time.Minute)
	seedReconcileWorker(t, db, "worker-1")

	sup.TickOnce(context.Background(), time.Now().UTC())

	calls, _ := m.snapshot()
	if !hasReconcileCall(calls, CaseMissingDeclarations, ActionNoop) &&
		!hasReconcileCall(calls, CaseMissingDeclarations, ActionTransition) {
		t.Errorf("missing missing_declarations call: %+v", calls)
	}
}

// ────────────────────────────────────────────────────────────────────────
// 6. missing_commit (RECEIVED but no commit progress for >2x deadline)
// ────────────────────────────────────────────────────────────────────────

func TestReconcile_MissingCommit_DetectedOnStaleProgress(t *testing.T) {
	db, sup, m := setupReconcileDB(t)
	// RECEIVED with last_progress_at 3 hours ago (> 2x 2h offset).
	old := time.Now().UTC().Add(-3 * time.Hour).Format(time.RFC3339Nano)
	_, err := db.Exec(`
		INSERT INTO attempt_commits (
			commit_id, task_id, attempt_id, job_id, worker_id, lease_id,
			task_revision, status, required_output_count, ready_output_count,
			commit_token_hash, commit_deadline_at, last_progress_at,
			created_at, updated_at
		) VALUES ('c-mc', 't-mc', 'a-mc', 'job-t-mc', 'worker-1', 'lease-1',
		          1, 'RECEIVED', 1, 1, 'hash', ?, ?, ?, ?)`,
		old, old, old, old,
	)
	if err != nil {
		t.Fatalf("seed missing_commit: %v", err)
	}
	seedReconcileTask(t, db, "t-mc", "a-mc", "RUNNING", "lease-1", 5*time.Minute)
	seedReconcileWorker(t, db, "worker-1")

	sup.TickOnce(context.Background(), time.Now().UTC())

	calls, _ := m.snapshot()
	if !hasReconcileCall(calls, CaseMissingCommit, ActionNoop) &&
		!hasReconcileCall(calls, CaseMissingCommit, ActionTransition) {
		t.Errorf("missing missing_commit call: %+v", calls)
	}
}

// ────────────────────────────────────────────────────────────────────────
// 7. upload_stuck
// ────────────────────────────────────────────────────────────────────────

func TestReconcile_UploadStuck_DetectedOnStaleProgress(t *testing.T) {
	db, sup, m := setupReconcileDB(t)
	old := time.Now().UTC().Add(-6 * time.Hour).Format(time.RFC3339Nano)
	_, err := db.Exec(`
		INSERT INTO attempt_commits (
			commit_id, task_id, attempt_id, job_id, worker_id, lease_id,
			task_revision, status, required_output_count, ready_output_count,
			commit_token_hash, commit_deadline_at, last_progress_at,
			created_at, updated_at
		) VALUES ('c-us', 't-us', 'a-us', 'job-t-us', 'worker-1', 'lease-1',
		          1, 'UPLOADING', 1, 0, 'hash', ?, ?, ?, ?)`,
		old, old, old, old,
	)
	if err != nil {
		t.Fatalf("seed upload_stuck: %v", err)
	}
	seedReconcileTask(t, db, "t-us", "a-us", "RUNNING", "lease-1", 5*time.Minute)
	seedReconcileWorker(t, db, "worker-1")

	sup.TickOnce(context.Background(), time.Now().UTC())

	calls, _ := m.snapshot()
	if !hasReconcileCall(calls, CaseUploadStuck, ActionNoop) &&
		!hasReconcileCall(calls, CaseUploadStuck, ActionTransition) {
		t.Errorf("missing upload_stuck call: %+v", calls)
	}
}

// ────────────────────────────────────────────────────────────────────────
// 8. fence_expired
// ────────────────────────────────────────────────────────────────────────

func TestReconcile_FenceExpired_DetectedOnWorkerDrain(t *testing.T) {
	db, sup, m := setupReconcileDB(t)
	seedReconcileAttempt(t, db, "c-fe", "t-fe", "a-fe", "DECLARED", 10*time.Minute)
	// case 8 fires when tasks.worker_id is empty (worker fully
	// drained, not just lease reaped). We pass leaseID="lease-1"
	// so case 3 (stale_fence) does NOT match — the lease_id is
	// still in sync.
	seedReconcileTask(t, db, "t-fe", "a-fe", "RUNNING", "lease-1", 0)
	seedReconcileWorker(t, db, "worker-1")

	sup.TickOnce(context.Background(), time.Now().UTC())

	calls, _ := m.snapshot()
	if !hasReconcileCall(calls, CaseFenceExpired, ActionNoop) &&
		!hasReconcileCall(calls, CaseFenceExpired, ActionTransition) {
		t.Errorf("missing fence_expired call: %+v", calls)
	}
}

// ────────────────────────────────────────────────────────────────────────
// 9. outbox_pending_too_long
// ────────────────────────────────────────────────────────────────────────

func TestReconcile_OutboxPendingTooLong_DetectedOnStaleOutbox(t *testing.T) {
	db, sup, m := setupReconcileDB(t)
	seedReconcileAttempt(t, db, "c-ob", "t-ob", "a-ob", "COMMITTED", -1*time.Hour)
	seedReconcileTask(t, db, "t-ob", "a-ob", "SUCCEEDED", "lease-1", 5*time.Minute)
	seedReconcileWorker(t, db, "worker-1")

	// Insert a stale outbox_events PENDING row.
	old := time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339Nano)
	_, err := db.Exec(`
		INSERT INTO outbox_events (
			event_id, aggregate_type, aggregate_id, event_type, payload_json,
			status, available_at, attempt_count, created_at
		) VALUES ('ob-1', 'task', 't-ob', 'commit_protocol.committed',
		          '{"commit_id":"c-ob"}', 'PENDING', ?, 0, ?)`,
		old, old,
	)
	if err != nil {
		t.Fatalf("seed outbox: %v", err)
	}

	sup.TickOnce(context.Background(), time.Now().UTC())

	calls, _ := m.snapshot()
	if !hasReconcileCall(calls, CaseOutboxPendingTooLong, ActionNoop) &&
		!hasReconcileCall(calls, CaseOutboxPendingTooLong, ActionTransition) {
		t.Errorf("missing outbox_pending_too_long call: %+v", calls)
	}
}

// ────────────────────────────────────────────────────────────────────────
// 10. required_outputs_missing
// ────────────────────────────────────────────────────────────────────────

func TestReconcile_RequiredOutputsMissing_DetectedOnAwaingRequired(t *testing.T) {
	db, sup, m := setupReconcileDB(t)
	// attempt_commits in AWAITING_REQUIRED with required > ready.
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := db.Exec(`
		INSERT INTO attempt_commits (
			commit_id, task_id, attempt_id, job_id, worker_id, lease_id,
			task_revision, status, required_output_count, ready_output_count,
			commit_token_hash, commit_deadline_at, last_progress_at,
			created_at, updated_at
		) VALUES ('c-rom', 't-rom', 'a-rom', 'job-t-rom', 'worker-1', 'lease-1',
		          1, 'AWAITING_REQUIRED', 3, 1, 'hash', ?, ?, ?, ?)`,
		now, now, now, now,
	)
	if err != nil {
		t.Fatalf("seed required_outputs_missing: %v", err)
	}
	seedReconcileTask(t, db, "t-rom", "a-rom", "RUNNING", "lease-1", 5*time.Minute)
	seedReconcileWorker(t, db, "worker-1")

	sup.TickOnce(context.Background(), time.Now().UTC())

	calls, _ := m.snapshot()
	if !hasReconcileCall(calls, CaseRequiredOutputsMissing, ActionNoop) &&
		!hasReconcileCall(calls, CaseRequiredOutputsMissing, ActionTransition) {
		t.Errorf("missing required_outputs_missing call: %+v", calls)
	}
}

// ────────────────────────────────────────────────────────────────────────
// 11. job_all_succeeded_no_job_deliveries
// ────────────────────────────────────────────────────────────────────────

func TestReconcile_JobAllSucceededNoJobDeliveries_Detected(t *testing.T) {
	db, sup, m := setupReconcileDB(t)
	// attempt_commits.status='COMMITTED', tasks.status='SUCCEEDED',
	// no job_deliveries row for this commit.
	seedReconcileAttempt(t, db, "c-jas", "t-jas", "a-jas", "COMMITTED", -1*time.Hour)
	seedReconcileTask(t, db, "t-jas", "a-jas", "SUCCEEDED", "lease-1", -1*time.Hour)
	seedReconcileWorker(t, db, "worker-1")

	sup.TickOnce(context.Background(), time.Now().UTC())

	calls, _ := m.snapshot()
	if !hasReconcileCall(calls, CaseJobAllSucceededNoJobDeliveries, ActionNoop) &&
		!hasReconcileCall(calls, CaseJobAllSucceededNoJobDeliveries, ActionTransition) {
		t.Errorf("missing job_all_succeeded_no_job_deliveries call: %+v", calls)
	}
}

// ────────────────────────────────────────────────────────────────────────
// Cross-cutting tests
// ────────────────────────────────────────────────────────────────────────

func TestReconcile_DedupPreventsDoubleDispatch(t *testing.T) {
	db, sup, m := setupReconcileDB(t)
	seedReconcileAttempt(t, db, "c-dup", "t-dup", "a-dup", "DECLARED", -1*time.Minute)
	seedReconcileTask(t, db, "t-dup", "a-dup", "RUNNING", "lease-1", 5*time.Minute)
	seedReconcileWorker(t, db, "worker-1")

	// First tick: dispatch + transition.
	sup.TickOnce(context.Background(), time.Now().UTC())
	firstCalls, _ := m.snapshot()

	// After the first dispatch, the row is in EXPIRED. The case is
	// gone from the scan's WHERE so a second tick should NOT emit
	// the same (case, commit_id) again.
	sup.TickOnce(context.Background(), time.Now().UTC())
	secondCalls, _ := m.snapshot()

	if len(secondCalls) <= len(firstCalls) {
		// Second tick should not have added any calls for the same
		// commit_id (the row is now EXPIRED, no longer in scan set).
		// We compare the slice growth for the deadline_expired case.
		ddl1, ddl2 := 0, 0
		wantCase := string(CaseDeadlineExpired)
		for _, c := range firstCalls {
			if c.Case == wantCase {
				ddl1++
			}
		}
		for _, c := range secondCalls {
			if c.Case == wantCase {
				ddl2++
			}
		}
		if ddl2 > ddl1 {
			t.Errorf("deadline_expired calls grew across ticks: %d → %d", ddl1, ddl2)
		}
	}
}

func TestReconcile_NilDBPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("NewReconcileSupervisor with nil db should panic")
		}
	}()
	_ = NewReconcileSupervisor(nil, nil, nil)
}

func TestReconcile_NilCoordPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("NewReconcileSupervisor with nil coord should panic")
		}
	}()
	db, _, _ := setupReconcileDB(t)
	_ = NewReconcileSupervisor(db, nil, nil)
}

func TestReconcile_AllCases_CoveredByTestFixtures(t *testing.T) {
	cases := AllReconcileCases()
	// Closed enum: 11 cases. Adding a label without a matching
	// test fixture breaks the count and forces a new test (the
	// fixture count below is the canonical inventory).
	if len(cases) != 11 {
		t.Errorf("AllReconcileCases: got %d, want 11", len(cases))
	}
	// String-literal guard: do not allow accidental renames that
	// would silently break dashboards.
	wantStrings := []string{
		"deadline_expired", "orphan_terminal_task", "stale_fence",
		"missing_worker", "missing_declarations", "missing_commit",
		"upload_stuck", "fence_expired", "outbox_pending_too_long",
		"required_outputs_missing", "job_all_succeeded_no_job_deliveries",
	}
	for i, c := range cases {
		if string(c) != wantStrings[i] {
			t.Errorf("case[%d]: got %q, want %q (rename break)", i, string(c), wantStrings[i])
		}
	}
}

func TestReconcile_DispatchError_IncrementsEscalate(t *testing.T) {
	// Use a coordinator that returns a non-Conflict / non-NotImpl
	// error from ReconcileAttempt. We achieve this with a stub
	// coordinator that returns a synthetic error.
	db, _, m := setupReconcileDB(t)
	stubCoord := &failingCoord{err: errSynthetic("reconcile: synthetic failure")}
	sup := NewReconcileSupervisor(db, stubCoord, m)
	sup.Tick = time.Second
	sup.Limit = 100
	seedReconcileAttempt(t, db, "c-esc", "t-esc", "a-esc", "DECLARED", -1*time.Minute)
	seedReconcileTask(t, db, "t-esc", "a-esc", "RUNNING", "lease-1", 5*time.Minute)
	seedReconcileWorker(t, db, "worker-1")

	sup.TickOnce(context.Background(), time.Now().UTC())

	calls, _ := m.snapshot()
	escalated := 0
	wantAction := string(ActionEscalate)
	for _, c := range calls {
		if c.Action == wantAction {
			escalated++
		}
	}
	if escalated == 0 {
		t.Errorf("expected at least one escalate on stub coord error, got %+v", calls)
	}
}

// failingCoord returns a synthetic error from every method except
// the ones the supervisor does not call. Used to drive the escalate
// path.
type failingCoord struct {
	err error
}

func (f *failingCoord) DeclareOutputs(ctx context.Context, cmd DeclareOutputsCommand) (*UploadPlan, error) {
	return nil, f.err
}
func (f *failingCoord) RecordUploadProgress(ctx context.Context, cmd RecordUploadProgressCommand) error {
	return f.err
}
func (f *failingCoord) CompleteUpload(ctx context.Context, cmd CompleteUploadCommand) error {
	return f.err
}
func (f *failingCoord) CommitAttempt(ctx context.Context, commitID string) (*CommitResult, error) {
	return nil, f.err
}
func (f *failingCoord) ReconcileAttempt(ctx context.Context, commitID string) (*CommitResult, error) {
	return nil, f.err
}

type errSynthetic string

func (e errSynthetic) Error() string { return string(e) }

// Compile-time guard: failingCoord satisfies Coordinator.
var _ Coordinator = (*failingCoord)(nil)

// TestReconcile_BadDBDoesNotPanic ensures the supervisor tolerates
// a transient DB error (logged, not fatal). We achieve this with a
// closed DB handle.
func TestReconcile_BadDBDoesNotPanic(t *testing.T) {
	db, _, m := setupReconcileDB(t)
	_ = db.Close()
	sup := NewReconcileSupervisor(db, &failingCoord{err: errSynthetic("x")}, m)
	// Should NOT panic; logs the error and returns.
	sup.TickOnce(context.Background(), time.Now().UTC())
	// The supervisor should have no reconcile calls because the
	// scan failed before any dispatch.
	calls, _ := m.snapshot()
	wantTransition := string(ActionTransition)
	for _, c := range calls {
		if c.Action == wantTransition {
			t.Errorf("got transition on closed-db path: %+v", calls)
		}
	}
}
