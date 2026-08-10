package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"velox-server/internal/store/migrations"

	_ "github.com/mattn/go-sqlite3"
)

func openStaleReconcilerTestDB(t *testing.T) (*SQLiteStore, *sql.DB) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "stale-reconcile.db")
	db, err := sql.Open("sqlite3", dbPath+"?_busy_timeout=5000&_journal_mode=WAL")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := migrations.RunMigrations(db, migrations.SQLiteMigrationsFS(), "sqlite"); err != nil {
		t.Fatal(err)
	}
	return &SQLiteStore{db: db, path: dbPath}, db
}

func seedStaleTask(t *testing.T, db *sql.DB, now time.Time) {
	t.Helper()
	old := now.Add(-time.Minute).Format(time.RFC3339Nano)
	_, err := db.Exec(`
		INSERT INTO tasks (task_id, job_id, status, revision, attempt_count,
		                   worker_id, lease_id, lease_expires_at, created_at, updated_at)
		VALUES ('stale-task', 'missing-job', 'RUNNING', 1, 1,
		        'worker-stale', 'lease-stale', ?, ?, ?)`, old, old, old)
	if err != nil {
		t.Fatal(err)
	}
}

func TestStaleExecutionReconciler_DryRunDoesNotMutate(t *testing.T) {
	store, db := openStaleReconcilerTestDB(t)
	now := time.Now().UTC()
	seedStaleTask(t, db, now)
	reconciler, err := NewStaleExecutionReconciler(store)
	if err != nil {
		t.Fatal(err)
	}

	report, err := reconciler.Reconcile(context.Background(), now, 100, false, "operator-test")
	if err != nil {
		t.Fatal(err)
	}
	if report.Mode != "dry-run" || len(report.Findings) == 0 {
		t.Fatalf("unexpected dry-run report: %+v", report)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM tasks WHERE task_id='stale-task'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "RUNNING" {
		t.Fatalf("dry-run mutated task status: %q", status)
	}
	var audits int
	if err := db.QueryRow(`SELECT COUNT(*) FROM audit_events`).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if audits != 0 {
		t.Fatalf("dry-run wrote %d audit events", audits)
	}
}

func TestStaleExecutionReconciler_ApplyIsIdempotent(t *testing.T) {
	store, db := openStaleReconcilerTestDB(t)
	now := time.Now().UTC()
	seedStaleTask(t, db, now)
	reconciler, err := NewStaleExecutionReconciler(store)
	if err != nil {
		t.Fatal(err)
	}

	first, err := reconciler.Reconcile(context.Background(), now, 100, true, "operator-test")
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Applied) != 1 {
		t.Fatalf("first apply applied=%d report=%+v", len(first.Applied), first)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM tasks WHERE task_id='stale-task'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "CANCELLED" {
		t.Fatalf("orphan task status=%q, want CANCELLED", status)
	}
	var audits int
	if err := db.QueryRow(`SELECT COUNT(*) FROM audit_events WHERE action='STALE_EXECUTION_RECONCILED'`).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if audits != 1 {
		t.Fatalf("audit count after first apply=%d, want 1", audits)
	}

	second, err := reconciler.Reconcile(context.Background(), now.Add(time.Second), 100, true, "operator-test")
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Applied) != 0 {
		t.Fatalf("second apply changed %d findings: %+v", len(second.Applied), second)
	}
	var auditsAfter int
	if err := db.QueryRow(`SELECT COUNT(*) FROM audit_events WHERE action='STALE_EXECUTION_RECONCILED'`).Scan(&auditsAfter); err != nil {
		t.Fatal(err)
	}
	if auditsAfter != audits {
		t.Fatalf("audit count grew on idempotent rerun: %d -> %d", audits, auditsAfter)
	}
}

func TestStaleExecutionReconciler_ExpiredLeaseIsAuditedAndClosesAttempt(t *testing.T) {
	store, db := openStaleReconcilerTestDB(t)
	now := time.Now().UTC()
	old := now.Add(-time.Minute).Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO jobs (job_id,status,max_retries,created_at,updated_at,migrated_at,revision) VALUES ('job-lease','RUNNING',3,?,?,?,0)`, old, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO tasks (task_id,job_id,status,revision,attempt_count,attempt_id,attempt_number,worker_id,lease_id,lease_expires_at,created_at,updated_at) VALUES ('task-lease','job-lease','RUNNING',1,1,'attempt-lease',1,'worker-lease','lease-1',?,?,?)`, old, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO task_attempts (id,task_id,job_id,attempt_number,worker_id,lease_id,status,report_version,created_at,updated_at) VALUES ('attempt-lease','task-lease','job-lease',1,'worker-lease','lease-1','RUNNING',0,?,?)`, old, old); err != nil {
		t.Fatal(err)
	}
	reconciler, err := NewStaleExecutionReconciler(store)
	if err != nil {
		t.Fatal(err)
	}
	report, err := reconciler.Reconcile(context.Background(), now, 100, true, "operator-test")
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Applied) != 1 {
		t.Fatalf("applied=%d report=%+v", len(report.Applied), report)
	}
	var taskStatus, attemptStatus string
	if err := db.QueryRow(`SELECT status FROM tasks WHERE task_id='task-lease'`).Scan(&taskStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status FROM task_attempts WHERE id='attempt-lease'`).Scan(&attemptStatus); err != nil {
		t.Fatal(err)
	}
	if taskStatus != "READY" || attemptStatus != "TIMED_OUT" {
		t.Fatalf("task/attempt statuses=%s/%s", taskStatus, attemptStatus)
	}
	var audits int
	if err := db.QueryRow(`SELECT COUNT(*) FROM audit_events WHERE resource_id='task-lease'`).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if audits != 1 {
		t.Fatalf("lease audit count=%d", audits)
	}
}

func TestStaleExecutionReconciler_OfflineWorkerIsPartitionedIdempotently(t *testing.T) {
	store, db := openStaleReconcilerTestDB(t)
	now := time.Now().UTC()
	old := now.Add(-20 * time.Minute).Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO workers (worker_id,worker_name,status,raw_json,migrated_at,connection_state,last_heartbeat_at,last_state_change_at) VALUES ('worker-offline','worker-offline','READY','{}',?,?,?,?)`, old, old, old, old); err != nil {
		t.Fatal(err)
	}
	reconciler, err := NewStaleExecutionReconciler(store)
	if err != nil {
		t.Fatal(err)
	}
	report, err := reconciler.Reconcile(context.Background(), now, 100, true, "operator-test")
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Applied) != 1 {
		t.Fatalf("offline applied=%d report=%+v", len(report.Applied), report)
	}
	var state string
	if err := db.QueryRow(`SELECT connection_state FROM workers WHERE worker_id='worker-offline'`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "PARTITIONED" {
		t.Fatalf("worker state=%q", state)
	}
	// Offline marking must not revoke a still-valid lease; this fixture has
	// no active task, so the assertion is represented by the unchanged count.
	second, err := reconciler.Reconcile(context.Background(), now.Add(time.Minute), 100, true, "operator-test")
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Applied) != 0 {
		t.Fatalf("second offline apply=%d", len(second.Applied))
	}
}

func TestStaleExecutionReconciler_AuditIsAppendOnly(t *testing.T) {
	store, db := openStaleReconcilerTestDB(t)
	now := time.Now().UTC()
	seedStaleTask(t, db, now)
	reconciler, err := NewStaleExecutionReconciler(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), now, 100, true, "operator-test"); err != nil {
		t.Fatal(err)
	}
	var id string
	if err := db.QueryRow(`SELECT id FROM audit_events LIMIT 1`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE audit_events SET action='tampered' WHERE id=?`, id); err == nil {
		t.Fatal("audit update unexpectedly succeeded")
	}
	if _, err := db.Exec(`DELETE FROM audit_events WHERE id=?`, id); err == nil {
		t.Fatal("audit delete unexpectedly succeeded")
	}
}
