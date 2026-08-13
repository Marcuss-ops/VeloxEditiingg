package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestBuildReconciliationRegistryStaleExecutionConverges(t *testing.T) {
	dbStore, err := NewSQLiteStore(filepath.Join(t.TempDir(), "stale-reconcile.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer dbStore.Close()

	now := time.Now().UTC()
	old := now.Add(-time.Minute).Format(time.RFC3339Nano)
	if _, err := dbStore.DB().Exec(`
		INSERT INTO tasks (task_id, job_id, status, revision, attempt_count,
		                   worker_id, lease_id, lease_expires_at, created_at, updated_at)
		VALUES ('bootstrap-stale-task', 'missing-job', 'RUNNING', 1, 1,
		        'worker-stale', 'lease-stale', ?, ?, ?)`, old, old, old); err != nil {
		t.Fatal(err)
	}

	registry, err := BuildReconciliationRegistry(dbStore, 0, 0, 10, "test-reconciliation")
	if err != nil {
		t.Fatal(err)
	}

	first := registry.Reconcile(context.Background(), now)
	if err := first.Err(); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := dbStore.DB().QueryRow(`SELECT status FROM tasks WHERE task_id='bootstrap-stale-task'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "CANCELLED" {
		t.Fatalf("task status=%q, want CANCELLED", status)
	}

	second := registry.Reconcile(context.Background(), now.Add(time.Second))
	if err := second.Err(); err != nil {
		t.Fatal(err)
	}
	var audits int
	if err := dbStore.DB().QueryRow(`SELECT COUNT(*) FROM audit_events WHERE action='STALE_EXECUTION_RECONCILED'`).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if audits != 1 {
		t.Fatalf("audit count=%d after convergent rerun, want 1", audits)
	}
}
