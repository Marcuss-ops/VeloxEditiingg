package store

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func seedWorker(t *testing.T, db *sql.DB, workerID, state, heartbeat string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO workers (worker_id, worker_name, status, raw_json, migrated_at, connection_state, last_heartbeat_at, last_state_change_at)
		VALUES (?, ?, ?, '{}', ?, ?, ?, ?)`, workerID, workerID, state, heartbeat, state, heartbeat, heartbeat); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerLostReconciler_PartitionsStaleWorker(t *testing.T) {
	store, db := openReconciliationTestDB(t)
	now := time.Now().UTC()
	old := now.Add(-1 * time.Hour).Format(time.RFC3339Nano)
	seedWorker(t, db, "worker-lost", "CONNECTED", old)

	r, err := NewWorkerLostReconciler(store, 5, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Reconcile(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	var state string
	if err := db.QueryRow(`SELECT connection_state FROM workers WHERE worker_id='worker-lost'`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "PARTITIONED" {
		t.Fatalf("connection_state = %q, want PARTITIONED", state)
	}

	// Second pass is a no-op (already PARTITIONED).
	if err := r.Reconcile(context.Background(), now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	var stateAfter string
	if err := db.QueryRow(`SELECT connection_state FROM workers WHERE worker_id='worker-lost'`).Scan(&stateAfter); err != nil {
		t.Fatal(err)
	}
	if stateAfter != "PARTITIONED" {
		t.Fatalf("connection_state after second pass = %q, want PARTITIONED (idempotent)", stateAfter)
	}
}

func TestWorkerLostReconciler_SkipsFreshWorker(t *testing.T) {
	store, db := openReconciliationTestDB(t)
	now := time.Now().UTC()
	fresh := now.Add(-5 * time.Second).Format(time.RFC3339Nano)
	seedWorker(t, db, "worker-fresh", "CONNECTED", fresh)

	r, err := NewWorkerLostReconciler(store, 5, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Reconcile(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	var state string
	if err := db.QueryRow(`SELECT connection_state FROM workers WHERE worker_id='worker-fresh'`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "CONNECTED" {
		t.Fatalf("connection_state = %q, want CONNECTED (fresh worker untouched)", state)
	}
}

func TestWorkerLostReconciler_SkipsAlreadyPartitioned(t *testing.T) {
	store, db := openReconciliationTestDB(t)
	now := time.Now().UTC()
	old := now.Add(-1 * time.Hour).Format(time.RFC3339Nano)
	seedWorker(t, db, "worker-part", "PARTITIONED", old)

	r, err := NewWorkerLostReconciler(store, 5, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Reconcile(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	var state string
	if err := db.QueryRow(`SELECT connection_state FROM workers WHERE worker_id='worker-part'`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "PARTITIONED" {
		t.Fatalf("connection_state = %q, want PARTITIONED", state)
	}
	// The audit event must not duplicate for an already-partitioned worker.
	var events int
	if err := db.QueryRow(`SELECT COUNT(*) FROM worker_events WHERE worker_id='worker-part'`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 0 {
		t.Fatalf("worker_events = %d, want 0 (no re-detection for already-partitioned worker)", events)
	}
}

func TestWorkerLostReconciler_NilStoreFailsClosed(t *testing.T) {
	if _, err := NewWorkerLostReconciler(nil, 5, 10); err == nil {
		t.Fatal("nil store must be rejected (fail-closed capability contract)")
	}
}
