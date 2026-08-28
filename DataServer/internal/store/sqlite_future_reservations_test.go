package store

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"velox-server/internal/taskgraph"
)

func TestFutureReservations_AreExclusiveAndReconciled(t *testing.T) {
	db, err := sql.Open("sqlite3", "file:future-reservation-test?mode=memory&cache=shared&_busy_timeout=5000")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE tasks(task_id TEXT PRIMARY KEY); CREATE TABLE task_specs(task_id TEXT PRIMARY KEY,payload_json TEXT); CREATE TABLE future_task_reservations(task_id TEXT PRIMARY KEY,job_id TEXT NOT NULL,worker_id TEXT NOT NULL,reservation_id TEXT NOT NULL UNIQUE,task_revision INTEGER,distance INTEGER, state TEXT NOT NULL DEFAULT '',expires_at TEXT,created_at TEXT,updated_at TEXT);`)
	if err != nil {
		t.Fatal(err)
	}
	repo := NewSQLiteTaskRepository(&SQLiteStore{db: db})
	ctx := context.Background()
	expiry := time.Now().UTC().Add(time.Minute)
	a := taskgraph.FutureReservation{TaskID: "t1", JobID: "j1", WorkerID: "worker-a", ReservationID: "r-a", Distance: 1, ExpiresAt: expiry}
	ok, err := repo.TryReserveFutureTask(ctx, a)
	if err != nil || !ok {
		t.Fatalf("reserve A: ok=%v err=%v", ok, err)
	}
	b := a
	b.WorkerID = "worker-b"
	b.ReservationID = "r-b"
	ok, err = repo.TryReserveFutureTask(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("worker B stole worker A reservation")
	}
	rows, err := repo.ListFutureReservations(ctx, "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ReservationID != "r-a" {
		t.Fatalf("reservations=%+v", rows)
	}
	if err := repo.ReconcileFutureReservations(ctx, "worker-a", nil); err != nil {
		t.Fatal(err)
	}
	rows, err = repo.ListFutureReservations(ctx, "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("reconcile did not release: %+v", rows)
	}
}

func TestFutureReservations_TransferUsesOwnerCAS(t *testing.T) {
	db, err := sql.Open("sqlite3", "file:future-reservation-transfer-test?mode=memory&cache=shared&_busy_timeout=5000")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE tasks(task_id TEXT PRIMARY KEY); CREATE TABLE task_specs(task_id TEXT PRIMARY KEY,payload_json TEXT); CREATE TABLE future_task_reservations(task_id TEXT PRIMARY KEY,job_id TEXT NOT NULL,worker_id TEXT NOT NULL,reservation_id TEXT NOT NULL UNIQUE,task_revision INTEGER,distance INTEGER, state TEXT NOT NULL DEFAULT '',expires_at TEXT,created_at TEXT,updated_at TEXT);`)
	if err != nil {
		t.Fatal(err)
	}
	repo := NewSQLiteTaskRepository(&SQLiteStore{db: db})
	ctx := context.Background()
	first := taskgraph.FutureReservation{TaskID: "t1", JobID: "j1", WorkerID: "worker-a", ReservationID: "r-a", Distance: 1, ExpiresAt: time.Now().UTC().Add(time.Minute)}
	if ok, err := repo.TryReserveFutureTask(ctx, first); err != nil || !ok {
		t.Fatalf("initial reservation: ok=%v err=%v", ok, err)
	}
	fallback := first
	fallback.WorkerID = "worker-b"
	fallback.ReservationID = "r-b"
	fallback.ExpiresAt = time.Now().UTC().Add(2 * time.Minute)
	if ok, err := repo.TransferFutureTask(ctx, "t1", "worker-a", fallback); err != nil || !ok {
		t.Fatalf("transfer: ok=%v err=%v", ok, err)
	}
	rows, err := repo.ListFutureReservations(ctx, "worker-b")
	if err != nil || len(rows) != 1 || rows[0].ReservationID != "r-b" {
		t.Fatalf("transferred reservation=%+v err=%v", rows, err)
	}
	stale := first
	stale.WorkerID = "worker-c"
	stale.ReservationID = "r-c"
	if ok, err := repo.TransferFutureTask(ctx, "t1", "worker-a", stale); err != nil || ok {
		t.Fatalf("stale owner transfer: ok=%v err=%v, want false", ok, err)
	}
}
