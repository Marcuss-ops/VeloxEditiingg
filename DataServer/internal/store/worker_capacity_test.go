package store

import (
	"context"
	"database/sql"
	"testing"
)

func newWorkerCapacityTestDB(t *testing.T) *SQLiteStore {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE tasks (
		task_id TEXT PRIMARY KEY,
		worker_id TEXT,
		status TEXT,
		lease_id TEXT,
		lease_expires_at TEXT
	)`); err != nil {
		t.Fatal(err)
	}
	return &SQLiteStore{db: db}
}

func TestGetWorkerCapacity_CountsOnlyCurrentLeases(t *testing.T) {
	s := newWorkerCapacityTestDB(t)
	_, err := s.db.Exec(`INSERT INTO tasks(task_id,worker_id,lease_id,status,lease_expires_at) VALUES
		('active-leased','w1','lease-1','LEASED','2026-08-05T12:10:00Z'),
		('active-running','w1','lease-2','RUNNING','2026-08-05T12:10:00Z'),
		('expired','w1','lease-3','RUNNING','2026-08-05T11:00:00Z'),
		('ready','w1','','READY','2026-08-05T12:10:00Z'),
		('other','w2','lease-4','RUNNING','2026-08-05T12:10:00Z')`)
	if err != nil {
		t.Fatal(err)
	}
	row, err := s.GetWorkerCapacity(context.Background(), "w1", "2026-08-05T12:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	if row.ActiveSlots != 2 {
		t.Fatalf("ActiveSlots = %d, want 2", row.ActiveSlots)
	}
}

func TestGetWorkerCapacities_BulkIncludesZeroWorkers(t *testing.T) {
	s := newWorkerCapacityTestDB(t)
	if _, err := s.db.Exec(`INSERT INTO tasks(task_id,worker_id,lease_id,status,lease_expires_at) VALUES ('active','w1','lease-1','RUNNING','2026-08-05T12:10:00Z')`); err != nil {
		t.Fatal(err)
	}
	rows, err := s.GetWorkerCapacities(context.Background(), []string{"w1", "w2"}, "2026-08-05T12:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	if rows["w1"].ActiveSlots != 1 || rows["w2"].ActiveSlots != 0 {
		t.Fatalf("capacities = %#v, want w1=1,w2=0", rows)
	}
}
