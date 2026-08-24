package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"velox-server/internal/taskgraph"
)

func TestFutureReservations_ConcurrentWorkersHaveExactlyOneOwner(t *testing.T) {
	db, err := sql.Open("sqlite3", "file:future-reservation-concurrent-owner?mode=memory&cache=shared&_busy_timeout=5000")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(8)
	_, err = db.Exec(`CREATE TABLE tasks(task_id TEXT PRIMARY KEY); CREATE TABLE task_specs(task_id TEXT PRIMARY KEY,payload_json TEXT); CREATE TABLE future_task_reservations(task_id TEXT PRIMARY KEY,job_id TEXT NOT NULL,worker_id TEXT NOT NULL,reservation_id TEXT NOT NULL UNIQUE,task_revision INTEGER,distance INTEGER,expires_at TEXT,created_at TEXT,updated_at TEXT);`)
	if err != nil {
		t.Fatal(err)
	}
	repo := NewSQLiteTaskRepository(&SQLiteStore{db: db})
	reservation := func(worker string) taskgraph.FutureReservation {
		return taskgraph.FutureReservation{
			TaskID: "task-race", JobID: "job-race", WorkerID: worker,
			ReservationID: "reservation-" + worker, TaskRevision: 1, Distance: 1,
			ExpiresAt: time.Now().UTC().Add(time.Minute),
		}
	}

	const workers = 16
	results := make(chan bool, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ok, err := repo.TryReserveFutureTask(context.Background(), reservation(fmt.Sprintf("worker-%02d", i)))
			results <- ok
			errs <- err
		}(i)
	}
	wg.Wait()
	close(results)
	close(errs)

	successes := 0
	for ok := range results {
		if ok {
			successes++
		}
	}
	lockFailures := 0
	for err := range errs {
		if err == nil {
			continue
		}
		if strings.Contains(err.Error(), "database table is locked") {
			lockFailures++
			continue
		}
		t.Fatalf("concurrent reservation returned unexpected error: %v", err)
	}
	// SQLite can reject every simultaneous writer before its INSERT CAS
	// executes, even with busy_timeout. Recover outside the contention
	// window; the retry is still checked against the same unique task key.
	if successes == 0 && lockFailures > 0 {
		ok, err := repo.TryReserveFutureTask(context.Background(), reservation("worker-retry"))
		if err != nil {
			t.Fatalf("reservation retry after SQLite lock contention: %v", err)
		}
		if ok {
			successes = 1
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent reservation successes=%d (lock failures=%d), want exactly 1", successes, lockFailures)
	}

	rows, err := repo.ListFutureReservations(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].TaskID != "task-race" {
		t.Fatalf("authoritative reservation rows=%+v, want one owner", rows)
	}
}
