package metrics

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func openLegacyLabelResolverDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	const schema = `
CREATE TABLE task_attempts (
	id TEXT PRIMARY KEY,
	task_id TEXT NOT NULL,
	worker_id TEXT NOT NULL
);
CREATE TABLE tasks (
	task_id TEXT PRIMARY KEY,
	executor_id TEXT NOT NULL DEFAULT '',
	executor_version INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE workers (
	worker_id TEXT PRIMARY KEY,
	worker_class TEXT NOT NULL DEFAULT ''
);
`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	return db
}

func TestSQLiteLabelResolver_Labels_FallsBackToLegacyWorkerClass(t *testing.T) {
	db := openLegacyLabelResolverDB(t)
	ctx := context.Background()

	if _, err := db.ExecContext(ctx,
		`INSERT INTO task_attempts (id, task_id, worker_id) VALUES (?, ?, ?);
		 INSERT INTO tasks (task_id, executor_id, executor_version) VALUES (?, ?, ?);
		 INSERT INTO workers (worker_id, worker_class) VALUES (?, ?)`,
		"attempt-1", "task-1", "worker-1",
		"task-1", "scene.composite.v1", 1,
		"worker-1", "mixed",
	); err != nil {
		t.Fatalf("seed rows: %v", err)
	}

	r := NewSQLiteLabelResolver(db)
	execID, execVer, workerClass, err := r.Labels(ctx, "attempt-1")
	if err != nil {
		t.Fatalf("Labels: %v", err)
	}
	if execID != "scene.composite.v1" {
		t.Fatalf("execID = %q; want scene.composite.v1", execID)
	}
	if execVer != "1" {
		t.Fatalf("execVer = %q; want 1", execVer)
	}
	if workerClass != "mixed" {
		t.Fatalf("workerClass = %q; want mixed", workerClass)
	}
}
