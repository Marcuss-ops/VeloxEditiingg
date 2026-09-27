package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

func TestMergeTaskSpecPayloadWaitsForConcurrentSQLiteWriter(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "task-spec-runtime.db") + "?_busy_timeout=5000&_journal_mode=WAL"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(4)
	if _, err := db.Exec(`
		CREATE TABLE tasks (
			task_id TEXT PRIMARY KEY,
			job_id TEXT NOT NULL,
			executor_id TEXT NOT NULL,
			status TEXT NOT NULL,
			updated_at TEXT NOT NULL
		);
		CREATE TABLE task_specs (
			task_id TEXT PRIMARY KEY,
			spec_version INTEGER NOT NULL,
			spec_hash TEXT NOT NULL,
			payload_json TEXT NOT NULL
		);
		INSERT INTO tasks VALUES ('task-1','job-1','scene.composite.v1','READY','2026-09-27T00:00:00Z');
		INSERT INTO task_specs VALUES ('task-1',1,'old-hash','{"scenes_json":"[]"}');
	`); err != nil {
		t.Fatal(err)
	}

	writer, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err := writer.ExecContext(context.Background(), `BEGIN IMMEDIATE`); err != nil {
		t.Fatal(err)
	}

	repo := NewSQLiteTaskRepository(&SQLiteStore{db: db})
	done := make(chan error, 1)
	go func() {
		_, mergeErr := repo.MergeTaskSpecPayload(context.Background(), "task-1", map[string]interface{}{
			"runtime_assets_pending": false,
			"runtime_assets":         []interface{}{map[string]interface{}{"asset_id": "drive-stock-1"}},
		})
		done <- mergeErr
	}()

	select {
	case err := <-done:
		t.Fatalf("merge returned before the competing writer released its lock: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := writer.ExecContext(context.Background(), `COMMIT`); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("merge after writer release: %v", err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("merge did not finish after the competing writer released its lock")
	}

	var pending bool
	var encoded, hash string
	if err := db.QueryRow(`SELECT json_extract(payload_json,'$.runtime_assets_pending'),payload_json,spec_hash FROM task_specs WHERE task_id='task-1'`).Scan(&pending, &encoded, &hash); err != nil {
		t.Fatal(err)
	}
	if pending {
		t.Fatal("runtime_assets_pending remained true after merge")
	}
	if hash == "" || hash == "old-hash" {
		t.Fatalf("spec hash was not refreshed: %q", hash)
	}
}
