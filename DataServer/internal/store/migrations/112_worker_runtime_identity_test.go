package migrations

import (
	"database/sql"
	"strings"
	"testing"

	_ "embed"

	_ "github.com/mattn/go-sqlite3"
)

//go:embed sqlite/109_worker_runtime_snapshots.sql
var sqliteSQL109WorkerRuntimeSnapshots string

//go:embed sqlite/112_worker_runtime_identity.sql
var sqliteSQL112WorkerRuntimeIdentity string

func applyWorkerRuntimeIdentityMigrations(t *testing.T, db *sql.DB) {
	t.Helper()

	// Migration 109 is applied against the smallest schema that its
	// ALTER TABLE and index statements require. This keeps the test
	// focused on the 109 → 112 upgrade contract instead of coupling it
	// to unrelated historical migrations.
	if _, err := db.Exec(`
		CREATE TABLE task_attempts (
			id TEXT PRIMARY KEY
		)`); err != nil {
		t.Fatalf("create task_attempts fixture: %v", err)
	}
	applyMigrationSQL(t, db, sqliteSQL109WorkerRuntimeSnapshots)
	applyMigrationSQL(t, db, sqliteSQL112WorkerRuntimeIdentity)
}

func TestMigration112_WorkerRuntimeIdentityColumns(t *testing.T) {
	db := openTestDB(t)
	applyWorkerRuntimeIdentityMigrations(t, db)

	expectedColumns := []string{
		"snapshot_id",
		"worker_id",
		"session_id",
		"hostname",
		"node_id",
		"worker_name",
		"worker_class",
		"rollout_group",
		"git_sha",
		"worker_version",
		"bundle_version",
		"bundle_hash",
		"engine_version",
		"ffmpeg_version",
		"protocol_version",
		"config_hash",
		"docker_image_digest",
		"cpu_model",
		"logical_cpu_count",
		"effective_cpu_count",
		"cpu_quota",
		"total_memory_bytes",
		"gpu_model",
		"gpu_driver",
		"kernel_version",
		"os_release",
		"storage_class",
		"capabilities_json",
		"connected_at",
		"disconnected_at",
	}
	for _, column := range expectedColumns {
		if !columnExists(t, db, "worker_runtime_snapshots", column) {
			t.Errorf("worker_runtime_snapshots column %q is missing", column)
		}
	}

	for _, column := range []string{"worker_session_id", "worker_snapshot_id"} {
		if !columnExists(t, db, "task_attempts", column) {
			t.Errorf("task_attempts column %q is missing", column)
		}
	}
}

func TestMigration112_WorkerSessionUniquenessAndIndexes(t *testing.T) {
	db := openTestDB(t)
	applyWorkerRuntimeIdentityMigrations(t, db)

	for _, index := range []string{
		"uq_worker_runtime_snapshots_worker_session",
		"idx_worker_runtime_snapshots_worker",
		"idx_worker_runtime_snapshots_session",
		"idx_worker_snapshot_version",
		"idx_attempt_worker_snapshot",
		"idx_task_attempts_worker_session",
	} {
		if !indexExists(t, db, index) {
			t.Errorf("expected index %q", index)
		}
	}

	const insert = `
		INSERT INTO worker_runtime_snapshots (
			snapshot_id, worker_id, session_id, connected_at
		) VALUES (?, ?, ?, ?)`
	if _, err := db.Exec(insert, "snapshot-1", "worker-1", "session-1", "2026-07-31T00:00:00Z"); err != nil {
		t.Fatalf("insert first runtime snapshot: %v", err)
	}

	if _, err := db.Exec(insert, "snapshot-2", "worker-1", "session-1", "2026-07-31T00:01:00Z"); err == nil {
		t.Fatal("expected duplicate worker/session pair to be rejected")
	} else if !strings.Contains(strings.ToLower(err.Error()), "unique") {
		t.Fatalf("duplicate worker/session error = %v, want UNIQUE violation", err)
	}

	if _, err := db.Exec(insert, "snapshot-3", "worker-1", "session-2", "2026-07-31T00:02:00Z"); err != nil {
		t.Fatalf("same worker with a distinct session should be allowed: %v", err)
	}
	if _, err := db.Exec(insert, "snapshot-4", "worker-2", "session-1", "2026-07-31T00:03:00Z"); err != nil {
		t.Fatalf("distinct worker with a reused session should be allowed: %v", err)
	}
}

func TestMigration112_WorkerRuntimeIdentityDefaultsAndIntegrity(t *testing.T) {
	db := openTestDB(t)
	applyWorkerRuntimeIdentityMigrations(t, db)

	if _, err := db.Exec(`
		INSERT INTO worker_runtime_snapshots (
			snapshot_id, worker_id, session_id, connected_at
		) VALUES (?, ?, ?, ?)`,
		"snapshot-defaults", "worker-defaults", "session-defaults", "2026-07-31T00:00:00Z",
	); err != nil {
		t.Fatalf("insert defaults row: %v", err)
	}

	var capabilities, workerClass, disconnectedAt sql.NullString
	var logicalCPUs, effectiveCPUs, memoryBytes int
	var cpuQuota float64
	if err := db.QueryRow(`
		SELECT capabilities_json, worker_class, disconnected_at,
		       logical_cpu_count, effective_cpu_count,
		       total_memory_bytes, cpu_quota
		  FROM worker_runtime_snapshots
		 WHERE snapshot_id = ?`, "snapshot-defaults").Scan(
		&capabilities, &workerClass, &disconnectedAt,
		&logicalCPUs, &effectiveCPUs, &memoryBytes, &cpuQuota,
	); err != nil {
		t.Fatalf("read default runtime snapshot fields: %v", err)
	}
	if capabilities.String != "{}" {
		t.Errorf("capabilities_json default = %q, want {}", capabilities.String)
	}
	if workerClass.String != "" {
		t.Errorf("worker_class default = %q, want empty", workerClass.String)
	}
	if disconnectedAt.Valid {
		t.Errorf("disconnected_at default should be NULL, got %q", disconnectedAt.String)
	}
	if logicalCPUs != 0 || effectiveCPUs != 0 || memoryBytes != 0 || cpuQuota != 0 {
		t.Errorf("hardware defaults = logical=%d effective=%d memory=%d quota=%v, want all zero", logicalCPUs, effectiveCPUs, memoryBytes, cpuQuota)
	}

	var integrity string
	if err := db.QueryRow(`PRAGMA integrity_check`).Scan(&integrity); err != nil {
		t.Fatalf("PRAGMA integrity_check: %v", err)
	}
	if integrity != "ok" {
		t.Fatalf("PRAGMA integrity_check = %q, want ok", integrity)
	}
}
