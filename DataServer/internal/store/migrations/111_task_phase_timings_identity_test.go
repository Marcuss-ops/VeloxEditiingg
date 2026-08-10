package migrations

import (
	"database/sql"
	_ "embed"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

//go:embed sqlite/042_task_phase_timings.sql
var sqliteSQL042TaskPhaseTimings string

//go:embed sqlite/070_engine_phase_metrics.sql
var sqliteSQL070EnginePhaseMetrics string

//go:embed sqlite/111_task_phase_timings_identity.sql
var sqliteSQL111TaskPhaseTimingsIdentity string

func applyTaskPhaseTimingIdentityMigrations(t *testing.T, db *sql.DB) {
	t.Helper()
	applyMigrationSQL(t, db, sqliteSQL042TaskPhaseTimings)
	// Migration 070 also extends the metrics table, which is unrelated to
	// this schema contract but required as a prerequisite by that migration.
	if _, err := db.Exec(`CREATE TABLE task_attempt_metrics (attempt_id TEXT PRIMARY KEY)`); err != nil {
		t.Fatalf("create task_attempt_metrics prerequisite: %v", err)
	}
	// Migration 070 owns component/action, which migration 111 indexes.
	// The test applies only the schema prerequisites needed for this contract.
	applyMigrationSQL(t, db, sqliteSQL070EnginePhaseMetrics)
	applyMigrationSQL(t, db, sqliteSQL111TaskPhaseTimingsIdentity)
}

func TestMigration111_TaskPhaseTimingsCanonicalIdentityColumns(t *testing.T) {
	db := openTestDB(t)
	applyTaskPhaseTimingIdentityMigrations(t, db)

	for _, column := range []string{
		"job_id", "task_id", "worker_id", "worker_snapshot_id",
		"executor_id", "executor_version",
	} {
		if !columnExists(t, db, "task_phase_timings", column) {
			t.Errorf("task_phase_timings column %q is missing", column)
		}
	}
	if !indexExists(t, db, "idx_task_phase_timings_identity") {
		t.Fatal("idx_task_phase_timings_identity is missing")
	}

	if _, err := db.Exec(`
		INSERT INTO task_phase_timings (attempt_id, phase, duration_ms)
		VALUES ('attempt-defaults', 'render', 10)`); err != nil {
		t.Fatalf("insert default phase timing: %v", err)
	}
	var jobID, taskID, workerID, snapshotID, executorID string
	var executorVersion int
	if err := db.QueryRow(`
		SELECT job_id, task_id, worker_id, worker_snapshot_id,
		       executor_id, executor_version
		FROM task_phase_timings WHERE attempt_id = 'attempt-defaults'`).Scan(
		&jobID, &taskID, &workerID, &snapshotID, &executorID, &executorVersion,
	); err != nil {
		t.Fatalf("read identity defaults: %v", err)
	}
	if jobID != "" || taskID != "" || workerID != "" || snapshotID != "" || executorID != "" || executorVersion != 0 {
		t.Fatalf("identity defaults = %q/%q/%q/%q/%q/%d; want empty/0", jobID, taskID, workerID, snapshotID, executorID, executorVersion)
	}
}
