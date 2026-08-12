package migrations

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
)

// ============================================================
// Migration 151: worker_deployment_state legacy backfill
// ============================================================

// applyProductionMigrationsUpTo applies the REAL production migration chain
// (SQLiteMigrationsFS, "sqlite" dir) up to and including maxVersion. It
// reuses the shared apply-up-to-N loop so a test can build a legacy database
// exactly as an upgraded production install would look before migration 151
// runs.
func applyProductionMigrationsUpTo(t *testing.T, db *sql.DB, maxVersion int) {
	t.Helper()
	applyMigrationsUpToFS(t, db, SQLiteMigrationsFS(), "sqlite", maxVersion)
}

func migrationTestDigest(c byte) string {
	return "sha256:" + strings.Repeat(string(c), 64)
}

// seedLegacyWorker inserts the canonical workers row that deployment_records
// FK-references (PRAGMA foreign_keys=ON in openTestDB).
func seedLegacyWorker(t *testing.T, db *sql.DB, workerID string) {
	t.Helper()
	if _, err := db.Exec(
		`INSERT INTO workers (worker_id, worker_name, node_role, raw_json, migrated_at) VALUES (?, ?, 'worker', '{}', datetime('now'))`,
		workerID, workerID+"-vps",
	); err != nil {
		t.Fatalf("seed worker %s: %v", workerID, err)
	}
}

// seedLegacyDeploymentRecord inserts a pre-151 deployment_records row. The
// table does NOT yet have error_message (migration 151 adds it), so the
// insert mirrors the post-134 schema exactly.
func seedLegacyDeploymentRecord(t *testing.T, db *sql.DB, deploymentID, workerID, previous, target, startedAt, finishedAt, status string) {
	t.Helper()
	var prev interface{}
	if previous != "" {
		prev = previous
	}
	var finished interface{}
	if finishedAt != "" {
		finished = finishedAt
	}
	if _, err := db.Exec(`
INSERT INTO deployment_records
  (deployment_id, worker_id, previous_digest, target_digest, started_at, finished_at, status, applied_by, is_rollback)
VALUES (?, ?, ?, ?, ?, ?, ?, 'legacy-fleetctl', 0)`,
		deploymentID, workerID, prev, target, startedAt, finished, status,
	); err != nil {
		t.Fatalf("seed deployment record %s: %v", deploymentID, err)
	}
}

// TestMigration151_LegacyBackfillPolicy is the canonical legacy upgrade
// scenario from the fleet persistent-state spec (§21):
//
//	op1: target=A SUCCEEDED  (older)
//	op2: target=B FAILED     (newer)
//
// After migration 151 the read model must be:
//
//	desired_digest         = B   ← latest record target (surviving intent)
//	running_digest         = NULL ← never invented from history
//	last_successful_digest = A   ← latest verified digest survives the failure
//	last_operation         = FAILED (op2)
//
// running_digest stays NULL until the first authenticated heartbeat — the
// migration must NOT fabricate it from deployment history.
func TestMigration151_LegacyBackfillPolicy(t *testing.T) {
	db := openTestDB(t)
	applyProductionMigrationsUpTo(t, db, 150)

	seedLegacyWorker(t, db, "wicket")
	digestA := migrationTestDigest('a')
	digestB := migrationTestDigest('b')
	seedLegacyDeploymentRecord(t, db, "deploy-success-old", "wicket", "", digestA,
		"2026-08-01T00:00:00Z", "2026-08-01T00:01:00Z", "SUCCEEDED")
	seedLegacyDeploymentRecord(t, db, "deploy-failed-new", "wicket", digestA, digestB,
		"2026-08-02T00:00:00Z", "2026-08-02T00:01:00Z", "FAILED")

	applyProductionMigrationsUpTo(t, db, 151)

	var (
		desired, lastSuccess, opID, opKind, opStatus, opError string
		running                                             sql.NullString
	)
	err := db.QueryRow(`
SELECT desired_digest, running_digest, last_successful_digest,
       last_operation_id, last_operation_kind, last_operation_status,
       last_operation_error
  FROM worker_deployment_state WHERE worker_id = 'wicket'`).Scan(
		&desired, &running, &lastSuccess, &opID, &opKind, &opStatus, &opError,
	)
	if err != nil {
		t.Fatalf("query worker_deployment_state: %v", err)
	}
	if desired != digestB {
		t.Errorf("desired_digest = %q, want %q (latest record target)", desired, digestB)
	}
	if running.Valid {
		t.Errorf("running_digest = %q, want NULL (must not be invented from history)", running.String)
	}
	if lastSuccess != digestA {
		t.Errorf("last_successful_digest = %q, want %q (latest verified digest)", lastSuccess, digestA)
	}
	if opID != "deploy-failed-new" {
		t.Errorf("last_operation_id = %q, want deploy-failed-new", opID)
	}
	if opKind != "update" {
		t.Errorf("last_operation_kind = %q, want update", opKind)
	}
	if opStatus != "FAILED" {
		t.Errorf("last_operation_status = %q, want FAILED", opStatus)
	}
	if opError != "" {
		t.Errorf("last_operation_error = %q, want empty for pre-151 record", opError)
	}

	// The column itself must be nullable (NULL is the "not yet observed"
	// state, distinct from an empty-string deployment intent).
	if !columnAllowsNull(t, db, "worker_deployment_state", "running_digest") {
		t.Error("running_digest column must be nullable after migration 151")
	}
}

// TestMigration151_LegacyBackfillLatestWinsByDeploymentID pins the
// deterministic "latest record" selection (§22): when two rows share the
// same started_at, the deployment_id tiebreak decides, so the same legacy
// DB always projects the same read model.
func TestMigration151_LegacyBackfillLatestWinsByDeploymentID(t *testing.T) {
	db := openTestDB(t)
	applyProductionMigrationsUpTo(t, db, 150)

	seedLegacyWorker(t, db, "wicket")
	digestA := migrationTestDigest('a')
	digestB := migrationTestDigest('b')
	// Identical started_at; the tiebreak must pick deploy-bb (higher id).
	seedLegacyDeploymentRecord(t, db, "deploy-aa", "wicket", "", digestA,
		"2026-08-01T00:00:00Z", "2026-08-01T00:01:00Z", "SUCCEEDED")
	seedLegacyDeploymentRecord(t, db, "deploy-bb", "wicket", digestA, digestB,
		"2026-08-01T00:00:00Z", "2026-08-01T00:01:00Z", "FAILED")

	applyProductionMigrationsUpTo(t, db, 151)

	var opID, desired string
	if err := db.QueryRow(`SELECT last_operation_id, desired_digest FROM worker_deployment_state WHERE worker_id = 'wicket'`).Scan(&opID, &desired); err != nil {
		t.Fatalf("query worker_deployment_state: %v", err)
	}
	if opID != "deploy-bb" {
		t.Errorf("last_operation_id = %q, want deploy-bb (deployment_id tiebreak)", opID)
	}
	if desired != digestB {
		t.Errorf("desired_digest = %q, want %q (tiebreak target)", desired, digestB)
	}
}

// TestMigration151_FreshSchemaHasNullableRunningDigest verifies the fresh
// install shape: the table exists, running_digest is nullable, and no state
// rows are backfilled when there is no deployment history.
func TestMigration151_FreshSchemaHasNullableRunningDigest(t *testing.T) {
	db := openTestDB(t)
	if err := RunMigrations(db, SQLiteMigrationsFS(), "sqlite"); err != nil {
		t.Fatalf("production RunMigrations on fresh DB: %v", err)
	}
	if !tableExists(t, db, "worker_deployment_state") {
		t.Fatal("worker_deployment_state missing after full chain")
	}
	if !columnExists(t, db, "worker_deployment_state", "running_digest") {
		t.Fatal("running_digest column missing")
	}
	if !columnAllowsNull(t, db, "worker_deployment_state", "running_digest") {
		t.Error("running_digest must be nullable on a fresh install")
	}
	var stateCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM worker_deployment_state`).Scan(&stateCount); err != nil {
		t.Fatalf("count state rows: %v", err)
	}
	if stateCount != 0 {
		t.Errorf("fresh install has %d state rows, want 0 (no history to backfill)", stateCount)
	}
}

// columnAllowsNull reports whether the given table column is nullable
// (notnull == 0 in PRAGMA table_info).
func columnAllowsNull(t *testing.T, db *sql.DB, table, column string) bool {
	t.Helper()
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		t.Fatalf("PRAGMA table_info(%s): %v", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			cid, notnull, pk int
			name, colType    string
			dflt             sql.NullString
		)
		if err := rows.Scan(&cid, &name, &colType, &notnull, &dflt, &pk); err != nil {
			t.Fatalf("scan PRAGMA table_info(%s): %v", table, err)
		}
		if name == column {
			return notnull == 0
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate PRAGMA table_info(%s): %v", table, err)
	}
	return false
}

// ============================================================
// Migration 152: worker_deployment_state.last_phase
// ============================================================

// TestMigration152_AddsLastPhaseColumn pins migration 152: the
// worker_deployment_state table gains a NOT NULL DEFAULT '' last_phase
// observability column, and legacy backfilled state rows carry the empty
// default (the migration must not invent a phase for historical data).
func TestMigration152_AddsLastPhaseColumn(t *testing.T) {
	db := openTestDB(t)
	// Seed the legacy history BEFORE 151 so its backfill produces the state
	// row that 152 then extends with last_phase (same ordering as
	// TestMigration151_LegacyBackfillPolicy above).
	applyProductionMigrationsUpTo(t, db, 150)

	seedLegacyWorker(t, db, "wicket")
	seedLegacyDeploymentRecord(t, db, "deploy-legacy-a", "wicket", "", migrationTestDigest('a'),
		"2026-08-01T00:00:00Z", "2026-08-01T00:01:00Z", "SUCCEEDED")

	applyProductionMigrationsUpTo(t, db, 152)

	var lastPhase string
	if err := db.QueryRow(
		`SELECT last_phase FROM worker_deployment_state WHERE worker_id = 'wicket'`).Scan(&lastPhase); err != nil {
		t.Fatalf("query worker_deployment_state.last_phase: %v", err)
	}
	if lastPhase != "" {
		t.Errorf("last_phase after migration = %q, want '' (no invented phase for legacy rows)", lastPhase)
	}
	// The column is NOT NULL DEFAULT '' — a direct insert without it stays valid.
	if _, err := db.Exec(`INSERT INTO worker_deployment_state
		(worker_id, desired_digest, running_digest, last_successful_digest,
		 last_operation_id, last_operation_kind, last_operation_status,
		 last_operation_error, updated_at)
		VALUES ('wicket-2', 'x', NULL, '', '', '', '', '', datetime('now'))`); err != nil {
		t.Fatalf("insert without last_phase must use the DEFAULT: %v", err)
	}
}

