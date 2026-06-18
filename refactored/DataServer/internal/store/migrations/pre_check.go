// Package migrations — pre-flight checks that must run BEFORE applying a SQL
// migration that otherwise has no-go SQL guard (SQLite's RAISE() can only be
// called from inside a trigger, so per-row precondition enforcement belongs
// here, not in the .sql file).
//
// Today there is exactly one such check:
//   * 028_legacy_drop.sql refuses to drop orchestrator_jobs /
//     orchestrator_outbox unconditionally until we confirm workflow_runs has
//     absorbed the legacy data. The check distinguishes three cases:
//     1. workflow_runs already has rows                    → DROP safely.
//     2. workflow_runs empty AND orchestrator_* empty      → DROP safely
//        (typical fresh install after running 001..027 alone).
//     3. workflow_runs empty AND orchestrator_* non-empty  → REFUSE with a
//        hard error that tells the operator exactly what to run next.
//     4. workflow_runs does not exist yet (older install)   → TREAT like
//        case 3 (data would be orphaned).
package migrations

import (
	"database/sql"
	"errors"
	"fmt"
)

// ErrLegacyOrchestratorNotMigrated is returned when 028 has legacy data that
// has not yet been lifted into workflow_runs. Operators must run the
// workflow migrator before the legacy orchestrator tables can be dropped.
var ErrLegacyOrchestratorNotMigrated = errors.New(
	"migrations: legacy orchestrator data is still present and workflow_runs is empty; " +
		"run `velox-server migrate workflows-v2 --apply` before upgrading past 028",
)

// MustDropLegacyOrchestrator is invoked by RunMigrations before migration 028
// to enforce the precondition that workflow_runs has either (a) absorbed the
// legacy orchestrator data, or (b) the legacy tables are empty / did not
// exist yet.
//
// Returns nil when the DROP can proceed; returns ErrLegacyOrchestratorNotMigrated
// when the data is still in the orchestrator_* tables and the workflow_v2
// migration has not been run yet. Other errors are infrastructure failures
// (e.g. SQL driver errors) and bubble up unchanged.
func MustDropLegacyOrchestrator(db *sql.DB, version int) error {
	if version != 28 {
		// Defence in depth — this check is only meaningful for 028_legacy_drop.
		return nil
	}
	if db == nil {
		return fmt.Errorf("migrations: pre_check: nil db handle")
	}

	workflowRunsExists, err := tableExistsOrError(db, "workflow_runs")
	if err != nil {
		return fmt.Errorf("migrations: pre_check: workflow_runs lookup: %w", err)
	}
	var workflowRows int64
	if workflowRunsExists {
		if err := db.QueryRow(`SELECT COUNT(*) FROM workflow_runs`).Scan(&workflowRows); err != nil {
			return fmt.Errorf("migrations: pre_check: count workflow_runs: %w", err)
		}
	}

	orchExists, err := tableExistsOrError(db, "orchestrator_jobs")
	if err != nil {
		return fmt.Errorf("migrations: pre_check: orchestrator_jobs lookup: %w", err)
	}
	var orchRows int64
	if orchExists {
		if err := db.QueryRow(`SELECT COUNT(*) FROM orchestrator_jobs`).Scan(&orchRows); err != nil {
			return fmt.Errorf("migrations: pre_check: count orchestrator_jobs: %w", err)
		}
	}

	// CASE A — at least one workflow_run row exists.
	// The workflow_v2 migrator already absorbed the legacy data, we can
	// safely drop the orchestrator tables.
	if workflowRows > 0 {
		return nil
	}

	// CASE B — fresh install path: workflow_runs empty AND orchestrator_*
	// empty (or not present). The DROP statements in 028_legacy_drop are
	// idempotent thanks to IF EXISTS, so we just proceed.
	if orchRows == 0 {
		return nil
	}

	// CASE C — workflow_runs empty AND orchestrator_jobs has data.
	// Refuse with an explicit operator-facing message so production
	// incidents do not silently lose data.
	return fmt.Errorf("%w (%d orphan rows in orchestrator_jobs, %d workflow_runs)",
		ErrLegacyOrchestratorNotMigrated, orchRows, workflowRows)
}

// ErrStorageKeyDuplicates is returned when migration 030 would attempt to
// CREATE UNIQUE INDEX on artifacts(storage_provider, storage_key) but legacy
// rows already contain duplicates for that pair. Operators must clean up
// duplicates before migration 030 can proceed.
var ErrStorageKeyDuplicates = errors.New(
	"migrations: artifacts(storage_provider, storage_key) has duplicates; " +
		"run `SELECT storage_provider, storage_key, COUNT(*) FROM artifacts " +
		"WHERE storage_key <> '' GROUP BY 1,2 HAVING COUNT(*) > 1` to inspect, " +
		"delete or coalesce the duplicates, then re-run migrations",
)

// MustEnsureNoStorageKeyDuplicates is invoked by RunMigrations before
// migration 030 to enforce the precondition that no two rows in
// `artifacts` share the same (storage_provider, storage_key) pair.
//
// Migration 030_artifact_uploads.sql installs:
//
//	CREATE UNIQUE INDEX idx_artifacts_storage_key
//	  ON artifacts(storage_provider, storage_key)
//	  WHERE storage_key <> '';
//
// SQLite refuses to apply that CREATE UNIQUE INDEX if duplicates already
// exist for the partial key. We surface those duplicates up-front so the
// operator gets a clean error (with a sample of the offending rows)
// instead of a generic SQLite constraint failure.
//
// Returns nil when no duplicates exist; returns ErrStorageKeyDuplicates
// (wrapped with COUNT and a sample) when they do. Versions other than 30
// are no-ops so this hook can sit in the RunMigrations loop without
// touching unrelated migrations.
func MustEnsureNoStorageKeyDuplicates(db *sql.DB, version int) error {
	if version != 30 {
		// Defence in depth — only meaningful for 030_artifact_uploads.
		return nil
	}
	if db == nil {
		return fmt.Errorf("migrations: pre_check: nil db handle")
	}

	exists, err := tableExistsOrError(db, "artifacts")
	if err != nil {
		return fmt.Errorf("migrations: pre_check: artifacts lookup: %w", err)
	}
	if !exists {
		// Pre-029 schema (older install) — no rows to conflict against.
		return nil
	}

	// Count groups that have >1 row.
	var dupGroups int64
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM (
			SELECT storage_provider, storage_key
			FROM artifacts
			WHERE storage_key <> ''
			GROUP BY storage_provider, storage_key
			HAVING COUNT(*) > 1
		)`).Scan(&dupGroups); err != nil {
		return fmt.Errorf("migrations: pre_check: count duplicates: %w", err)
	}
	if dupGroups == 0 {
		return nil
	}

	// Pull a sample of the offending rows for the operator runbook —
	// we want the message to be actionable, not a bare count.
	rows, err := db.Query(`
		SELECT storage_provider, storage_key, COUNT(*) AS n, MIN(id) AS sample_id
		FROM artifacts
		WHERE storage_key <> ''
		GROUP BY storage_provider, storage_key
		HAVING COUNT(*) > 1
		ORDER BY n DESC, storage_key ASC
		LIMIT 10`)
	if err != nil {
		return fmt.Errorf("migrations: pre_check: sample duplicates: %w", err)
	}
	defer rows.Close()

	type dupSample struct {
		provider   string
		key        string
		count      int64
		sampleID   string
	}
	var samples []dupSample
	for rows.Next() {
		var s dupSample
		if err := rows.Scan(&s.provider, &s.key, &s.count, &s.sampleID); err != nil {
			return fmt.Errorf("migrations: pre_check: scan sample: %w", err)
		}
		samples = append(samples, s)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("migrations: pre_check: rows err: %w", err)
	}

	return fmt.Errorf("%w (%d duplicate groups; first 10: %v)",
		ErrStorageKeyDuplicates, dupGroups, samples)
}

// tableExists is a small helper that survives older SQLite snapshots where
// sqlite_master exists but the named table does not.
//
// Note: SELECT COUNT(*) always returns a row (the count, possibly 0), so this
// helper cannot surface sql.ErrNoRows — driver errors are propagated verbatim.
//
// Renamed from `tableExists` to `tableExistsOrError` here to avoid colliding
// with the test-local `tableExists` in migrations_test.go which has a
// different (t, db, table) signature.
func tableExistsOrError(db *sql.DB, name string) (bool, error) {
	var count int
	err := db.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?`,
		name,
	).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}
