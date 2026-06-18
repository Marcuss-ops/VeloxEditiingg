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
