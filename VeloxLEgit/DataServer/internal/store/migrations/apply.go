// Package migrations / apply.go
//
// Forward (UP) migration consumers. RunMigrations (in runner.go)
// drives the apply loop and calls into applyMigration here for the
// single-migration transaction; AppliedVersions / PendingVersions
// expose the post-apply state for ops + monitoring.
//
// The pre-flight helpers MustDropLegacyOrchestrator and
// MustEnsureNoStorageKeyDuplicates (called per-migration from
// RunMigrations) live elsewhere in this package.
package migrations

import (
	"database/sql"
	"embed"
	"fmt"
	"strings"
	"time"
)

// applyMigration runs a single migration inside a transaction and records it
// in schema_migrations. Tolerates already-applied ALTER TABLE ADD COLUMN
// (duplicate-column) and DROP COLUMN (no-such-column) errors so partial
// boots or Path-B rollouts don't crash the master.
func applyMigration(db *sql.DB, m Migration) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	stmts := splitStatements(m.SQL)
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			// Tolerate "no such column" for DROP COLUMN — the column
			// may have been removed by a prior partial run or never existed.
			if strings.Contains(strings.ToLower(stmt), "drop column") &&
				strings.Contains(strings.ToLower(err.Error()), "no such column") {
				continue
			}
			// Tolerate "duplicate column name" for ADD COLUMN — the column
			// may have been added by a prior partial run (a previous
			// transaction committed before INSERT INTO schema_migrations
			// succeeded) or by a sister migration on a parallel dialect
			// track. Concretely this unblocks the Path B rollout: any
			// pre-Path-B production DB that already applied the legacy
			// migrations/039_add_job_required_resource_columns.sql sees
			// the duplicate-column error here on its first boot through
			// migrations.SQLiteMigrationsFS(), where the renamed
			// 045_add_job_required_resource_columns.sql replays the same
			// ALTER TABLE ADD COLUMN. Without this pass-through those
			// boots would abort.
			if strings.Contains(strings.ToLower(stmt), "alter table") &&
				strings.Contains(strings.ToLower(stmt), "add column") &&
				strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
				continue
			}
			return fmt.Errorf("execute migration: %w", err)
		}
	}

	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := tx.Exec(
		`INSERT INTO schema_migrations (version, name, checksum, applied_at) VALUES (?, ?, ?, ?)`,
		m.Version, m.Name, m.Checksum, now,
	); err != nil {
		return fmt.Errorf("record migration: %w", err)
	}

	return tx.Commit()
}

// AppliedVersions returns the list of applied migration version numbers
// in ascending order.
func AppliedVersions(db *sql.DB) ([]int, error) {
	rows, err := db.Query(`SELECT version FROM schema_migrations ORDER BY version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var versions []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		versions = append(versions, v)
	}
	return versions, rows.Err()
}

// PendingVersions returns the migrations that exist in the embedded FS
// but haven't yet been applied.
func PendingVersions(db *sql.DB, fs embed.FS, dir string) ([]Migration, error) {
	all, err := discoverMigrations(fs, dir)
	if err != nil {
		return nil, err
	}
	applied, err := listApplied(db)
	if err != nil {
		return nil, err
	}

	var pending []Migration
	for _, m := range all {
		if _, ok := applied[m.Version]; !ok {
			pending = append(pending, m)
		}
	}
	return pending, nil
}
