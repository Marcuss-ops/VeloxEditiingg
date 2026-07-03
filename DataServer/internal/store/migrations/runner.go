// Package migrations / runner.go
//
// RunMigrations is the canonical entry point for the SQLite-+-Postgres
// migration runner. It composes the discovery (embed.FS file scan +
// schema_migrations tracking-table ensure) and the apply-loop (per-
// migration tx + pre-flight gates) from sibling files in this package.
//
// Layout served:
//
//	sqlite/    — SQLite-cumulative .sql files. The only callsite in
//	             production is internal/store/sqlite.go::NewSQLiteStoreFromHandle.
//	postgres/  — Postgres-native .sql files.
//
// The embed.FS directives + SQLiteMigrationsFS / PostgresMigrationsFS
// accessors live in discovery.go (where migrations are sourced). This
// file owns the orchestrator only.
//
// Note: EnsureApplied was previously exposed here; it was retired in
// this split because it had no production consumers (only test
// coverage). RunDown was likewise retired — see down.go.
package migrations

import (
	"database/sql"
	"embed"
	"fmt"
	"log"
)

// RunMigrations discovers and applies all pending embedded migrations.
// It creates the schema_migrations table if it doesn't exist, then applies
// each migration that hasn't been run yet, in version order.
//
// Sole public orchestrator for production boot paths (NewSQLiteStore ->
// NewSQLiteStoreFromHandle) and tests. Calls EnsureSchemaTable,
// discoverMigrations, listApplied, MustDropLegacyOrchestrator,
// MustEnsureNoStorageKeyDuplicates, and applyMigration — all defined
// in sibling files in this package.
func RunMigrations(db *sql.DB, migrationsFS embed.FS, dir string) error {
	if err := EnsureSchemaTable(db); err != nil {
		return fmt.Errorf("migrations: ensure schema table: %w", err)
	}

	migs, err := discoverMigrations(migrationsFS, dir)
	if err != nil {
		return fmt.Errorf("migrations: discover: %w", err)
	}
	if len(migs) == 0 {
		return nil
	}

	applied, err := listApplied(db)
	if err != nil {
		return fmt.Errorf("migrations: list applied: %w", err)
	}

	for _, m := range migs {
		if prev, ok := applied[m.Version]; ok {
			if prev.Checksum != m.Checksum {
				return fmt.Errorf(
					"migrations: checksum mismatch for %03d_%s: was %s, now %s. "+
						"Never modify an applied migration — create a new one instead",
					m.Version, m.Name, prev.Checksum, m.Checksum,
				)
			}
			continue
		}

		// Pre-flight check before destructive migrations. Today this fires for
		// 028_legacy_drop (workflow_v2 precondition) and 029_artifact_uploads
		// (artifacts.storage_key uniqueness precondition).
		if err := MustDropLegacyOrchestrator(db, m.Version); err != nil {
			return fmt.Errorf("migrations: pre_check %03d_%s: %w", m.Version, m.Name, err)
		}
		if err := MustEnsureNoStorageKeyDuplicates(db, m.Version); err != nil {
			return fmt.Errorf("migrations: pre_check %03d_%s: %w", m.Version, m.Name, err)
		}

		if err := applyMigration(db, m); err != nil {
			return fmt.Errorf("migrations: apply %03d_%s: %w", m.Version, m.Name, err)
		}
		log.Printf("[MIGRATE] Applied %03d_%s (checksum: %s)", m.Version, m.Name, m.Checksum)
	}

	return nil
}
