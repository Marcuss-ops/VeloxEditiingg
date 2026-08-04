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

// legacyChecksums contains exact checksums for historical files whose
// version remains embedded but whose responsibilities changed. These are
// narrow compatibility exceptions, not a general checksum bypass.
var legacyChecksums = map[int]map[string]struct{}{
	1: {
		// Original 001_initial.sql before the Dark Editor tables were removed.
		"90d2c1512ac2954c7b201c62b2abe3ba2b9f7b478c88880e56d64906b7deee8d": {},
	},
	40: {
		// Retired testdata/040_stub_dark_editor_projects.sql. Current v40
		// creates task_specs; migration 129 repairs that table for installs
		// that recorded the retired v40 checksum.
		"b2095b5cf5342e67301c1638c83a4c9f4df7da6e37bb4523de70f8d635e6e4b4": {},
	},
}

// retiredMigrationChecksums contains exact checksums for migrations that
// are intentionally no longer embedded. Their schema_migrations rows are
// preserved, but arbitrary missing versions fail closed so checksum
// integrity is not weakened by silently ignoring unknown history.
var retiredMigrationChecksums = map[int]map[string]struct{}{
	44: {
		// Original sqlite/044_stub_dark_editor_projects.sql.
		"40e9918c77bd3cec4e734e51deb95bd4cf43ac73f67904968e01595934d6214f": {},
	},
}

func isAcceptedLegacyChecksum(version int, checksum string) bool {
	_, ok := legacyChecksums[version][checksum]
	return ok
}

func isAcceptedRetiredMigration(version int, checksum string) bool {
	_, ok := retiredMigrationChecksums[version][checksum]
	return ok
}

func validateAppliedMigrationSet(applied map[int]appliedMigration, discovered []Migration) error {
	discoveredVersions := make(map[int]struct{}, len(discovered))
	for _, m := range discovered {
		discoveredVersions[m.Version] = struct{}{}
	}
	for version, prev := range applied {
		if _, ok := discoveredVersions[version]; ok {
			continue
		}
		if !isAcceptedRetiredMigration(version, prev.Checksum) {
			return fmt.Errorf("migrations: applied migration %03d is no longer embedded with an authorized checksum; refusing to ignore unknown migration history", version)
		}
	}
	return nil
}

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

	if err := validateAppliedMigrationSet(applied, migs); err != nil {
		return err
	}

	for _, m := range migs {
		if prev, ok := applied[m.Version]; ok {
			if prev.Checksum != m.Checksum && !isAcceptedLegacyChecksum(m.Version, prev.Checksum) {
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
