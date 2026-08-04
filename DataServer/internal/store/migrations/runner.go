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

// legacyChecksums pins the exact checksums of the pre-Dark-Editor-exit
// variants of the historical migrations that were deliberately amended
// when the Dark Editor domain was removed from Velox:
//
//   - 001_initial.sql no longer creates the dark_editor_* tables
//     (fresh databases never see them);
//
// Installations that already applied the ORIGINAL variant hold this
// legacy checksum in schema_migrations. Accepting exactly this value
// preserves their upgrade path to 128 without weakening checksum
// validation for any other migration or any future edit — a recorded
// checksum that matches neither the on-disk content nor this allowlist
// still fails boot. See migrations/README.md §Dark Editor domain exit.
var legacyChecksums = map[int]map[string]struct{}{
	1: {
		// sha256 of the ORIGINAL 001_initial.sql (pre dark-editor removal).
		"90d2c1512ac2954c7b201c62b2abe3ba2b9f7b478c88880e56d64906b7deee8d": {},
	},
}

// isAcceptedLegacyChecksum reports whether the recorded (already-applied)
// checksum for a version is one of the sanctioned pre-dark-editor-exit
// values, in which case the mismatch against the amended on-disk file is
// tolerated once so the install can continue to the 128 drop.
func isAcceptedLegacyChecksum(version int, checksum string) bool {
	_, ok := legacyChecksums[version][checksum]
	return ok
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
