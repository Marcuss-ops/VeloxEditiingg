// Package migrations / down.go
//
// Rollback-status introspection surface. RunDown was retired in this
// split because it had no production consumers (only a test-only
// round-trip check in migrations_down_roundtrip_test.go) and the user
// explicitly instructed the removal of test-only public symbols
// rather than carrying them forward "for possible future use".
//
// What remains here: MigrationStatus + ListMigrationStatus — the
// post-apply introspection helpers that operators and CI monitoring
// use to validate an environment's migration-recording state. The
// naming reflects the historical rollback checkpoint it evolved from;
// no live DOWN code path runs on master.
package migrations

import (
	"database/sql"
	"embed"
	"strings"
)

// MigrationStatus represents the status of a single migration.
type MigrationStatus struct {
	Version  int
	Name     string
	Status   string // "applied", "pending", "checksum_mismatch"
	Checksum string
}

// ListMigrationStatus returns the full status of all embedded migrations.
// Tolerant of a missing schema_migrations table (returns all pending
// in that case so a partially-bootstrapped environment doesn't 500).
func ListMigrationStatus(db *sql.DB, fs embed.FS, dir string) ([]MigrationStatus, error) {
	all, err := discoverMigrations(fs, dir)
	if err != nil {
		return nil, err
	}
	applied, err := listApplied(db)
	if err != nil {
		// If the table doesn't exist, nothing is applied.
		if strings.Contains(err.Error(), "no such table") {
			applied = map[int]appliedMigration{}
		} else {
			return nil, err
		}
	}

	result := make([]MigrationStatus, 0, len(all))
	for _, m := range all {
		ms := MigrationStatus{
			Version:  m.Version,
			Name:     m.Name,
			Checksum: m.Checksum,
		}
		if a, ok := applied[m.Version]; ok {
			if a.Checksum == m.Checksum {
				ms.Status = "applied"
			} else {
				ms.Status = "checksum_mismatch"
			}
		} else {
			ms.Status = "pending"
		}
		result = append(result, ms)
	}
	return result, nil
}
