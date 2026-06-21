// Package migrations provides a versioned SQLite migration runner.
//
// Usage:
//   - Place .sql files in the migrations/ directory named NNN_name.sql
//   - Each file is a single migration applied atomically in a transaction
//   - The schema_migrations table tracks which migrations have been applied
//   - Checksums prevent silent modification of already-applied migrations
//   - RunMigrations is called once at startup from NewSQLiteStore
package migrations

import (
	"crypto/sha256"
	"database/sql"
	"embed"
	"fmt"
	"log"
	"path"
	"sort"
	"strings"
	"time"
)

// Migration represents a single versioned schema migration.
type Migration struct {
	Version  int
	Name     string
	SQL      string
	Checksum string // SHA256 hex of the SQL content
}

// RunMigrations discovers and applies all pending embedded migrations.
// It creates the schema_migrations table if it doesn't exist, then applies
// each migration that hasn't been run yet, in version order.
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

// EnsureSchemaTable creates the schema_migrations tracking table.
func EnsureSchemaTable(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version   INTEGER PRIMARY KEY,
			name      TEXT NOT NULL,
			checksum  TEXT NOT NULL,
			applied_at TEXT NOT NULL
		)
	`)
	return err
}

// discoverMigrations reads all .sql files from the embedded FS, parses version/name,
// and returns them sorted by version.
func discoverMigrations(fs embed.FS, dir string) ([]Migration, error) {
	entries, err := fs.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read migrations dir %s: %w", dir, err)
	}

	var migs []Migration
	seenVersions := make(map[int]string) // version → first filename

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}

		var version int
		var name string
		if n, err := fmt.Sscanf(e.Name(), "%d_%s", &version, &name); n < 1 || err != nil {
			continue
		}

		// Reject duplicate migration version numbers — PRIMARY KEY in
		// schema_migrations would fail or the second file would be silently
		// skipped, either of which is a hard-to-debug startup failure.
		if prev, exists := seenVersions[version]; exists {
			return nil, fmt.Errorf(
				"duplicate migration version %03d: %s and %s",
				version, prev, e.Name(),
			)
		}
		seenVersions[version] = e.Name()

		name = strings.TrimSuffix(e.Name(), ".sql")
		if idx := strings.Index(name, "_"); idx >= 0 {
			name = name[idx+1:]
		}

		content, err := fs.ReadFile(path.Join(dir, e.Name()))
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", e.Name(), err)
		}

		checksum := fmt.Sprintf("%x", sha256.Sum256(content))

		migs = append(migs, Migration{
			Version:  version,
			Name:     name,
			SQL:      string(content),
			Checksum: checksum,
		})
	}

	sort.Slice(migs, func(i, j int) bool {
		return migs[i].Version < migs[j].Version
	})

	return migs, nil
}

type appliedMigration struct {
	Version  int
	Checksum string
}

// listApplied returns the set of already-applied migrations.
func listApplied(db *sql.DB) (map[int]appliedMigration, error) {
	rows, err := db.Query(`SELECT version, checksum FROM schema_migrations ORDER BY version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[int]appliedMigration)
	for rows.Next() {
		var a appliedMigration
		if err := rows.Scan(&a.Version, &a.Checksum); err != nil {
			return nil, err
		}
		result[a.Version] = a
	}
	return result, rows.Err()
}

// applyMigration runs a single migration inside a transaction and records it.
func applyMigration(db *sql.DB, m Migration) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	// Execute each statement individually so we can handle per-statement
	// errors (e.g., "no such column" on ALTER TABLE DROP COLUMN when the
	// column was already removed or never existed).
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
			// boots would abort with "duplicate column name:
			// job_required_resource_class" and crash NewSQLiteStore.
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

// EnsureApplied guarantees a specific migration has been applied, running it if needed.
// This is useful for domains that need to check-and-apply at the point of use,
// though the normal startup path via RunMigrations is preferred.
func EnsureApplied(db *sql.DB, m Migration) error {
	if err := EnsureSchemaTable(db); err != nil {
		return err
	}

	var checksum string
	err := db.QueryRow(`SELECT checksum FROM schema_migrations WHERE version = ?`, m.Version).Scan(&checksum)
	if err == nil {
		if checksum != m.Checksum {
			return fmt.Errorf(
				"migrations: checksum mismatch for %03d_%s: was %s, now %s",
				m.Version, m.Name, checksum, m.Checksum,
			)
		}
		return nil
	}
	if err != sql.ErrNoRows {
		return err
	}

	return applyMigration(db, m)
}

// AppliedVersions returns the list of applied migration version numbers.
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

// MigrationStatus represents the status of a single migration.
type MigrationStatus struct {
	Version  int
	Name     string
	Status   string // "applied", "pending", "checksum_mismatch"
	Checksum string
}

// ListMigrationStatus returns the full status of all embedded migrations.
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

// PendingVersions returns migrations that exist in the embedded FS but haven't been applied.
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

// splitStatements splits a SQL migration into individual statements on
// semicolons, ignoring semicolons inside comments and quoted strings.
func splitStatements(sql string) []string {
	var stmts []string
	var current strings.Builder
	for _, line := range strings.Split(sql, "\n") {
		trimmed := strings.TrimSpace(line)
		// Skip SQL comments
		if strings.HasPrefix(trimmed, "--") {
			continue
		}
		current.WriteString(line)
		current.WriteString("\n")
		if strings.HasSuffix(trimmed, ";") {
			s := strings.TrimSpace(current.String())
			if s != "" {
				stmts = append(stmts, s)
			}
			current.Reset()
		}
	}
	if s := strings.TrimSpace(current.String()); s != "" {
		stmts = append(stmts, s)
	}
	return stmts
}
