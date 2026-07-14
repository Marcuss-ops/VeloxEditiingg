// Package migrations / discovery.go
//
// Embedded migration FS, the canonical Migration type, and the helpers
// that scan the embed.FS for SQL files plus query the schema_migrations
// tracking table.
//
// Concrete on SQLite + embed.FS (no abstract framework). The
// discoverMigrations + listApplied / appliedMigration helpers are the
// read-side of the migration lifecycle; EnsureSchemaTable bootstraps
// the schema_migrations table on first contact; splitStatements is a
// minimal per-statement splitter shared by the apply-loop.
package migrations

import (
	"crypto/sha256"
	"database/sql"
	"embed"
	"fmt"
	"path"
	"sort"
	"strings"
)

// SQLite migrations live under sqlite/*.sql.
//
//go:embed sqlite/*.sql
var sqliteRootFS embed.FS

// Postgres migrations live under postgres/*.sql.
//
//go:embed postgres/*.sql
var postgresRootFS embed.FS

// SQLiteMigrationsFS exposes the embedded SQLite migration files to
// callers outside the migrations package (notably internal/store/sqlite.go
// via NewSQLiteStore and the platform/database tests). Exposed via a
// function (rather than a package var promotion) so the embed directive
// in this file remains the single source of truth. The dir parameter
// passed to RunMigrations should be "sqlite".
func SQLiteMigrationsFS() embed.FS { return sqliteRootFS }

// PostgresMigrationsFS exposes the embedded Postgres migration files.
// Same rationale as SQLiteMigrationsFS — function-based export keeps
// the embed directive as the single source of truth. RunMigrations dir
// should be "postgres".
func PostgresMigrationsFS() embed.FS { return postgresRootFS }

// Migration represents a single versioned schema migration.
type Migration struct {
	Version  int
	Name     string
	SQL      string
	Checksum string // SHA256 hex of the SQL content
}

// appliedMigration is the on-disk shape persisted to schema_migrations.
type appliedMigration struct {
	Version  int
	Checksum string
}

// EnsureSchemaTable creates the schema_migrations tracking table if
// it does not yet exist. Idempotent.
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

// discoverMigrations reads all .sql files from the embedded FS, parses
// version/name, and returns them sorted by version.
//
// DOWN pairing (.down.sql) and non-sql entries are skipped — DOWN
// migrations are explicit rollback scripts invoked by RunDown, not
// applied at startup.
func discoverMigrations(fs embed.FS, dir string) ([]Migration, error) {
	entries, err := fs.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read migrations dir %s: %w", dir, err)
	}

	var migs []Migration
	seenVersions := make(map[int]string) // version → first filename

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") || strings.HasSuffix(e.Name(), ".down.sql") {
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

// splitStatements splits a SQL migration into individual statements on
// semicolons, ignoring semicolons inside SQL comments.
func splitStatements(sql string) []string {
	var stmts []string
	var current strings.Builder
	for _, line := range strings.Split(sql, "\n") {
		trimmed := strings.TrimSpace(line)
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
