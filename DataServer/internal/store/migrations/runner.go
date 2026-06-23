// Package migrations / runner.go
//
// Single source of truth for the embedded FS instances used by the
// two-backend (SQLite + Postgres) dialect-aware migration runner.
// Replaces the historical embed.go (SQLite-only at the package root)
// and postgres_runner.go (Postgres-only under postgres/) so callers no
// longer have to know which file holds which dialect's FS.
//
// Layout served:
//
//	sqlite/   — SQLite-cumulative migration files (45 .sql, evolution
//	            from 001_initial through 045 — see migrations/sqlite/
//	            for the canonical ordering; per-version historical
//	            context lives in git log, not in this doc).
//	postgres/ — Postgres-native migration files (10 .sql — see
//	            migrations/postgres/ for the canonical ordering; same
//	            per-version scope caveat as sqlite/).
//
// Accessors:
//
//	SQLiteMigrationsFS() — caller-facing accessor for the SQLite FS.
//	                      Embedded under sqlite/*.sql. Use this for
//	                      production boot paths and any new code
//	                      (the only callsite in DataServer is
//	                      internal/store/sqlite.go::NewSQLiteStoreFromHandle).
//	PostgresMigrationsFS() — caller-facing accessor for the Postgres FS.
//	                        Embedded under postgres/*.sql.
package migrations

import (
	"embed"
)

//go:embed sqlite/*.sql
var sqliteRootFS embed.FS

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
