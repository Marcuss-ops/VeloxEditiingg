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
//   sqlite/   — SQLite-cumulative migration files (38 .sql, evolution
//               from 001_initial through 038_drop_jobs_raw_json).
//   postgres/ — Postgres-native migration files (10 .sql, 001_initial
//               through 010_drive). Consolidated Phase 2 artifacts +
//               jobs PG DDLs were folded into 006_artifacts.sql and
//               002_jobs.sql respectively; the previous Phase 2 PG
//               files deleted in the platform/database migration.
//
// Accessors:
//
//   SQLiteMigrationsFS() — caller-facing accessor for the SQLite FS.
//                         Embedded under sqlite/*.sql.
//   PostgresMigrationsFS() — caller-facing accessor for the Postgres FS.
//                           Embedded under postgres/*.sql.
//   MigrationsFS — backward-compat alias returning the SQLite FS.
//                  Pre-platform/database callers reached for this
//                  var directly; it stays so old callers keep
//                  compiling without re-test rewrites.
package migrations

import (
	"embed"
)

//go:embed sqlite/*.sql
var sqliteRootFS embed.FS

//go:embed postgres/*.sql
var postgresRootFS embed.FS

// MigrationsFS is provided by embed.go (the tracked file).
// This file adds dialect-specific accessors for the two-backend world.
//
// New code should prefer SQLiteMigrationsFS() so the dialect intent is
// explicit at the call site.

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
