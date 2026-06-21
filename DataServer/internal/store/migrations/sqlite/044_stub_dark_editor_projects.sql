-- 044_stub_dark_editor_projects.sql
--
-- Stubs the dark_editor_projects table so NewSQLiteStore's
-- postMigrationAdjustments (sqlite.go::store.SQLiteStore) can run
-- against a fresh test DB without failing on the first
-- ensureColumn("dark_editor_projects", "folder_id", "TEXT") call.
--
-- Without this migration any test factory that calls NewSQLiteStore
-- (e.g. contracts/jobs_repository_contract_test.go's
-- NewSQLiteJobRepositoryFactory) propagates the failure through
-- the postMigrationAdjustments guard, blocking every contract test
-- that exercises CreateJob / Get / ClaimNext round-trips — including
-- the 3 new ClaimResult.Requirements contract subtests added in the
-- PR-04.6 followup.
--
-- PLACEMENT NOTE (Path B resolution): relocated from
-- migrations/040_stub_dark_editor_projects.sql to
-- migrations/sqlite/044_stub_dark_editor_projects.sql so the
-- recursive embed `//go:embed sqlite/*.sql` (declared in
-- migrations/runner.go and exposed via migrations.SQLiteMigrationsFS())
-- picks it up. Version 040 on this track is already occupied by
-- 040_task_specs.sql, so 044 is the next free slot after
-- 043_task_attempt_metrics.sql at the time of the move. The version
-- ordering remains monotonic — 044 runs after all 001-043 entries —
-- and the stub is idempotent (CREATE TABLE IF NOT EXISTS) with no
-- dependency on later migrations, so it can be renumbered upward
-- without breaking the schema end-state.
--
-- Future contributors creating migration 045+:
--   - Place new .sql files under migrations/sqlite/ — that's the
--     dialect-aware / recursive embed path sqlite.go now uses
--     (`migrations.RunMigrations(db, migrations.SQLiteMigrationsFS(),
--     "sqlite")`). Files at the package root migrations/*.sql are
--     still embedded by the legacy MigrationsFS for backwards-compat
--     callers, but the production boot path is
--     migrations.SQLiteMigrationsFS().
--
-- The folder_id column is INTENTIONALLY omitted from the CREATE so
-- ensureColumn exercises its ALTER TABLE ADD COLUMN path against a
-- table that already exists, preserving the production-on-existing-DB
-- semantics where folder_id is added to a pre-existing table. New
-- deployments via migrations see the same end state either way.

CREATE TABLE IF NOT EXISTS dark_editor_projects (
    id         TEXT PRIMARY KEY,
    name       TEXT,
    created_at TEXT
);
