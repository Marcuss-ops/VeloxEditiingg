-- 040_stub_dark_editor_projects.sql
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
-- PLACEMENT NOTE: this migration MUST live at the migrations/
-- package root (DataServer/internal/store/migrations/040_*.sql) so
-- the runner's `MigrationsFS` (declared in embed.go with
-- `//go:embed *.sql`) captures it. The `*` glob matches files in
-- the same package directory only — subdirectories
-- `migrations/sqlite/` and `migrations/postgres/` declared by
-- runner.go (`//go:embed sqlite/*.sql`, `//go:embed postgres/*.sql`)
-- would NOT be picked up by the legacy MigrationsFS path that
-- sqlite.go still calls today (`migrations.RunMigrations(db,
-- migrationsFS, ".")`).
--
-- Future contributors creating migration 041+:
--   - Place new .sql files at migrations/ root (correct today)
--   - OR migrate sqlite.go to call `migrations.RunMigrations(db,
--     migrations.SQLiteMigrationsFS(), "sqlite")` and place the
--     file under migrations/sqlite/ — that's the dialect-aware
--     refactor runner.go is steering toward but has not yet been
--     wired into the SQLite adapter.
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
