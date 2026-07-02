-- Reverses 068_task_requirements.sql
-- Drops the index first (explicit, mirrors UP ordering) then the table.
-- Both uses IF EXISTS so a second RunDown (after UP -> DOWN -> UP) is a no-op
-- minus the DELETE FROM schema_migrations that RunDown itself performs.
--
-- The runner excludes *.down.sql files from discoverMigrations so this
-- file does NOT participate in startup migration. Operators apply it
-- explicitly via migrations.RunDown(db, fs, dir, 68). The runner also
-- deletes the row from schema_migrations for version 068, so a
-- subsequent RunMigrations will re-apply 068 cleanly (UP -> DOWN -> UP
-- idempotency on the task_requirements + idx_task_requirements_task_id
-- subset).
DROP INDEX IF EXISTS idx_task_requirements_task_id;
DROP TABLE IF EXISTS task_requirements;
