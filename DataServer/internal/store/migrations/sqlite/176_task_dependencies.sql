-- 176_task_dependencies.sql
--
-- Persistent multi-Task DAG (docs/100-percent-plan/04 §2): add the
-- depends_on edge list to the canonical tasks table.
--
-- Until this migration, taskgraph.Task.DependsOn existed only in the
-- in-memory model: migration 039 created tasks WITHOUT a depends_on
-- column, so TickReadiness' AreDependenciesSatisfied check read an
-- always-empty edge list (every PENDING task transitioned
-- unconditionally) and a master restart reconstructed a graph of no
-- edges. depends_on also makes the graph queryable: readiness
-- ticks, failure propagation and cycle detection can run in SQL
-- instead of loading every task into memory.
--
-- Idempotency strategy (same pattern as 022): the ALTER TABLE and the
-- bootstrap CREATE TABLE are both IF-guarded by the migrations runner's
-- error tolerance (duplicate column / no such table pass-through), so
-- this migration applies cleanly on fresh databases, on real upgraded
-- databases, and on sparse legacy-upgrade fixtures that never created
-- the tasks domain at all (e.g. the validation legacy fixture).
--
-- The 039 UNIQUE INDEX idx_tasks_job_id_unique (one task per job) is
-- DROPPED here: Track 4 requires one Job to expand into multiple
-- persistent Tasks. The non-unique idx_tasks_job_id index (also from
-- 039) already covers job-scoped lookups, and task_id remains the
-- PRIMARY KEY. The canonical single-task model is preserved for jobs
-- created before fan-out exists: they simply publish one task.

ALTER TABLE tasks ADD COLUMN depends_on TEXT NOT NULL DEFAULT '[]';

DROP INDEX IF EXISTS idx_tasks_job_id_unique;
