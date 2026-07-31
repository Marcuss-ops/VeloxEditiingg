-- 111_task_phase_timings_identity.sql
--
-- Observability chain / block 1: stamp the canonical identity tuple on
-- task_phase_timings.
--
-- task_phase_timings is the per-phase summary layer. Before this
-- migration its rows carried only (attempt_id, component, action, …
-- timings): querying "which worker / which executor version produced
-- this phase timing" required a join back to task_attempts, and the
-- worker-session/snapshot link did not exist at all. These six columns
-- denormalize the canonical identity tuple onto every phase row. The
-- values are copied by the MASTER at ingest from the canonical identity
-- tuple — never accepted verbatim from the worker payload.
--
-- All DEFAULTs are constant per SQLite ALTER TABLE rules.

ALTER TABLE task_phase_timings ADD COLUMN job_id              TEXT    NOT NULL DEFAULT '';
ALTER TABLE task_phase_timings ADD COLUMN task_id             TEXT    NOT NULL DEFAULT '';
ALTER TABLE task_phase_timings ADD COLUMN worker_id           TEXT    NOT NULL DEFAULT '';
ALTER TABLE task_phase_timings ADD COLUMN worker_snapshot_id  TEXT    NOT NULL DEFAULT '';
ALTER TABLE task_phase_timings ADD COLUMN executor_id         TEXT    NOT NULL DEFAULT '';
ALTER TABLE task_phase_timings ADD COLUMN executor_version    INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_task_phase_timings_identity
    ON task_phase_timings(worker_id, worker_snapshot_id, component, action);
