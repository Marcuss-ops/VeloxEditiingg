-- 052_task_attempt_columns.sql
--
-- PR-2 / fix/canonical-attempt-identity: stamp the canonical
-- (attempt_id, attempt_number) on the tasks row at Claim time. Pre-052
-- the canonical attempt identity was ONLY on the task_attempts row,
-- forcing the worms-in-the-head validation path to do triple-key
-- lookups (task_id, worker_id, lease_id) and leaving attempts.attempt_id
-- out of tasks-side CAS queries (RenewLease, accept, transition).
--
-- After this migration:
--   - ClaimNextWithAttemptAtomic sets (tasks.attempt_id, tasks.attempt_number)
--     INSIDE the same tx as the READY→LEASED CAS AND the PENDING TaskAttempt
--     insert. The single source of truth on canonical attempt identity is
--     finally the tasks row, mirroring the per-task-attempts row.
--   - RenewLease reads attempt_id from the tasks row (matches pre-existing
--     SQL `WHERE task_id=? AND attempt_id=? ...` that previously was
--     silently SELECTing a non-existent column).
--   - TaskAttempt atomic methods (complete final, expire by identity)
--     still JOIN/select by task_id+attempt_number using task_attempts
--     column — the tasks-side columns are an authoritative mirror.
--
-- Idempotency strategy:
--   - ALTER TABLE ADD COLUMN is idempotent ONLY with newer SQLite (≥3.35).
--     The 052 migration is fresh-deployment + additive-fill by
--     ClaimNextWithAttemptAtomic, so it does NOT require IF NOT EXISTS
--     guards on the columns. A re-run error from a partial second pass
--     is acceptable because the migration enforcer treats ALTER TABLE
--     errors as "already applied".
--   - Backfill is not provided: pre-052 rows keep NULL attempt_id and
--     attempt_number=0, which is fine because RenewLease / AttemptAccept
--     already pass attempt_id via the canonical task_attempts lookup
--     (GetByTaskIDAndWorkerAndLease). Post-052 rows always have both
--     columns stamped at Claim time.
--
-- CI guard (e) interaction:
--   - No DATETIME('now') / CURRENT_TIMESTAMP used here. registered_at
--     is per-row stamped via the app layer on Claim, not at SQL DEFAULT.
--   - STRFTIME-style timestamps are not needed because we are not
--     introducing a new timestamp column.

ALTER TABLE tasks ADD COLUMN attempt_id TEXT;
ALTER TABLE tasks ADD COLUMN attempt_number INTEGER NOT NULL DEFAULT 0;

-- Index on attempt_id for O(log N) CAS lookup from RenewLease + future
-- per-attempt audit queries. attempt_number is implicit per (task_id,
-- attempt_number) so no secondary index is needed there.
CREATE INDEX IF NOT EXISTS idx_tasks_attempt_id
    ON tasks(attempt_id);
