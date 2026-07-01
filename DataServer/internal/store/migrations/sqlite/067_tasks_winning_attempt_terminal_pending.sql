-- migrations/sqlite/067_tasks_winning_attempt_terminal_pending.sql
--
-- Artifact Commit Protocol — Phase 2.6.
--
-- Three new columns on tasks that decouple the ingest-time terminal
-- write from the coordinator-time commit protocol.
--
-- winning_attempt_terminal_pending is the canonical
-- intermediate-state marker. IngestTaskResultAtomic (legacy TaskResult
-- path) writes this to 1 instead of flipping tasks.status to
-- SUCCEEDED — leaving the actual SUCCEEDED write to
-- Coordinator.CommitAttempt in a single atomic tx.
--
-- winning_attempt_id is set by CommitAttempt when the commit protocol
-- ratifies the attempt that produced the outputs. Combined with
-- winning_attempt_committed_at it provides the canonical audit trail
-- for which attempt ultimately won and when the commit tx committed.
--
-- Idempotency: the migrations runner tolerates duplicate-column
-- errors on ALTER TABLE ADD COLUMN (see migrations.go::applyMigration).
-- Idempotent re-runs of this .sql on a partial-ro apply are safe.

ALTER TABLE tasks ADD COLUMN winning_attempt_terminal_pending INTEGER NOT NULL DEFAULT 0;
ALTER TABLE tasks ADD COLUMN winning_attempt_id TEXT;
ALTER TABLE tasks ADD COLUMN winning_attempt_committed_at TEXT;

CREATE INDEX IF NOT EXISTS idx_tasks_winning_attempt_id
    ON tasks(winning_attempt_id);
