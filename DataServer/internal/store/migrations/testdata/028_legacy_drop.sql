-- 028_legacy_drop.sql — Final legacy orchestrator drop.
--
-- Permanently drops the legacy orchestrator_jobs + orchestrator_outbox tables
-- that were replaced by workflow_runs + outbox_events in PR 8 + PR 9.
--
-- The precondition for this migration lives in Go (see
-- internal/store/migrations/pre_check.go — MustDropLegacyOrchestrator):
--   * if workflow_runs is non-empty: proceed;
--   * if workflow_runs is empty AND orchestrator_* are empty: drop anyway;
--   * if workflow_runs is empty AND orchestrator_* are non-empty: abort
--     with an explicit Go-level error (see pre_check.go).
--
-- This file is therefore intentionally unconditional — the previous version
-- used `SELECT CASE WHEN … RAISE(ABORT, …)`, but RAISE() may only be invoked
-- from inside a trigger, so the in-SQL guard aborted fresh installs without
-- ever reaching the DROP statements.
--
-- DOWN-MIGRATION NOTE
-- No downgrade path is provided. The legacy orchestrator types
-- (*queue.Orchestrator, queue.JobStep, queue.MultiStepJob) and store
-- methods (UpsertOrchestratorJob*, PollOrchestratorOutbox,
-- MarkOutboxProcessed, InsertOutboxEntry) were REMOVED in PR 8 + PR 9.
-- Recreating the tables would produce zombie rows no Go code can read.
-- Restore workflow_runs / workflow_steps / outbox_events from your
-- nightly backup if you need to roll back.

DROP TABLE IF EXISTS orchestrator_jobs;
DROP TABLE IF EXISTS orchestrator_outbox;
