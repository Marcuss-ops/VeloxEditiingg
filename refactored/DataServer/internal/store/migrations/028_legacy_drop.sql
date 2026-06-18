-- 028_legacy_drop.sql — Final legacy orchestrator drop.
--
-- Permanently drops the legacy orchestrator_jobs + orchestrator_outbox tables
-- that were replaced by workflow_runs + outbox_events in PR 8 + PR 9.
--
-- ┌────────────────────────────────────────────────────────────────────────┐
-- │  ⚠  OPERATOR PREREQUISITE                                              │
-- │                                                                         │
-- │  This migration will NOT complete unless workflow_runs has at least   │
-- │  one row. Run the workflow migrator first and verify its output:       │
-- │                                                                         │
-- │    velox-server migrate workflows-v2 --apply                           │
-- │    # Expect: runs_found > 0, runs_migrated == runs_found,             │
-- │    #         invalid_runs == 0                                          │
-- │                                                                         │
-- │  Only after that finishes cleanly (invalid_runs == 0) is running       │
-- │  028_legacy_drop safe. The guard below refuses to drop the legacy       │
-- │  tables if workflow_runs is empty.                                      │
-- └────────────────────────────────────────────────────────────────────────┘
--
-- Backing SQLite-RAISE pattern: SELECT CASE WHEN … THEN RAISE(ABORT, msg)
-- END returns the RAISE() expression only when the precondition fails, which
-- causes the migration transaction to abort. Otherwise the CASE returns
-- NULL and the migration continues to the DROP TABLE statements.
SELECT CASE
    WHEN NOT EXISTS (SELECT 1 FROM workflow_runs)
    THEN RAISE(ABORT,
        '028_legacy_drop refused: workflow_runs is empty. '
        || 'Run `velox-server migrate workflows-v2 --apply` first, '
        || 'then re-run the migration to drop orchestrator_jobs + '
        || 'orchestrator_outbox.')
END;

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
