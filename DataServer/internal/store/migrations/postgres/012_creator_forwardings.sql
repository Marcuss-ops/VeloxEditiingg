-- Migration 012: creator_forwardings (Postgres dialect)
--
-- Creator Forwarding — persistent source of truth for remote-creator
-- job forwarding lifecycle. Mirrors the SQLite migration 055 + 089
-- (extra fields) in a single Postgres DDL.
--
-- Status vocabulary:
--   PENDING          — forwarding record created, no poller has claimed it yet.
--   POLLING          — claimed by a runner, actively checking remote status.
--   READY_TO_FORWARD — remote creator has completed; payload ready to enqueue.
--   FORWARDING       — enqueue in progress (short-lived).
--   RETRY_WAIT       — enqueue failed; waiting for backoff before retry.
--   FORWARDED        — Job + Task + TaskSpec created; target_job_id populated.
--   FAILED           — terminal failure after max attempts exhausted.
--   CANCELLED        — cancelled by the operator or client.
--   BLOCKED          — operator intervention required (e.g., invalid payload).

CREATE TABLE IF NOT EXISTS creator_forwardings (
    forwarding_id         TEXT PRIMARY KEY,
    source_provider       TEXT NOT NULL,
    source_job_id         TEXT NOT NULL,
    source_status         TEXT NOT NULL DEFAULT '',
    target_executor_id    TEXT NOT NULL,
    target_job_id         TEXT,
    payload_json          TEXT NOT NULL DEFAULT '',
    payload_sha256        TEXT NOT NULL DEFAULT '',
    status                TEXT NOT NULL DEFAULT 'PENDING',
    attempt_count         INTEGER NOT NULL DEFAULT 0,
    next_attempt_at       TEXT NOT NULL DEFAULT '',
    poll_attempts         INTEGER NOT NULL DEFAULT 0,
    next_poll_at          TEXT NOT NULL DEFAULT '',
    last_polled_at        TEXT NOT NULL DEFAULT '',
    locked_by             TEXT NOT NULL DEFAULT '',
    lease_id              TEXT NOT NULL DEFAULT '',
    lease_expires_at      TEXT NOT NULL DEFAULT '',
    last_error_code       TEXT NOT NULL DEFAULT '',
    last_error_message    TEXT NOT NULL DEFAULT '',
    last_error_class      TEXT NOT NULL DEFAULT '',
    created_at            TEXT NOT NULL DEFAULT '',
    updated_at            TEXT NOT NULL DEFAULT '',
    forwarded_at          TEXT NOT NULL DEFAULT ''
);

-- Prevent duplicate forwarding records for the same provider + creator job + target stage.
CREATE UNIQUE INDEX IF NOT EXISTS idx_pg_creator_forwardings_source
    ON creator_forwardings(source_provider, source_job_id, target_executor_id);

-- Ensure at most one target Job per forwarding.
CREATE UNIQUE INDEX IF NOT EXISTS idx_pg_creator_forwardings_target_job
    ON creator_forwardings(target_job_id)
    WHERE target_job_id IS NOT NULL AND target_job_id != '';

-- Index for runner claim queries: find PENDING / RETRY_WAIT records
-- whose lease has expired (or never set) and whose next_attempt_at is due.
CREATE INDEX IF NOT EXISTS idx_pg_creator_forwardings_claim
    ON creator_forwardings(status, next_attempt_at, lease_expires_at);

-- Index for monitoring: oldest pending age, queue depth.
CREATE INDEX IF NOT EXISTS idx_pg_creator_forwardings_status_created
    ON creator_forwardings(status, created_at);
