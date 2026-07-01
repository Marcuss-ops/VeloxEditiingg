-- 055_creator_forwardings.sql
--
-- Creator Forwarding — persistent source of truth for remote-creator
-- job forwarding lifecycle.
--
-- Replaces the volatile in-memory goroutine-per-request polling
-- (scheduleCreatorPolling) with a durable, lease-based, supervised
-- runner model. Every forwarded result is tracked here so that:
--   - a master restart does not lose pending forwardings;
--   - concurrent pollers produce deterministic target_job_ids;
--   - retries with backoff are durable across process boundaries;
--   - the forwarding → enqueue transition is atomic and idempotent.
--
-- Status vocabulary:
--   PENDING          — forwarding record created, no poller has claimed it yet.
--   POLLING          — claimed by a runner, actively checking remote status.
--   READY_TO_FORWARD — remote creator has completed; payload ready to enqueue.
--   FORWARDING       — enqueue in progress (short-lived).
--   RETRY_WAIT       — enqueue failed; waiting for backoff before retry.
--   FORWARDED        — Job + Task + TaskSpec created; target_job_id populated.
--   FAILED           — terminal failure after max attempts exhausted.
--   BLOCKED          — operator intervention required (e.g., invalid payload).
--
-- Constraints:
--   UNIQUE(source_provider, source_job_id, target_executor_id)
--     → one forwarding record per (provider, creator job, target stage).
--   UNIQUE(target_job_id)
--     → at most one Velox Job created from a forwarding record.
--
-- Lease design mirrors the task-claim pattern:
--   - locked_by + lease_id + lease_expires_at guard concurrent runners.
--   - A runner with an expired lease can be preempted by another runner.
--   - RenewLease is called periodically (leaseDuration/3) during polling.
--
-- Idempotency: CREATE TABLE IF NOT EXISTS is safe for re-runs.
-- The migration runner's checksum gate prevents modification of an
-- already-applied migration; fresh deployments run this once.

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
    locked_by             TEXT NOT NULL DEFAULT '',
    lease_id              TEXT NOT NULL DEFAULT '',
    lease_expires_at      TEXT NOT NULL DEFAULT '',
    last_error_code       TEXT NOT NULL DEFAULT '',
    last_error_message    TEXT NOT NULL DEFAULT '',
    created_at            TEXT NOT NULL DEFAULT '',
    updated_at            TEXT NOT NULL DEFAULT '',
    forwarded_at          TEXT NOT NULL DEFAULT ''
);

-- Prevent duplicate forwarding records for the same provider + creator job + target stage.
CREATE UNIQUE INDEX IF NOT EXISTS idx_creator_forwardings_source
    ON creator_forwardings(source_provider, source_job_id, target_executor_id);

-- Ensure at most one target Job per forwarding.
CREATE UNIQUE INDEX IF NOT EXISTS idx_creator_forwardings_target_job
    ON creator_forwardings(target_job_id)
    WHERE target_job_id IS NOT NULL AND target_job_id != '';

-- Index for runner claim queries: find PENDING / RETRY_WAIT records
-- whose lease has expired (or never set) and whose next_attempt_at is due.
CREATE INDEX IF NOT EXISTS idx_creator_forwardings_claim
    ON creator_forwardings(status, next_attempt_at, lease_expires_at);

-- Index for monitoring: oldest pending age, queue depth.
CREATE INDEX IF NOT EXISTS idx_creator_forwardings_status_created
    ON creator_forwardings(status, created_at);
