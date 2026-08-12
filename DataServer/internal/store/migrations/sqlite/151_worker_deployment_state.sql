-- Canonical worker deployment read model.
--
-- deployment_records is an append-only operation history and workers.raw_json
-- is a heartbeat snapshot. This table keeps the operator-facing dimensions
-- separate and durable so a failed newer rollout cannot overwrite the last
-- verified digest and a missing heartbeat cannot be mistaken for no desired
-- digest.
--
-- Digest provenance (three independent fields, three independent writers):
--   * desired_digest         — control-plane intent (deployment request).
--   * running_digest         — OBSERVED digest; written ONLY by an
--                              authenticated heartbeat. NULL until the first
--                              authenticated heartbeat observes a digest; it
--                              is never inferred from history or operator
--                              input.
--   * last_successful_digest — last VERIFIED digest; advanced only by a
--                              successful verification (terminal SUCCEEDED /
--                              ROLLED_BACK transition).
CREATE TABLE IF NOT EXISTS worker_deployment_state (
    worker_id               TEXT PRIMARY KEY,
    desired_digest          TEXT NOT NULL DEFAULT '',
    running_digest          TEXT,
    last_successful_digest  TEXT NOT NULL DEFAULT '',
    last_operation_id       TEXT NOT NULL DEFAULT '',
    last_operation_kind     TEXT NOT NULL DEFAULT '',
    last_operation_status   TEXT NOT NULL DEFAULT '',
    last_operation_error    TEXT NOT NULL DEFAULT '',
    updated_at              TEXT NOT NULL
);

ALTER TABLE deployment_records
    ADD COLUMN error_message TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_worker_deployment_state_status
    ON worker_deployment_state(last_operation_status, updated_at DESC);

-- Backfill only when a state row does not already exist. The latest history
-- row defines desired/operation state; the latest verified row defines the
-- last known-good digest independently.
--
-- Legacy backfill policy:
--   * desired_digest        ← target of the NEWEST record (surviving intent).
--   * running_digest        ← NULL. running_digest is observed, not inferred:
--                             a legacy history cannot say what the worker is
--                             actually running. It stays NULL until the first
--                             authenticated heartbeat writes it.
--   * last_successful_digest ← target of the newest SUCCEEDED/ROLLED_BACK
--                             record (the last digest verified on the
--                             worker; a newer FAILED record never erases it).
--   * last_operation_*      ← dimensions of the newest record.
--
-- Ordering is deterministic: (started_at DESC, deployment_id DESC). The
-- deployment_id tiebreak keeps equal-timestamp legacy rows stable so the
-- same legacy DB always projects the same read model.
INSERT OR IGNORE INTO worker_deployment_state (
    worker_id, desired_digest, running_digest, last_successful_digest,
    last_operation_id, last_operation_kind, last_operation_status,
    last_operation_error, updated_at
)
SELECT latest.worker_id,
       latest.target_digest,
       NULL,
       COALESCE((
           SELECT successful.target_digest
             FROM deployment_records successful
            WHERE successful.worker_id = latest.worker_id
              AND successful.status IN ('SUCCEEDED', 'ROLLED_BACK')
            ORDER BY successful.started_at DESC, successful.deployment_id DESC
            LIMIT 1
       ), ''),
       latest.deployment_id,
       CASE WHEN latest.is_rollback = 1 THEN 'rollback' ELSE 'update' END,
       latest.status,
       COALESCE(latest.error_message, ''),
       COALESCE(latest.finished_at, latest.started_at)
  FROM deployment_records latest
 WHERE NOT EXISTS (
       SELECT 1
         FROM deployment_records newer
        WHERE newer.worker_id = latest.worker_id
          AND (newer.started_at > latest.started_at OR
               (newer.started_at = latest.started_at AND
                newer.deployment_id > latest.deployment_id))
   );
