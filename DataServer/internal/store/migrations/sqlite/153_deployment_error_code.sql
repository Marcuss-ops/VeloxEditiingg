-- Error code separation: stable machine-routable error code alongside the
-- human-readable message.
--
-- The fleet executor classifies every failed rollout into a closed error-code
-- vocabulary (DRAIN_TIMEOUT / DEPLOY_COMMAND_FAILED / RESTART_FAILED /
-- READY_TIMEOUT / DIGEST_MISMATCH / SSH_FAILED, plus step-specific
-- extensions like SMOKE_FAILED and DRIVE_DELIVERY_FAILED). The message text
-- is free-form and may change; the code is the stable routing key for
-- metrics, retry policy, admin filtering and tests.
--
-- Writer contract:
--   * deployment_records.error_code is the JOURNAL copy: each history row
--     keeps its own code+message so audit history is never rewritten.
--   * worker_deployment_state.last_operation_error_code is the READ MODEL
--     copy of the LAST operation's code. A new operation (PENDING insert)
--     clears it along with last_operation_error; the previous code survives
--     only in the journal history.
--   * A SUCCEEDED / ROLLED_BACK terminal write clears both code and message.
ALTER TABLE deployment_records
    ADD COLUMN error_code TEXT NOT NULL DEFAULT '';

ALTER TABLE worker_deployment_state
    ADD COLUMN last_operation_error_code TEXT NOT NULL DEFAULT '';
