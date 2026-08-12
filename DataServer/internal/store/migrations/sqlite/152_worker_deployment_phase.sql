-- Rollout phase observability: worker_deployment_state.last_phase.
--
-- The fleet executor drives a phase-level pipeline (DRAINING → DEPLOYING →
-- RESTARTING → WAITING_READY → VERIFYING_DIGEST) that is NOT part of the
-- persisted deployment_records vocabulary (which stays append-only
-- PENDING/SUCCEEDED/FAILED/ROLLED_BACK). This column makes the CURRENT phase
-- of the last operation observable on the operator read model, alongside
-- last_operation_status / last_operation_error, so a dashboard can show not
-- just that a rollout FAILED but WHERE it stopped (e.g. FAILED during
-- VERIFYING_DIGEST with digest_mismatch).
--
-- Writer contract:
--   * last_phase is written ONLY by the fleet executor through
--     RecordDeploymentPhase (worker_deployment_state.last_phase).
--   * Heartbeat projections and deployment-record transitions PRESERVE the
--     last recorded phase (the upserts never blank it out).
--   * Terminal outcomes remain expressed by last_operation_status; the phase
--     vocabulary is the in-flight pipeline only.
ALTER TABLE worker_deployment_state
    ADD COLUMN last_phase TEXT NOT NULL DEFAULT '';
