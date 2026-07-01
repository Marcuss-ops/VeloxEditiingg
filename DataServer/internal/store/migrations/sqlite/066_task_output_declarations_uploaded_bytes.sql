-- migrations/sqlite/066_task_output_declarations_uploaded_bytes.sql
--
-- Artifact Commit Protocol (Phase 2.x) — per-declaration progress
-- counter.
--
-- Migrating 062's declaration table to expose the rolling
-- uploaded_bytes the coordinator flips CAS-gated on (commit_id,
-- upload_id) inside RecordUploadProgress. Default 0 keeps the
-- column NOT NULL without breaking pre-Phase-2 rows: a worker's
-- first heartbeat bumps it monotonically forward from zero.
--
-- Why a partial-index and not a column on the parent table:
-- uploaded_bytes is per-(commit, output_kind, logical_name) — i.e.
-- per declaration, not per attempt. Two declarations on the same
-- commit track bytes independently.

ALTER TABLE task_output_declarations
    ADD COLUMN uploaded_bytes INTEGER NOT NULL DEFAULT 0;
