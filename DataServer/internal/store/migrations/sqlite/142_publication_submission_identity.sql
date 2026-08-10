-- 142_publication_submission_identity.sql
-- Keep asynchronous submission identity, canonical verification operation, and
-- positive reconciler evidence separate from the final published media ID.

ALTER TABLE publication_states ADD COLUMN submitted_remote_id TEXT NOT NULL DEFAULT '';
ALTER TABLE publication_states ADD COLUMN verification_operation TEXT NOT NULL DEFAULT '';
ALTER TABLE publication_states ADD COLUMN reconciliation_verified INTEGER NOT NULL DEFAULT 0;

-- Non-terminal rows can recover their submitted operation from the previous
-- remote checkpoint. Historical PUBLISHED rows are ambiguous and are moved
-- to an explicit reconciliation-required checkpoint instead.
UPDATE publication_states
SET state = 'PARTIAL',
    retry_from = 'VERIFYING',
    remote_id = '',
    last_error_code = 'LEGACY_RECONCILIATION_REQUIRED'
WHERE state = 'PUBLISHED' AND submitted_remote_id = '';

UPDATE publication_states
SET submitted_remote_id = COALESCE(NULLIF(remote_id, ''), '')
WHERE submitted_remote_id = '' AND state <> 'PUBLISHED';
