-- 024_legacy_state_cleanup.sql
-- Migrate legacy job states to the canonical 7-state machine:
--   PENDING → LEASED → RUNNING → SUCCEEDED
--                    ↓
--               RETRY_WAIT → PENDING (retry)
--                    ↓
--               FAILED
--   PENDING → CANCELLED

-- PROCESSING/ASSIGNED → RUNNING (the job was actively being worked on)
UPDATE jobs SET status = 'RUNNING' WHERE UPPER(status) IN ('PROCESSING', 'ASSIGNED');

-- COMPLETED → SUCCEEDED
UPDATE jobs SET status = 'SUCCEEDED' WHERE UPPER(status) = 'COMPLETED';

-- ERROR/LOST → FAILED
UPDATE jobs SET status = 'FAILED' WHERE UPPER(status) IN ('ERROR', 'LOST');

-- QUEUED → PENDING (semantically identical)
UPDATE jobs SET status = 'PENDING' WHERE UPPER(status) = 'QUEUED';

-- CANCELLING → CANCELLED (terminal)
UPDATE jobs SET status = 'CANCELLED' WHERE UPPER(status) = 'CANCELLING';

-- RETRYING → RETRY_WAIT
UPDATE jobs SET status = 'RETRY_WAIT' WHERE UPPER(status) = 'RETRYING';

-- Clean up result_json blobs that may have embedded legacy status strings
UPDATE jobs SET result_json = REPLACE(result_json, '"status":"PROCESSING"', '"status":"RUNNING"') WHERE result_json LIKE '%"status":"PROCESSING"%';
UPDATE jobs SET result_json = REPLACE(result_json, '"status":"COMPLETED"', '"status":"SUCCEEDED"') WHERE result_json LIKE '%"status":"COMPLETED"%';
UPDATE jobs SET result_json = REPLACE(result_json, '"status":"ERROR"', '"status":"FAILED"') WHERE result_json LIKE '%"status":"ERROR"%';
UPDATE jobs SET result_json = REPLACE(result_json, '"status":"QUEUED"', '"status":"PENDING"') WHERE result_json LIKE '%"status":"QUEUED"%';

-- Clean up job_history blobs
UPDATE jobs SET result_json = REPLACE(result_json, '"status":"PROCESSING"', '"status":"RUNNING"') WHERE result_json LIKE '%"status":"PROCESSING"%';
