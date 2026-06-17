-- Migration 012: Normalize job schema with separate request_json and result_json
-- request_json: immutable request payload (stored once at creation)
-- result_json: mutable operational state (no history/logs in blob)
-- raw_json: kept for backward compatibility during transition

-- Add request_json column (immutable request payload)
ALTER TABLE jobs ADD COLUMN request_json TEXT NOT NULL DEFAULT '';

-- Add result_json column (mutable operational state)
ALTER TABLE jobs ADD COLUMN result_json TEXT NOT NULL DEFAULT '';

-- Copy raw_json to request_json and result_json for existing data
UPDATE jobs SET request_json = raw_json, result_json = raw_json;

-- Indices for operational columns used in queries
CREATE INDEX IF NOT EXISTS idx_jobs_last_error ON jobs(last_error);
CREATE INDEX IF NOT EXISTS idx_jobs_completed_at ON jobs(completed_at);
