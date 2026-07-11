-- Migration 011: Add job queue performance indices
-- PR2 — Job SQLite source of truth: index for worker assignment lookups

CREATE INDEX IF NOT EXISTS idx_jobs_assigned_to ON jobs(assigned_to, status);
