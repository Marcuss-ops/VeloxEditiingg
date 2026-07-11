-- 037_job_delivery_plans.sql — PR delivery plan resolver
--
-- Rationale:
--   * job_delivery_plans maps a job_id to its explicit delivery plan:
--     which destinations should receive the artifact and with what priority.
--   * A job without an explicit plan falls back to ALL enabled
--     delivery_destinations (current behavior).
--   * The PRIMARY KEY (job_id, destination_id) prevents duplicate
--     entries and makes upserts trivial.
--   * The `enabled` column allows per-destination toggling per-job
--     without affecting the global destination configuration.
--   * The `priority` column allows ordering of destinations (e.g.
--     primary YouTube channel before secondary).
--
-- DOWN-MIGRATION NOTE: not provided. Drop and backfill from backup.

CREATE TABLE IF NOT EXISTS job_delivery_plans (
    job_id          TEXT NOT NULL,
    destination_id  TEXT NOT NULL,
    enabled         INTEGER NOT NULL DEFAULT 1,
    priority        INTEGER NOT NULL DEFAULT 0,
    metadata_json   TEXT NOT NULL DEFAULT '{}',
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,

    PRIMARY KEY (job_id, destination_id),
    FOREIGN KEY (job_id) REFERENCES jobs(job_id),
    FOREIGN KEY (destination_id) REFERENCES delivery_destinations(destination_id)
);

-- Fast lookup: all enabled plans for a given job.
CREATE INDEX IF NOT EXISTS idx_job_delivery_plans_job
    ON job_delivery_plans(job_id, enabled);
