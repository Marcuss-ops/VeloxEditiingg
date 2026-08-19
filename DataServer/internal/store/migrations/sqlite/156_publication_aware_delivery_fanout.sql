-- 156_publication_aware_delivery_fanout.sql
--
-- A delivery plan previously keyed a destination only once per job. That
-- shape cannot represent two language publications targeting the same
-- channel, and it also loses the publication/artifact association at
-- completion time. Keep the legacy empty publication_id as the compatible
-- value for old jobs, while allowing one plan per (job, publication,
-- destination).

PRAGMA foreign_keys = OFF;

ALTER TABLE job_delivery_plans RENAME TO job_delivery_plans_pre_publication;

CREATE TABLE job_delivery_plans (
    job_id          TEXT NOT NULL,
    publication_id  TEXT NOT NULL DEFAULT '',
    destination_id  TEXT NOT NULL,
    enabled         INTEGER NOT NULL DEFAULT 1,
    priority        INTEGER NOT NULL DEFAULT 0,
    retry_budget    INTEGER NOT NULL DEFAULT 5,
    metadata_json   TEXT NOT NULL DEFAULT '{}',
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,

    PRIMARY KEY (job_id, publication_id, destination_id),
    FOREIGN KEY (job_id) REFERENCES jobs(job_id),
    FOREIGN KEY (destination_id) REFERENCES delivery_destinations(destination_id)
);

INSERT INTO job_delivery_plans
    (job_id, publication_id, destination_id, enabled, priority, retry_budget,
     metadata_json, created_at, updated_at)
SELECT job_id, '', destination_id, enabled, priority, retry_budget,
       metadata_json, created_at, updated_at
FROM job_delivery_plans_pre_publication;

DROP TABLE job_delivery_plans_pre_publication;

CREATE INDEX IF NOT EXISTS idx_job_delivery_plans_job
    ON job_delivery_plans(job_id, enabled);

CREATE INDEX IF NOT EXISTS idx_job_delivery_plans_publication
    ON job_delivery_plans(job_id, publication_id, enabled);

ALTER TABLE job_deliveries ADD COLUMN publication_id TEXT NOT NULL DEFAULT '';

DROP INDEX IF EXISTS idx_job_delivery_artifact_destination;
DROP INDEX IF EXISTS idx_job_delivery_artifact_publication_destination;
CREATE UNIQUE INDEX IF NOT EXISTS idx_job_delivery_artifact_destination
    ON job_deliveries(artifact_id, publication_id, destination_id);

PRAGMA foreign_keys = ON;
