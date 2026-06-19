CREATE TABLE IF NOT EXISTS job_progress (
    job_id         TEXT PRIMARY KEY,
    attempt_number INTEGER NOT NULL DEFAULT 1,
    percent        REAL NOT NULL DEFAULT 0,
    stage          TEXT NOT NULL DEFAULT '',
    current_item   INTEGER NOT NULL DEFAULT 0,
    total_items    INTEGER NOT NULL DEFAULT 0,
    message        TEXT NOT NULL DEFAULT '',
    updated_at     TEXT NOT NULL
);
