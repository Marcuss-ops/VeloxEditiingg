-- Hard placement reservations for the worker-scoped future-asset window.
-- This is scheduler metadata, not a Task lifecycle state.
CREATE TABLE IF NOT EXISTS future_task_reservations (
    task_id        TEXT PRIMARY KEY,
    job_id         TEXT NOT NULL,
    worker_id      TEXT NOT NULL,
    reservation_id TEXT NOT NULL UNIQUE,
    task_revision  INTEGER NOT NULL DEFAULT 0,
    distance       INTEGER NOT NULL,
    expires_at     TEXT NOT NULL,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_future_task_reservations_worker
    ON future_task_reservations(worker_id, distance, expires_at);
