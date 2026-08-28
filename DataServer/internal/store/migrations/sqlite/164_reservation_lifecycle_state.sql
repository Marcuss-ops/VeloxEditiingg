-- Reservation lifecycle state machine: RESERVED → PLANNING → PREPARING → PREPARED → EXPIRED
-- Only PREPARED permits ClaimTaskForWorkerAtomic under StrictPrefetchClaim.
-- Legacy-upgrade fixtures may not materialize migration 150's table; make the
-- lifecycle migration total before extending it.
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
ALTER TABLE future_task_reservations ADD COLUMN state TEXT NOT NULL DEFAULT '';
