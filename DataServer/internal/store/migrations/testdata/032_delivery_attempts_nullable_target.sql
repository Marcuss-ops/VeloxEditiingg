-- 032_delivery_attempts_nullable_target.sql
--
-- Makes delivery_attempts.delivery_target_id nullable so the sentinel value 0
-- can be replaced with NULL. Migration 013 created this column as NOT NULL;
-- migration 022 relaxed it for fresh installs (CREATE TABLE IF NOT EXISTS)
-- but upgraded databases still carry the NOT NULL constraint.
--
-- SQLite does not support ALTER TABLE ALTER COLUMN, so we rebuild the table:
--   1. Create delivery_attempts_v032 with the desired schema.
--   2. Copy all rows, converting delivery_target_id = 0 → NULL.
--   3. Drop the old table.
--   4. Rename the new table.
--   5. Recreate indexes.
--
-- The rebuild is idempotent: if delivery_target_id is already nullable
-- (fresh installs from migration 022), step 4 is a no-op because the
-- column already matches.

-- =========================================================================
-- 1. Create new table with nullable delivery_target_id
-- =========================================================================

CREATE TABLE IF NOT EXISTS delivery_attempts_v032 (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    delivery_target_id   INTEGER,                                      -- now nullable
    attempt_number       INTEGER NOT NULL DEFAULT 0,
    status               TEXT    NOT NULL DEFAULT 'scheduled',
    result               TEXT    NOT NULL DEFAULT '{}',
    started_at           TEXT,
    completed_at         TEXT,
    error_message        TEXT,
    worker_id            TEXT,
    delivery_id          TEXT
);

-- =========================================================================
-- 2. Copy rows: convert delivery_target_id = 0 → NULL
-- =========================================================================

INSERT INTO delivery_attempts_v032
    (id, delivery_target_id, attempt_number, status, result,
     started_at, completed_at, error_message, worker_id, delivery_id)
SELECT
    id,
    CASE WHEN delivery_target_id = 0 THEN NULL ELSE delivery_target_id END,
    COALESCE(attempt_number, 0),
    COALESCE(status, 'scheduled'),
    COALESCE(result, '{}'),
    started_at,
    completed_at,
    error_message,
    worker_id,
    delivery_id
FROM delivery_attempts;

-- =========================================================================
-- 3. Drop old table
-- =========================================================================

DROP TABLE delivery_attempts;

-- =========================================================================
-- 4. Rename new table into place
-- =========================================================================

ALTER TABLE delivery_attempts_v032 RENAME TO delivery_attempts;

-- The INSERT with explicit id values does not update sqlite_sequence;
-- without this UPDATE the next auto-incremented id could collide with
-- existing rows.
UPDATE sqlite_sequence
   SET seq = (SELECT COALESCE(MAX(id), 0) FROM delivery_attempts)
 WHERE name = 'delivery_attempts';

-- =========================================================================
-- 5. Recreate indexes (idempotent via IF NOT EXISTS)
-- =========================================================================

CREATE INDEX IF NOT EXISTS idx_delivery_attempts_target
    ON delivery_attempts(delivery_target_id);

CREATE INDEX IF NOT EXISTS idx_delivery_attempts_status
    ON delivery_attempts(status);

CREATE INDEX IF NOT EXISTS idx_delivery_attempts_legacy_target
    ON delivery_attempts(delivery_target_id, started_at);

CREATE INDEX IF NOT EXISTS idx_delivery_attempts_status_started
    ON delivery_attempts(status, started_at);

CREATE INDEX IF NOT EXISTS idx_delivery_attempts_delivery
    ON delivery_attempts(delivery_id, started_at);
