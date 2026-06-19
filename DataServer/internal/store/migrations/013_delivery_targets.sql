-- Migration 013: Delivery targets and attempts
-- Separates Drive/YouTube delivery state from job render state.
-- Targets are resolved at enqueue time (not at delivery time).

-- Delivery targets resolved at enqueue time
CREATE TABLE IF NOT EXISTS delivery_targets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    target_type TEXT NOT NULL CHECK(target_type IN ('youtube', 'drive')),
    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'scheduled', 'uploading', 'completed', 'failed', 'needs_reauth', 'skipped')),
    config TEXT NOT NULL DEFAULT '{}',
    result TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_attempt_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_delivery_targets_job ON delivery_targets(job_id);
CREATE INDEX IF NOT EXISTS idx_delivery_targets_status ON delivery_targets(status);
CREATE INDEX IF NOT EXISTS idx_delivery_targets_type ON delivery_targets(target_type);

-- Delivery attempts audit trail
CREATE TABLE IF NOT EXISTS delivery_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    delivery_target_id INTEGER NOT NULL REFERENCES delivery_targets(id) ON DELETE CASCADE,
    attempt_number INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'scheduled' CHECK(status IN ('scheduled', 'uploading', 'completed', 'failed')),
    result TEXT NOT NULL DEFAULT '{}',
    started_at TEXT,
    completed_at TEXT,
    error_message TEXT,
    worker_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_delivery_attempts_target ON delivery_attempts(delivery_target_id);
CREATE INDEX IF NOT EXISTS idx_delivery_attempts_status ON delivery_attempts(status);
