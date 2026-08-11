-- Migration 146: canonical reconciliation traceability
--
-- Phase A3: every canonical reconciler transition stamps WHY the system
-- made the transition. reconciled_at is the wall-clock of the last
-- reconciler mutation; reconciliation_reason is the stable machine
-- reason code (e.g. STALE_AWAITING_ARTIFACT); reconciliation_version
-- is a per-reason monotonic counter so a job that was reconciled
-- multiple times (e.g. STALE_AWAITING_ARTIFACT then operator recover)
-- keeps an auditable history without rewriting the reason column.
--
-- The columns are NULL/'') on jobs that were never touched by a
-- reconciler; reconciler CAS transitions set them atomically with the
-- status flip so there is never a window where the status changed but
-- the reconciliation stamp is missing.
ALTER TABLE jobs ADD COLUMN reconciled_at TEXT;
ALTER TABLE jobs ADD COLUMN reconciliation_reason TEXT NOT NULL DEFAULT '';
ALTER TABLE jobs ADD COLUMN reconciliation_version INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_jobs_reconciliation
    ON jobs(reconciliation_reason, reconciled_at);
