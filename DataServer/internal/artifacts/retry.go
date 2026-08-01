// Package artifacts / retry.go — idempotent retry-able quarantine.
// Extracted from reconciler.go: the two-phase QUARANTINED flip with
// deferred outbox emission (retried out of band by downstream consumers).
// Raw SQL here (UPDATE artifacts + INSERT INTO outbox_events) is covered
// by the reconciler sql-allowlist marker atop reconciler.go
// (baseline-ratched in scripts/ci/sql-baseline.txt).
package artifacts

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
)

// =====================================================================
// Rule 3 helper: transactional QUARANTINED flip + ARTIFACT_QUARANTINED
// outbox event.
//
// Two separate commits (NOT one combined tx with soft-skip on missing
// outbox_events). Reason: the single-tx + soft-skip pattern relies on
// SQLite's behavior where a single failed statement does NOT poison a
// whole transaction; this is undocumented and varies across SQLite
// builds (`SQLITE_OMIT_*`, future drivers). Splitting cleanly decouples
// the failure surfaces: QUARANTINED status is always durable when
// emitted; outbox emission is best-effort and reported separately.
// =====================================================================

// isNoSuchTable returns true when err is the SQLite "no such table"
// "no such column" error. Used to soft-skip over schema-roll phases
// where outbox_events / job_deliveries may not yet exist. The
// Reconciler's quarantineArtifactTx uses this to surface a
// status-only quarantine when the outbox_events schema is incomplete.
func isNoSuchTable(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "no such table") ||
		strings.Contains(msg, "no such column") ||
		strings.Contains(msg, "Error 1")
}

// ErrArtifactAlreadyQuarantined is returned when the UPDATE matches 0
// rows because a concurrent reconciler (or admin) has already flipped
// the status. Callers should treat this as success (idempotent).
var ErrArtifactAlreadyQuarantined = errors.New("reconciler: artifact already terminal")

// ErrQuarantineStatusOnly is returned when the QUARANTINED status was
// committed but the ARTIFACT_QUARANTINED outbox event emission failed
// (best-effort). Callers (rule 3 counting) surface this as a separate
// bucket so dashboards can detect outbox schema drift without scraping logs.
var ErrQuarantineStatusOnly = errors.New("reconciler: quarantine status committed but outbox event deferred")

func (r *Reconciler) quarantineArtifactTx(ctx context.Context, artifactID, reason string) error {
	if artifactID == "" {
		return fmt.Errorf("reconciler: quarantineArtifactTx: empty artifactID")
	}

	// Phase 1: flip READY -> QUARANTINED in its own transaction.
	// The CAS WHERE clause prevents stomping concurrent finalizers.
	tx1, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("reconciler: quarantineArtifactTx begin-status: %w", err)
	}
	res, err := tx1.ExecContext(ctx, `
		UPDATE artifacts
		SET status = 'QUARANTINED'
		WHERE id = ? AND status = 'READY'`, artifactID)
	if err != nil {
		_ = tx1.Rollback()
		return fmt.Errorf("reconciler: quarantine UPDATE: %w", err)
	}
	affected, rerr := res.RowsAffected()
	if rerr != nil {
		_ = tx1.Rollback()
		return rerr
	}
	if affected == 0 {
		_ = tx1.Rollback()
		// Idempotent: another reconciler (or admin, or a foreground
		// Finalize that beat us) already flipped to a terminal state.
		return ErrArtifactAlreadyQuarantined
	}
	if err := tx1.Commit(); err != nil {
		return fmt.Errorf("reconciler: quarantineArtifactTx commit-status: %w", err)
	}

	// Phase 2: emit ARTIFACT_QUARANTINED outbox event in its own tx.
	// Best-effort: if outbox_events is missing or the commit fails, the
	// caller (rule 3) learns about it via ErrQuarantineStatusOnly so
	// operators can distinguish "outbox healthy + successful quarantine"
	// from "outbox broken + status-only quarantine" in dashboard stats.
	// The QUARANTINED status flip from Phase 1 is the source of truth;
	// downstream consumers can replay the missed event out of band by
	// re-reading artifacts where status='QUARANTINED'.
	tx2, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		log.Printf("[RECONCILER] quarantine outbox begin failed artifact=%s (status committed): %v", artifactID, err)
		return ErrQuarantineStatusOnly
	}
	payload := fmt.Sprintf(`{"artifact_id":%q,"reason":%q,"detected_at":%q}`,
		artifactID, reason, r.clock.Now().UTC().Format(time.RFC3339))
	now := r.clock.Now().UTC().Format(time.RFC3339)
	if _, err := tx2.ExecContext(ctx, `
		INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload_json, status, available_at, created_at)
		VALUES ('artifact', ?, 'ARTIFACT_QUARANTINED', ?, 'PENDING', ?, ?)`,
		artifactID, payload, now, now); err != nil {
		_ = tx2.Rollback()
		if isNoSuchTable(err) {
			log.Printf("[RECONCILER] outbox_events missing; QUARANTINED status still committed for artifact=%s (event emission deferred)", artifactID)
			return ErrQuarantineStatusOnly
		}
		log.Printf("[RECONCILER] quarantine outbox INSERT failed artifact=%s (status committed): %v", artifactID, err)
		return ErrQuarantineStatusOnly
	}
	if err := tx2.Commit(); err != nil {
		log.Printf("[RECONCILER] quarantine outbox commit failed artifact=%s (status committed): %v", artifactID, err)
		return ErrQuarantineStatusOnly
	}
	return nil
}
