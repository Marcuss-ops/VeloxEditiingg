// Package artifacts / sqlite_finalize_writer.go
//
// Single atomic SQL transaction that promotes a job to SUCCEEDED via a
// verified artifact. Sole writer of jobs.status='SUCCEEDED' (audit
// invariant); the scan_test allowlist pins finalize_phases.go (the
// jobs CAS step) as the authoritative anchor for that SQL fragment.
//
// FinalizeVerified is decomposed into 7 private *Tx step methods on
// *SQLiteFinalizeWriter (Step 2.5 is the tasks sweep that closes the
// documented "jobs SUCCEEDED but tasks RUNNING/LEASED/PENDING" desync
// enforced by invariant Q5). Each step:
//   - Receives the caller's *sql.Tx (does NOT open its own).
//   - Performs ONE logical CAS / read / insert.
//   - Returns a wrapped ErrTransitionConflict on RowsAffected != 1.
//   - Does NOT commit, rollback, or call tx.End — those remain
//     exclusively in the orchestrator so the whole flow stays
//     atomic. The orchestrator's defer-Rollback is the single
//     safety net; the steps must NEVER swallow that contract by
//     issuing their own tx finalization.
//
// The step methods live in the sibling files of this package:
//   - finalize_phases.go   — validateFinalizingUploadTx,
//     markJobSucceededTx, markTaskSucceededTx, markArtifactReadyTx
//     (the per-table CAS flips, steps 1–3).
//   - finalize_deliveries.go — resolveDeliveryDestinationsTx,
//     insertPendingDeliveriesTx, completeUploadTx (steps 4–6).
//
// This file keeps the contract surface (interface, struct, wiring,
// orchestrator) plus the CAS-precondition helper that steps depend on.
package artifacts

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"velox-server/internal/store"
)

// FinalizationWriter is the verified-finalization persistence contract:
// one method, one tx, single-writer of jobs.status='SUCCEEDED'.
//
// Invariants:
//   - One *sql.Tx wraps the entire flow. Any inner error rolls the tx
//     back; jobs, artifacts, artifact_uploads, and job_deliveries
//     either commit together or are not touched at all.
//   - The job_id CAS at step 2 is identity-free at the SQL layer; auth
//     is fully verified at step 1 (artifact_uploads CAS on
//     status='FINALIZING' + worker_id + lease_id + attempt_number).
//   - No other layer may flip jobs.status='SUCCEEDED'. The audit
//     visibility of this tx is the contract enforced by scan_test.go.
//
// Preconditions:
//   - cmd.UploadID, cmd.ArtifactID, cmd.JobID must be non-empty.
//   - cmd.UploadID must be in FINALIZING state with worker_id + lease_id
//   - attempt_number matching the cmd. The Service.Finalize path
//     performs the RECEIVED→FINALIZING CAS before delegating here.
//
// Error behavior:
//   - Empty identity field            → fmt.Errorf("... required").
//   - Upload not in FINALIZING       → ErrUploadStateInvalid with the
//     actual status surfaced (caller
//     must transition first).
//   - worker/lease/attempt mismatch  → ErrTransitionConflict with both
//     sides of the auth diff reported.
//   - jobs CAS affects != 1         → ErrTransitionConflict (status
//     not in RUNNING/AWAITING_ARTIFACT,
//     or revision mismatch).
//   - artifacts CAS affects != 1    → ErrTransitionConflict (artifact
//     not in STAGING, or id/job mismatch).
//   - artifact_uploads FINALIZING→COMPLETED CAS affects != 1
//     → ErrTransitionConflict (peer stole
//     the FINALIZING slot mid-tx).
//   - Post-tx reader returns nil    → wrapped hard error (after a
//     successful CAS on the same id
//     the row MUST exist).
type FinalizationWriter interface {
	FinalizeVerified(ctx context.Context, cmd FinalizeVerifiedCommand) (*store.Artifact, error)
}

// SQLiteFinalizeWriter is the SQLite-backed FinalizationWriter.
//
// SQLite serializes writers; concurrent FinalizeVerified callers on the
// same upload_id are race-free at the SQL layer because the
// artifact_uploads FINALIZING→COMPLETED CAS picks exactly one winner.
type SQLiteFinalizeWriter struct {
	db     *sql.DB
	reader ArtifactReader
	// resolver is optional for render-only/test finalization paths. When
	// a delivery is required, a nil resolver fails closed rather than
	// selecting global destinations. Wired at construction so the
	// explicit destination set is resolved alongside finalization.
	resolver DeliveryPlanResolver
}

// NewSQLiteFinalizeWriter wires the finalize writer. The reader is
// required (post-tx SELECT); a nil resolver is fail-closed for
// delivery finalization.
func NewSQLiteFinalizeWriter(db *sql.DB, reader ArtifactReader, resolver DeliveryPlanResolver) *SQLiteFinalizeWriter {
	if db == nil {
		panic("artifacts: NewSQLiteFinalizeWriter requires a non-nil *sql.DB")
	}
	if reader == nil {
		panic("artifacts: NewSQLiteFinalizeWriter requires a non-nil ArtifactReader (consumed by post-tx SELECT)")
	}
	return &SQLiteFinalizeWriter{db: db, reader: reader, resolver: resolver}
}

var _ FinalizationWriter = (*SQLiteFinalizeWriter)(nil)

// ── CAS-precondition helper (shared by validateFinalizingUploadTx) ──────

// uploadCASPrecondition is the per-row snapshot read at the
// artifact_uploads CAS-precondition step. Batches the four auth
// columns so the precondition check is one Scan.
type uploadCASPrecondition struct {
	Status        string
	WorkerID      string
	LeaseID       string
	AttemptNumber int
}

// loadUploadSessionForCASInTx reads the four CAS-precondition columns
// for an artifact_uploads row inside the supplied tx.
//
// Returns ErrUploadNotFound (wrapped) when 0 rows match.
func loadUploadSessionForCASInTx(ctx context.Context, tx *sql.Tx, uploadID string) (*uploadCASPrecondition, error) {
	if uploadID == "" {
		return nil, fmt.Errorf("artifacts: loadUploadSessionForCASInTx: empty uploadID")
	}
	row := tx.QueryRowContext(ctx, `
		SELECT status, worker_id, lease_id, attempt_number
		FROM artifact_uploads WHERE upload_id = ?`, uploadID)
	out := &uploadCASPrecondition{}
	if scanErr := row.Scan(&out.Status, &out.WorkerID, &out.LeaseID, &out.AttemptNumber); scanErr != nil {
		if errors.Is(scanErr, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: upload_id=%s", ErrUploadNotFound, uploadID)
		}
		return nil, fmt.Errorf("artifacts: loadUploadSessionForCASInTx: %w", scanErr)
	}
	return out, nil
}

// ── FinalizeVerified orchestrator (single external tx boundary) ─────────

// FinalizeVerified is the verified-finalization entry point. It opens
// the *single* SQL transaction that wraps the entire finalization
// flow, then dispatches to 6 private *Tx step methods in order:
//
//  1. validateFinalizingUploadTx — auth + state precondition
//  2. markJobSucceededTx         — sole writer of jobs.status='SUCCEEDED'
//     2.5 markTaskSucceededTx       — sweeps tasks[RUNNING/LEASED/PENDING] → SUCCEEDED
//  3. markArtifactReadyTx        — artifacts STAGING → READY
//  4. resolveDeliveryDestinationsTx — per-job delivery plan
//  5. insertPendingDeliveriesTx  — durable job_deliveries rows
//  6. completeUploadTx           — artifact_uploads FINALIZING → COMPLETED
//
// The Commit + post-tx artifact read happen here, never inside the
// steps. Any step error propagates up: the defer-Rollback reverts
// the entire tx atomically.
func (w *SQLiteFinalizeWriter) FinalizeVerified(ctx context.Context, cmd FinalizeVerifiedCommand) (*store.Artifact, error) {
	if cmd.UploadID == "" || cmd.ArtifactID == "" || cmd.JobID == "" {
		return nil, fmt.Errorf("artifacts: FinalizeVerified: upload/artifact/job ids are required")
	}

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("artifacts: FinalizeVerified begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if err := w.validateFinalizingUploadTx(ctx, tx, cmd); err != nil {
		return nil, err
	}

	verifiedAt := cmd.VerifiedAt
	if verifiedAt.IsZero() {
		verifiedAt = time.Now().UTC()
	}
	nowStr := verifiedAt.UTC().Format(time.RFC3339)

	if err := w.markTaskSucceededTx(ctx, tx, cmd, nowStr); err != nil {
		return nil, err
	}
	if err := w.markJobSucceededTx(ctx, tx, cmd, nowStr); err != nil {
		return nil, err
	}
	if err := w.markArtifactReadyTx(ctx, tx, cmd, nowStr); err != nil {
		return nil, err
	}
	resolved, err := w.resolveDeliveryDestinationsTx(ctx, tx, cmd)
	if err != nil {
		return nil, err
	}
	if err := w.insertPendingDeliveriesTx(ctx, tx, cmd, nowStr, resolved); err != nil {
		return nil, err
	}
	if err := w.completeUploadTx(ctx, tx, cmd, nowStr); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("artifacts: FinalizeVerified commit: %w", err)
	}
	committed = true

	// Post-tx artifact read via the read-only reader. A nil result is
	// a data-integrity bug — after a successful CAS on the same id
	// the row MUST exist.
	out, err := w.reader.GetByID(ctx, cmd.ArtifactID)
	if err != nil {
		return nil, fmt.Errorf("artifacts: FinalizeVerified post-tx read: %w", err)
	}
	if out == nil {
		return nil, fmt.Errorf("artifacts: FinalizeVerified post-tx read: artifact %s missing after successful CAS",
			cmd.ArtifactID)
	}
	return out, nil
}
