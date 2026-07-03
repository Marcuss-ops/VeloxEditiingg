// Package artifacts / upload_session_writer.go — Blocco 4 step #5 split.
//
// upload_session_writer.go owns SQL for the `artifact_uploads` table
// inside the verified-finalization tx. The functions are unexported;
// external callers go through SQLiteFinalizationRepository.
//
// Companion to artifact_writer.go — same architecture: per-table SQL
// extracted into its own file so the coordinator can route each
// write through a single dedicated function. Atomicity is preserved
// because every in-tx method here receives the coordinator's *sql.Tx.
//
// The package-level nilOrString / nilOrStringPtr / formatTimePtr /
// parseTimeRFC3339 helpers are consolidated here as the canonical
// home for the artifacts package. They were previously duplicated as
// private file-locals in sqlite_finalization_repository.go; the
// store package keeps its own duplicate in internal/store/artifact_uploads.go
// until file-3/4 of the canonical-SQL-gateway migration retires both
// copies (per the inline comment there).
package artifacts

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ── IN-TX METHODS ───────────────────────────────────────────────────────

// insertUploadSessionCreatedInTx inserts a CREATED artifact_uploads
// row paired with the supplied uploadID. Joins the same tx as the
// matching artifacts STAGING insert in CreateArtifactAndUploadSession
// so the pair commits or rolls back together.
func insertUploadSessionCreatedInTx(
	ctx context.Context,
	tx *sql.Tx,
	cmd CreateArtifactAndUploadSessionCommand,
	now, expiresAt time.Time,
) error {
	if cmd.ArtifactID == "" || cmd.UploadID == "" || cmd.JobID == "" {
		return fmt.Errorf("artifacts: insertUploadSessionCreatedInTx: artifact_id, upload_id and job_id are required")
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO artifact_uploads (
		    upload_id, artifact_id, job_id, attempt_number, worker_id, lease_id,
		    status, temporary_storage_key,
		    expected_size_bytes, expected_sha256,
		    expected_revision,
		    received_size_bytes, received_sha256,
		    created_at, expires_at, completed_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		cmd.UploadID, cmd.ArtifactID, cmd.JobID, cmd.AttemptNumber,
		cmd.WorkerID, cmd.LeaseID,
		"CREATED", cmd.TemporaryStorageKey,
		cmd.ExpectedSizeBytes, nilOrString(cmd.ExpectedSHA256),
		cmd.ExpectedRevision,
		0, nil,
		now.UTC().Format(time.RFC3339),
		expiresAt.UTC().Format(time.RFC3339),
		nil,
	); err != nil {
		return fmt.Errorf("artifacts: insertUploadSessionCreatedInTx: %w", err)
	}
	return nil
}

// uploadCASPrecondition is the shape returned by
// loadUploadSessionForCASInTx. Batches the four CAS-precondition
// columns so the coordinator can validate them inline.
type uploadCASPrecondition struct {
	Status        string
	WorkerID      string
	LeaseID       string
	AttemptNumber int
}

// loadUploadSessionForCASInTx reads (status, worker_id, lease_id,
// attempt_number) for the CAS-precondition check at Step 1 of
// FinalizeVerified. The coordinator asserts status='FINALIZING' and
// worker/lease/attempt match the command — Service.Finalize must CAS
// RECEIVED → FINALIZING before invoking.
//
// Returns ErrUploadNotFound (wrapped) when 0 rows match.
func loadUploadSessionForCASInTx(
	ctx context.Context,
	tx *sql.Tx,
	uploadID string,
) (*uploadCASPrecondition, error) {
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

// casUploadSessionCompletedInTx flips FINALIZING → COMPLETED and
// stamps completed_at. The CAS is the closing write of the
// verified-finalization tx, joining the same *sql.Tx as the jobs CAS
// + artifacts CAS + job_deliveries INSERT.
func casUploadSessionCompletedInTx(
	ctx context.Context,
	tx *sql.Tx,
	uploadID, completedAtStr string,
) error {
	if uploadID == "" {
		return fmt.Errorf("artifacts: casUploadSessionCompletedInTx: empty uploadID")
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE artifact_uploads
		SET status = 'COMPLETED',
		    completed_at = ?
		WHERE upload_id = ?
		  AND status = 'FINALIZING'`,
		completedAtStr, uploadID)
	if err != nil {
		return fmt.Errorf("artifacts: casUploadSessionCompletedInTx: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("%w: upload affected=%d upload=%s",
			ErrTransitionConflict, n, uploadID)
	}
	return nil
}

// ── PACKAGE-LEVEL HELPERS ───────────────────────────────────────────────
//
// Canonical home in the artifacts package. Internal/store keeps its
// own duplicate of nilOrString in internal/store/artifact_uploads.go
// until file-3/4 of the canonical-SQL-gateway migration retires both
// copies.

// nilOrString maps "" → nil so the column stores NULL rather than "",
// matching the migration's nullable TEXT columns for expected_sha256.
func nilOrString(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

// nilOrStringPtr is the *string counterpart of nilOrString: a nil
// pointer or a pointer-to-empty maps to nil.
func nilOrStringPtr(p *string) interface{} {
	if p == nil || *p == "" {
		return nil
	}
	return *p
}

// formatTimePtr renders a *time.Time as RFC3339 UTC, or nil if unset.
func formatTimePtr(p *time.Time) interface{} {
	if p == nil || p.IsZero() {
		return nil
	}
	return p.UTC().Format(time.RFC3339)
}

// parseTimeRFC3339 parses an RFC3339 string into a time.Time, or
// zero-value when the string is empty.
func parseTimeRFC3339(t *time.Time, raw string) error {
	if raw == "" {
		*t = time.Time{}
		return nil
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return err
	}
	*t = parsed
	return nil
}
