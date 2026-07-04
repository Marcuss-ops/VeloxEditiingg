// Package artifacts / sqlite_finalize_writer.go
//
// Single atomic SQL transaction that promotes a job to SUCCEEDED via a
// verified artifact. Sole writer of jobs.status='SUCCEEDED' (audit
// invariant); the scan_test allowlist pins this file as the
// authoritative anchor for that SQL fragment.
package artifacts

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"velox-server/internal/identity"
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
	// resolver is optional: nil falls through to the all-enabled
	// destinations SELECT inside the tx. Wired at construction so the
	// resolved destination set is computed inside the same tx that
	// INSERTs into job_deliveries (transactional safety for the
	// per-job delivery plan).
	resolver DeliveryPlanResolver
}

// NewSQLiteFinalizeWriter wires the finalize writer. The reader is
// required (post-tx SELECT); resolver is optional (nil = all-enabled
// fallback).
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

	// Step 1: artifact_uploads CAS-precondition load. Tightened to
	// 'FINALIZING' only — accepting 'RECEIVED' here would mask a
	// missing orchestration step with a misleading late-stage
	// ErrTransitionConflict at the COMPLETED flip below; rejecting
	// here surfaces the precondition failure with the correct
	// ErrUploadStateInvalid sentinel so the caller can retry.
	pre, err := loadUploadSessionForCASInTx(ctx, tx, cmd.UploadID)
	if err != nil {
		return nil, err
	}
	if pre.Status != "FINALIZING" {
		return nil, fmt.Errorf("%w: upload=%s status=%s (expected FINALIZING — Service.Finalize must CAS RECEIVED->FINALIZING first)",
			ErrUploadStateInvalid, cmd.UploadID, pre.Status)
	}
	if pre.WorkerID != cmd.WorkerID || pre.LeaseID != cmd.LeaseID || pre.AttemptNumber != cmd.AttemptNumber {
		return nil, fmt.Errorf("%w: auth upload=%s worker=%s->%s lease=%s->%s attempt=%d->%d",
			ErrTransitionConflict, cmd.UploadID,
			pre.WorkerID, cmd.WorkerID, pre.LeaseID, cmd.LeaseID,
			pre.AttemptNumber, cmd.AttemptNumber)
	}

	verifiedAt := cmd.VerifiedAt
	if verifiedAt.IsZero() {
		verifiedAt = time.Now().UTC()
	}
	nowStr := verifiedAt.UTC().Format(time.RFC3339)

	// Step 2: jobs CAS — sole writer of jobs.status='SUCCEEDED'.
	//
	// WHERE allows status IN ('RUNNING', 'AWAITING_ARTIFACT'). The
	// AWAITING_ARTIFACT branch is the post-task-completion state
	// written elsewhere once all tasks succeed; this writer closes
	// the loop to SUCCEEDED. RUNNING → SUCCEEDED is preserved for
	// legacy workers without an artifact contract (defensive backward
	// compat).
	jobQuery := `
		UPDATE jobs
		SET status = 'SUCCEEDED',
		    completed_at = ?,
		    updated_at   = ?,
		    revision     = revision + 1
		WHERE job_id = ?
		  AND status IN ('RUNNING', 'AWAITING_ARTIFACT')`
	jobArgs := []interface{}{nowStr, nowStr, cmd.JobID}
	if cmd.ExpectedRevision != 0 {
		jobQuery += ` AND revision = ?`
		jobArgs = append(jobArgs, cmd.ExpectedRevision)
	}
	jobRes, err := tx.ExecContext(ctx, jobQuery, jobArgs...)
	if err != nil {
		return nil, fmt.Errorf("artifacts: FinalizeVerified jobs CAS: %w", err)
	}
	if n, _ := jobRes.RowsAffected(); n != 1 {
		return nil, fmt.Errorf("%w: jobs affected=%d upload=%s",
			ErrTransitionConflict, n, cmd.UploadID)
	}

	// Step 3: artifacts CAS: STAGING → READY. Master-computed
	// (storage_key, sha256, size, mime) stamped here; the tx commits
	// them atomically with the Job flip so a partial state where Job
	// is SUCCEEDED but artifacts is still STAGING cannot be observed.
	res, err := tx.ExecContext(ctx, `
		UPDATE artifacts
		SET status = 'READY',
		    storage_provider = ?,
		    storage_key = ?,
		    sha256 = ?, size_bytes = ?, mime_type = ?,
		    verified_at = ?
		WHERE id = ? AND job_id = ? AND status = 'STAGING'`,
		cmd.StorageProvider, cmd.StorageKey,
		cmd.SHA256, cmd.SizeBytes, cmd.MIMEType,
		nowStr,
		cmd.ArtifactID, cmd.JobID,
	)
	if err != nil {
		return nil, fmt.Errorf("artifacts: FinalizeVerified artifacts CAS: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return nil, fmt.Errorf("%w: artifacts affected=%d artifact=%s",
			ErrTransitionConflict, n, cmd.ArtifactID)
	}

	// Step 4: per-job delivery destinations + per-destination max_attempts.
	//
	// Step 5/8 of the canonical-purity plan: switch from
	// w.resolver.ResolveDestinations (which dropped retry_budget at the
	// interface boundary) to a per-destination projection that carries
	// MaxAttempts, then stamp it on the INSERT so the durable attempt
	// cap survives worker restarts. Resolved inline because the
	// resolution happens inside the same tx that INSERTs
	// job_deliveries; splitting it out would force a destination slice
	// across the writer boundary with no separation win.
	var resolved []DeliveryDestination
	if cmd.DestinationID != "" {
		// Single-destination explicit path. The cmd-level
		// DestinationID always wins over a per-job plan because it
		// pins routing to one tail (override semantics). max_attempts
		// defaults to 5 (schema default).
		resolved = []DeliveryDestination{{
			DestinationID: cmd.DestinationID,
			MaxAttempts:   5,
		}}
	} else if w.resolver != nil {
		rd, rerr := w.resolver.ResolveDestinations(ctx, cmd.JobID, cmd.ArtifactID)
		if rerr != nil {
			return nil, fmt.Errorf("artifacts: FinalizeVerified plan resolver: %w", rerr)
		}
		resolved = rd
	} else {
		// No resolver wired: legacy all-enabled-destinations SELECT
		// inside the tx. max_attempts defaults to 5 because there is
		// no per-plan budget to consult.
		rows, qerr := tx.QueryContext(ctx,
			`SELECT destination_id FROM delivery_destinations WHERE enabled = 1`)
		if qerr == nil {
			defer rows.Close()
			for rows.Next() {
				var did string
				if err := rows.Scan(&did); err == nil && did != "" {
					resolved = append(resolved, DeliveryDestination{
						DestinationID: did,
						MaxAttempts:   5,
					})
				}
			}
		}
	}

	for _, dest := range resolved {
		deliveryID, err := identity.NewHex128()
		if err != nil {
			return nil, fmt.Errorf("generate delivery ID: %w", err)
		}
		// Defense-in-depth: a resolver that returned MaxAttempts=0
		// (e.g. pre-069 plan read returning the table default but
		// also explicitly zeroed) must NOT translate to
		// job_deliveries.max_attempts=0 — the runner's
		// `lease.AttemptNumber >= maxAttempts` branch would mark FAILED
		// on attempt 1. Re-enforce the schema default here to keep the
		// INSERT contract pinned.
		maxAttempts := dest.MaxAttempts
		if maxAttempts <= 0 {
			maxAttempts = 5
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO job_deliveries (delivery_id, artifact_id, destination_id, status, max_attempts, idempotency_key, created_at, updated_at)
			SELECT ?, ?, ?, 'PENDING', ?, ?, ?, ?
			WHERE NOT EXISTS (
				SELECT 1 FROM job_deliveries
				WHERE artifact_id = ? AND destination_id = ?
			)`,
			deliveryID, cmd.ArtifactID, dest.DestinationID,
			maxAttempts, cmd.ArtifactID+"_"+dest.DestinationID, nowStr, nowStr,
			cmd.ArtifactID, dest.DestinationID,
		)
		if err != nil {
			return nil, fmt.Errorf("artifacts: FinalizeVerified job_deliveries insert (dest=%s, max_attempts=%d): %w",
				dest.DestinationID, maxAttempts, err)
		}
	}

	// Step 5: artifact_uploads CAS: FINALIZING → COMPLETED. Closing
	// write of the verified-finalization tx; joining the same *sql.Tx
	// avoids a liveness bug where a process crash between tx-commit
	// and a separate post-commit UPDATE would leave the upload row
	// stuck in FINALIZING forever, blocking retries even though jobs
	// and artifacts are already SUCCEEDED.
	res, err = tx.ExecContext(ctx, `
		UPDATE artifact_uploads
		SET status = 'COMPLETED',
		    completed_at = ?
		WHERE upload_id = ?
		  AND status = 'FINALIZING'`,
		nowStr, cmd.UploadID)
	if err != nil {
		return nil, fmt.Errorf("artifacts: FinalizeVerified artifact_uploads CAS: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return nil, fmt.Errorf("%w: upload affected=%d upload=%s",
			ErrTransitionConflict, n, cmd.UploadID)
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
