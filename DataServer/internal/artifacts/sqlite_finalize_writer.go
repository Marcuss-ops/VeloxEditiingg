// Package artifacts / sqlite_finalize_writer.go
//
// Single atomic SQL transaction that promotes a job to SUCCEEDED via a
// verified artifact. Sole writer of jobs.status='SUCCEEDED' (audit
// invariant); the scan_test allowlist pins this file as the
// authoritative anchor for that SQL fragment.
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
//	1. validateFinalizingUploadTx — auth + state precondition
//	2. markJobSucceededTx         — sole writer of jobs.status='SUCCEEDED'
//	2.5 markTaskSucceededTx       — sweeps tasks[RUNNING/LEASED/PENDING] → SUCCEEDED
//	3. markArtifactReadyTx        — artifacts STAGING → READY
//	4. resolveDeliveryDestinationsTx — per-job delivery plan
//	5. insertPendingDeliveriesTx  — durable job_deliveries rows
//	6. completeUploadTx           — artifact_uploads FINALIZING → COMPLETED
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

	if err := w.markJobSucceededTx(ctx, tx, cmd, nowStr); err != nil {
		return nil, err
	}
	if err := w.markTaskSucceededTx(ctx, tx, cmd, nowStr); err != nil {
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

// ── Step 1: validateFinalizingUploadTx ──────────────────────────────────

// validateFinalizingUploadTx enforces the auth + state precondition on
// the artifact_uploads row. This is the *only* place in the writer
// where identity is checked at the SQL layer: the subsequent job and
// artifact CASes are identity-free, so any drift here MUST be caught
// before we start flipping other tables.
//
// Tightened to 'FINALIZING' only — accepting 'RECEIVED' here would
// mask a missing orchestration step with a misleading late-stage
// ErrTransitionConflict at the COMPLETED flip below; rejecting here
// surfaces the precondition failure with the correct
// ErrUploadStateInvalid sentinel so the caller can retry.
func (w *SQLiteFinalizeWriter) validateFinalizingUploadTx(ctx context.Context, tx *sql.Tx, cmd FinalizeVerifiedCommand) error {
	pre, err := loadUploadSessionForCASInTx(ctx, tx, cmd.UploadID)
	if err != nil {
		return err
	}
	if pre.Status != "FINALIZING" {
		return fmt.Errorf("%w: upload=%s status=%s (expected FINALIZING — Service.Finalize must CAS RECEIVED->FINALIZING first)",
			ErrUploadStateInvalid, cmd.UploadID, pre.Status)
	}
	if pre.WorkerID != cmd.WorkerID || pre.LeaseID != cmd.LeaseID || pre.AttemptNumber != cmd.AttemptNumber {
		return fmt.Errorf("%w: auth upload=%s worker=%s->%s lease=%s->%s attempt=%d->%d",
			ErrTransitionConflict, cmd.UploadID,
			pre.WorkerID, cmd.WorkerID, pre.LeaseID, cmd.LeaseID,
			pre.AttemptNumber, cmd.AttemptNumber)
	}
	return nil
}

// ── Step 2: markJobSucceededTx ──────────────────────────────────────────

// markJobSucceededTx is the sole writer of jobs.status='SUCCEEDED'.
// The audit contract enforced by scan_test.go pivots on this query
// being anchored in this file.
//
// WHERE allows status IN ('RUNNING', 'AWAITING_ARTIFACT'). The
// AWAITING_ARTIFACT branch is the post-task-completion state
// written elsewhere once all tasks succeed; this writer closes
// the loop to SUCCEEDED. RUNNING → SUCCEEDED is preserved for
// legacy workers without an artifact contract (defensive backward
// compat).
//
// Optional cmd.ExpectedRevision>0 adds an extra CAS guard
// (optimistic concurrency) — when zero, the guard is omitted to
// match the legacy "any in-flight run" semantic.
func (w *SQLiteFinalizeWriter) markJobSucceededTx(ctx context.Context, tx *sql.Tx, cmd FinalizeVerifiedCommand, nowStr string) error {
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
		return fmt.Errorf("artifacts: FinalizeVerified jobs CAS: %w", err)
	}
	if n, _ := jobRes.RowsAffected(); n != 1 {
		return fmt.Errorf("%w: jobs affected=%d upload=%s",
			ErrTransitionConflict, n, cmd.UploadID)
	}
	return nil
}

// ── Step 2.5: markTaskSucceededTx ──────────────────────────────────────
//
// markTaskSucceededTx sweeps the canonical `tasks` row for this job_id
// to SUCCEEDED inside the same tx that flipped jobs.status='SUCCEEDED'
// in Step 2. Closes the well-known desync surfaced by Phase 1.5
// invariant Q5 (scripts/ci/check-completion-protocol-invariants.sh):
// a job that the closure tx commits as SUCCEEDED while a worker
// release / fast-abort-finalization has stranded the corresponding
// task in RUNNING/LEASED/PENDING.
//
// WHERE accepts status IN ('RUNNING','LEASED','PENDING') so we cover:
//   - the common case: task was RUNNING when the worker submitted the
//     verified-finalize RPC (Lease → RUNNING closed cleanly),
//   - the fast-abort case: task was PENDING (master promoted Job =
//     SUCCEEDED before the claimant flip ran),
//   - the defensive case: a still-LEASED task whose offer never
//     produced an AcceptTaskAtomic.
//
// RowsAffected == 0 is INTENTIONALLY not a Tx-fatal condition: not
// every job has a tasks row (legacy job-only ingestion paths pre-
// migration-039 may legitimately have no tasks row at INSERT time).
// The Q5 invariant is the post-fix gate that catches
// real desync; this step enforces it forward.
//
// HARD DEPENDENCY: this writer requires migration 039 (the
// DataServer/internal/store/migrations/sqlite/039_tasks.sql create
// of the `tasks` table). RunMigrations
// (DataServer/internal/store/migrations/runner.go) auto-applies
// every embedded pending migration in version order on the master
// boot path, so a healthy production deploy always has 039 in place
// before any finalize RPC reaches this writer.
//
// On a half-migrated pre-039 DB the UPDATE below surfaces
// "no such table: tasks" and rolls the entire finalization tx back
// — this is the intended fail-fast signal so a half-migrated
// deploy cannot silently land Q5-flagged SUCCEEDED jobs.
//
// TEST FIXTURES: openPropagationDB in retry_budget_propagation_test.go
// bypasses RunMigrations and MUST ship its own `tasks` table mirroring
// DataServer/internal/store/migrations/sqlite/039_tasks.sql, or
// FinalizeVerified will roll back at Step 2.5 with the same
// `no such table: tasks` error. Static fixtures that produce jobs via
// direct INSERT into `jobs` (no `tasks` row) are still safe —
// RowsAffected == 0 at Step 2.5 is non-fatal by design.
func (w *SQLiteFinalizeWriter) markTaskSucceededTx(ctx context.Context, tx *sql.Tx, cmd FinalizeVerifiedCommand, nowStr string) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE tasks
		SET status = 'SUCCEEDED',
		    completed_at = ?,
		    updated_at   = ?
		WHERE job_id = ?
		  AND status IN ('RUNNING', 'LEASED', 'PENDING')`,
		nowStr, nowStr, cmd.JobID,
	)
	if err != nil {
		return fmt.Errorf("artifacts: FinalizeVerified tasks sweep: %w", err)
	}
	// n intentionally not asserted: see doc above. The tx only commits
	// the flip if subsequent steps succeed, so a partial "no task row
	// updated" pass is fine — repair via Q5 / reconciliation if needed.
	return nil
}

// ── Step 3: markArtifactReadyTx ─────────────────────────────────────────

// markArtifactReadyTx flips artifacts.status: STAGING → READY and
// stamps the master-computed (storage_provider, storage_key, sha256,
// size, mime, verified_at) tuple atomically with the job flip. The
// shared tx guarantees a partial state where Job is SUCCEEDED but
// artifacts is still STAGING cannot be observed.
func (w *SQLiteFinalizeWriter) markArtifactReadyTx(ctx context.Context, tx *sql.Tx, cmd FinalizeVerifiedCommand, nowStr string) error {
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
		return fmt.Errorf("artifacts: FinalizeVerified artifacts CAS: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("%w: artifacts affected=%d artifact=%s",
			ErrTransitionConflict, n, cmd.ArtifactID)
	}
	return nil
}

// ── Step 4: resolveDeliveryDestinationsTx ───────────────────────────────

// resolveDeliveryDestinationsTx computes the per-job delivery
// destination set inside the same tx that INSERTs into job_deliveries
// (transactional safety for the per-job delivery plan).
//
// Resolution order:
//  1. cmd.DestinationID explicit override → single-destination plan
//     with max_attempts=5 (schema default). The cmd-level pin always
//     wins over a per-job plan because it pins routing to one tail.
//  2. w.resolver wired → delegated via DeliveryPlanResolver.
//  3. nil resolver → legacy all-enabled-destinations SELECT inside
//     the tx. max_attempts defaults to 5 because there is no
//     per-plan budget to consult.
//
// Step 5/8 of the canonical-purity plan: switch from the legacy
// resolver (which dropped retry_budget at the interface boundary) to
// a per-destination projection that carries MaxAttempts, then stamp
// it on the INSERT so the durable attempt cap survives worker
// restarts. Resolved inline because the resolution happens inside
// the same tx that INSERTs job_deliveries; splitting it out would
// force a destination slice across the writer boundary with no
// separation win.
//
// rows.Close is deferred inside the helper so cursor cleanup is
// automatic even on early-return Scan errors.
func (w *SQLiteFinalizeWriter) resolveDeliveryDestinationsTx(ctx context.Context, tx *sql.Tx, cmd FinalizeVerifiedCommand) ([]DeliveryDestination, error) {
	if cmd.DestinationID != "" {
		// Single-destination explicit path.
		return []DeliveryDestination{{
			DestinationID: cmd.DestinationID,
			MaxAttempts:   5,
		}}, nil
	}
	if w.resolver != nil {
		rd, rerr := w.resolver.ResolveDestinations(ctx, cmd.JobID, cmd.ArtifactID)
		if rerr != nil {
			return nil, fmt.Errorf("artifacts: FinalizeVerified plan resolver: %w", rerr)
		}
		return rd, nil
	}
	// No resolver wired: legacy all-enabled-destinations SELECT
	// inside the tx. max_attempts defaults to 5.
	rows, qerr := tx.QueryContext(ctx,
		`SELECT destination_id FROM delivery_destinations WHERE enabled = 1`)
	if qerr != nil {
		return nil, fmt.Errorf("artifacts: FinalizeVerified destinations SELECT: %w", qerr)
	}
	defer rows.Close()
	var resolved []DeliveryDestination
	for rows.Next() {
		var did string
		if err := rows.Scan(&did); err != nil {
			return nil, fmt.Errorf("artifacts: FinalizeVerified destinations Scan: %w", err)
		}
		if did == "" {
			continue
		}
		resolved = append(resolved, DeliveryDestination{
			DestinationID: did,
			MaxAttempts:   5,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("artifacts: FinalizeVerified destinations iter: %w", err)
	}
	return resolved, nil
}

// ── Step 5: insertPendingDeliveriesTx ───────────────────────────────────

// insertPendingDeliveriesTx materializes one job_deliveries row per
// resolved destination, idempotent on (artifact_id, destination_id)
// via the WHERE NOT EXISTS guard so a re-run of the same tx (e.g.
// after a transient commit error) cannot create duplicate delivery
// rows.
//
// Defense-in-depth: a resolver that returned MaxAttempts=0
// (e.g. pre-069 plan read returning the table default but also
// explicitly zeroed) must NOT translate to
// job_deliveries.max_attempts=0 — the runner's
// `lease.AttemptNumber >= maxAttempts` branch would mark FAILED on
// attempt 1. Re-enforce the schema default (5) here to keep the
// INSERT contract pinned.
//
// idempotency_key = "<artifact_id>_<destination_id>" so the
// deterministic uniqueness constraint at the SQL layer (see
// migrations 0xx) is also a no-op when the same (artifact,
// destination) pair is presented twice.
func (w *SQLiteFinalizeWriter) insertPendingDeliveriesTx(ctx context.Context, tx *sql.Tx, cmd FinalizeVerifiedCommand, nowStr string, resolved []DeliveryDestination) error {
	for _, dest := range resolved {
		deliveryID, err := identity.NewHex128()
		if err != nil {
			return fmt.Errorf("generate delivery ID: %w", err)
		}
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
			return fmt.Errorf("artifacts: FinalizeVerified job_deliveries insert (dest=%s, max_attempts=%d): %w",
				dest.DestinationID, maxAttempts, err)
		}
	}
	return nil
}

// ── Step 6: completeUploadTx ────────────────────────────────────────────

// completeUploadTx is the closing write of the verified-finalization
// tx: artifact_uploads FINALIZING → COMPLETED. Joining the same
// *sql.Tx avoids a liveness bug where a process crash between
// tx-commit and a separate post-commit UPDATE would leave the upload
// row stuck in FINALIZING forever, blocking retries even though jobs
// and artifacts are already SUCCEEDED.
func (w *SQLiteFinalizeWriter) completeUploadTx(ctx context.Context, tx *sql.Tx, cmd FinalizeVerifiedCommand, nowStr string) error {
	res, err := tx.ExecContext(ctx, `
		UPDATE artifact_uploads
		SET status = 'COMPLETED',
		    completed_at = ?
		WHERE upload_id = ?
		  AND status = 'FINALIZING'`,
		nowStr, cmd.UploadID)
	if err != nil {
		return fmt.Errorf("artifacts: FinalizeVerified artifact_uploads CAS: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("%w: upload affected=%d upload=%s",
			ErrTransitionConflict, n, cmd.UploadID)
	}
	return nil
}
