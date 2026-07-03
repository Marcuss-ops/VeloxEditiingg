// sql-allowlist: artifacts SQLiteFinalizationRepository — sole atomic-tx writer for jobs.status=SUCCEEDED (single-writer enforced by scan_test.go). Future refactor candidate for relocation into internal/store alongside the other typed repos.

// Package artifacts / sqlite_finalization_repository.go — PR 3.5-a / Blocco 4 step #5.
//
// This file is the ONLY legal writer of jobs.status='SUCCEEDED'.
// The scan test (scan_test.go) greps every .go under internal/ and
// rejects any single-quoted SQL writer of that terminal state outside
// the audited allowlist (see scan_test.go for the precise regex).
//
// Blocco 4 step #5 split: per-table SQL previously interleaved in
// this file is now split into two unexported per-table writers:
//
//   - artifact_writer.go        (`artifacts` table — STAGING insert, READY CAS, post-tx projection read)
//   - upload_session_writer.go  (`artifact_uploads` table — CREATED insert, FINALIZING CAS-precondition load, COMPLETED CAS, nilOrString helpers)
//
// This file is now a slim TX coordinator: owns the *sql.Tx lifecycle,
// the jobs.status='SUCCEEDED' write (kept INLINE to satisfy the
// scan_test.go allowlist that explicitly pin-points this file), and
// the per-destination job_deliveries INSERT resolver pipeline.
// job_deliveries INSERT stays here because the per-destination set is
// computed by the DeliveryPlanResolver (or the fallback all-enabled
// scan) inside the same tx, and splitting it would force the writer
// to receive a destination slice — ergonomics noise without any
// separation win.
//
// Atomicity is preserved exactly: every per-table write still joins
// the same *sql.Tx. The split is structural — same SQL, same
// ordering, same guarantees.
package artifacts

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"velox-server/internal/identity"
	"velox-server/internal/store"
)

// SQLiteFinalizationRepository is the SQLite-backed implementation of
// FinalizationRepository. SQLite serializes writers, so concurrent
// FinalizeVerified callers on the same job_id race-free at the SQL
// layer; service-layer ENFORCES the state-machine legality (RECEIVED
// then FINALIZING then COMPLETED) before this code runs.
//
// Blocco 4 step #5 note: this struct no longer embeds any per-table
// SQL. It owns the *sql.DB handle + the optional DeliveryPlanResolver.
// Per-table writes are routed to artifact_writer.go /
// upload_session_writer.go through the coordinator's *sql.Tx.
type SQLiteFinalizationRepository struct {
	db           *sql.DB
	planResolver DeliveryPlanResolver // optional; nil falls back to all enabled destinations
}

// NewSQLiteFinalizationRepository wraps an existing *sql.DB. The caller
// owns the connection (typically the same one used by
// store.SQLiteStore).
func NewSQLiteFinalizationRepository(db *sql.DB) *SQLiteFinalizationRepository {
	if db == nil {
		panic("artifacts: NewSQLiteFinalizationRepository requires a non-nil *sql.DB")
	}
	return &SQLiteFinalizationRepository{db: db}
}

// WithPlanResolver attaches a DeliveryPlanResolver to the repository.
// When set, FinalizeVerified uses it to resolve per-job delivery destinations
// instead of querying all enabled delivery_destinations globally.
func (r *SQLiteFinalizationRepository) WithPlanResolver(resolver DeliveryPlanResolver) *SQLiteFinalizationRepository {
	r.planResolver = resolver
	return r
}

// Compile-time interface checks.
var (
	_ FinalizationRepository = (*SQLiteFinalizationRepository)(nil)
)

// CreateArtifactAndUploadSession atomically inserts the `artifacts`
// row (STAGING) AND the `artifact_uploads` row (CREATED). If either
// INSERT fails, the entire tx rolls back — no orphan STAGING row can
// leak from a half-initialized upload session.
//
// Blocco 4 step #5: per-table SQL routed to artifact_writer.go +
// upload_session_writer.go. The coordinator owns the tx lifecycle;
// the writers own their table's columns.
//
// Defensive zero-time guards: if the caller leaves CreatedAt/ExpiresAt
// at their zero values (e.g. forgotten by a handler), the method fills
// in CreatedAt=now() and ExpiresAt=now()+storage.uploadTTL (the same
// 24h default that the rest of the artifacts package uses). Zero
// timestamps would otherwise render as year 0001 and silently poison
// the schema's RFC3339 ordering.
func (r *SQLiteFinalizationRepository) CreateArtifactAndUploadSession(
	ctx context.Context,
	cmd CreateArtifactAndUploadSessionCommand,
) error {
	if cmd.ArtifactID == "" || cmd.UploadID == "" || cmd.JobID == "" {
		return fmt.Errorf("artifacts: CreateArtifactAndUploadSession: artifact_id, upload_id and job_id are required")
	}

	now := cmd.CreatedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	expiresAt := cmd.ExpiresAt
	if expiresAt.IsZero() {
		// defaultUploadTTL = 24h — matches the spec's reconciler rule
		// so upload expiry lines up with orphan-blob retention.
		expiresAt = now.Add(defaultUploadTTL)
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("artifacts: CreateArtifactAndUploadSession begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	// Per-table writes routed to the writers (joined tx).
	if err := insertArtifactStagingInTx(ctx, tx, artifactStagingRow{
		ArtifactID: cmd.ArtifactID,
		JobID:      cmd.JobID,
		AttemptID:  cmd.AttemptID,
		Kind:       cmd.Kind,
		CreatedAt:  now,
	}); err != nil {
		return err
	}
	if err := insertUploadSessionCreatedInTx(ctx, tx, cmd, now, expiresAt); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("artifacts: CreateArtifactAndUploadSession commit: %w", err)
	}
	committed = true
	return nil
}

// FinalizeVerified is the canonical atomic SUCCEEDED write. It flips
// jobs RUNNING | AWAITING_ARTIFACT → SUCCEEDED, artifacts STAGING →
// READY, inserts the per-destination job_deliveries rows, and in-tx
// flips artifact_uploads FINALIZING → COMPLETED.
//
// cleanup/remove-job-attempts-runtime: the legacy "job_attempts
// RENDER_FINISHED → SUCCEEDED" CAS step was removed. Identity is now
// fully verified at the artifact_uploads CAS chain (worker_id +
// lease_id + attempt_number) and through the new task_attempts
// read-back at service.loadAttempt. jobs.status='SUCCEEDED' remains
// the canonical audit-visible terminal write — scan_test enforces
// single-writer.
//
// NO other code path in the data server writes jobs.status='SUCCEEDED'.
// The scan test enforces this; the absence of any SUCCEEDED writer in
// the JobRepository interface also enforces it.
//
// Blocco 4 step #5: jobs CAS stays INLINE here (scan_test allowlist
// explicitly pins the `SET status = 'SUCCEEDED'` SQL fragment to
// sqlite_finalization_repository.go). The artifacts CAS and the
// artifact_uploads CAS precondition load + COMPLETED flip move to
// the per-table writers.
//
// Returns the post-tx artifact row.
func (r *SQLiteFinalizationRepository) FinalizeVerified(
	ctx context.Context,
	cmd FinalizeVerifiedCommand,
) (*store.Artifact, error) {
	if cmd.UploadID == "" || cmd.ArtifactID == "" || cmd.JobID == "" {
		return nil, fmt.Errorf("artifacts: FinalizeVerified: upload/artifact/job ids are required")
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("artifacts: FinalizeVerified begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	// 1. artifact_uploads CAS-precondition load (routed to
	//    upload_session_writer.go). Must be 'FINALIZING' state with
	//    matching worker + lease + attempt. Accepting 'RECEIVED' here
	//    would mask a missing orchestration step with a misleading
	//    late-stage ErrTransitionConflict from the step 7 flip;
	//    tightening to 'FINALIZING' only surfaces the precondition
	//    failure here with the correct ErrUploadStateInvalid sentinel.
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

	// 2. jobs CAS — KEPT INLINE (scan_test allowlist pins this fragment
	//    to this file).
	//
	// PR-01 (post-migration 048): the runtime columns assigned_to,
	// lease_id, lease_expiry, retry_count were dropped from `jobs`.
	// Identity is verified end-to-end at step 1 (artifact_uploads CAS
	// chain: status='FINALIZING' + worker_id + lease_id + attempt_number)
	// and at service.loadAttempt (which now reads from task_attempts
	// joined through tasks). The `jobs` row no longer carries
	// worker/lease identity — the CAS here is identity-free, gated only
	// on the state-machine and (optionally) the revision.
	//
	// cleanup/remove-job-attempts-runtime: removed the legacy step 4
	// job_attempts CAS write; the in-tx integrity is preserved by
	// artifact_uploads (single CAS at step 1) + task_attempts read
	// (service.loadAttempt) which together close off any
	// worker/lease/attempt mismatch window.
	//
	// PR-02: WHERE now allows status IN ('RUNNING', 'AWAITING_ARTIFACT').
	// `AWAITING_ARTIFACT` is the post-task-completion state written by
	// handleTaskResult.maybeTransitionJob when all tasks succeed; this
	// finalizer is the SINGLE legal writer that closes the loop to
	// SUCCEEDED (closed by audit §P0.2 per internal/artifacts/scan_test.go).
	// The direct RUNNING → SUCCEEDED path is preserved for legacy
	// workers without an artifact contract (defensive backward compat).
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

	// 3. artifacts CAS: STAGING → READY (routed to artifact_writer.go).
	if err := casArtifactReadyInTx(ctx, tx, artifactReadyFields{
		ArtifactID:      cmd.ArtifactID,
		JobID:           cmd.JobID,
		StorageProvider: cmd.StorageProvider,
		StorageKey:      cmd.StorageKey,
		SHA256Hex:       cmd.SHA256,
		SizeBytes:       cmd.SizeBytes,
		MIMEType:        cmd.MIMEType,
		VerifiedAtStr:   nowStr,
	}); err != nil {
		return nil, err
	}

	// 4. job_attempts CAS: removed by cleanup/remove-job-attempts-runtime.
	//    Previously this step UPDATEd job_attempts SET status='SUCCEEDED'
	//    gated on (worker_id, lease_id, attempt_number) and
	//    UPPER(status) IN ('RENDER_FINISHED','PROCESSING'). Per the
	//    cleanup, the runtime CAS chain on job_attempts is retired;
	//    per-attempt close-out is now driven by taskingestion.Ingest
	//    via TransitionTaskToTerminalAtomic on task_attempts
	//    (canonical layer). Auth is still fully gated at step 1
	//    (artifact_uploads CAS: worker_id+lease_id+attempt_number) +
	//    the task_attempts read-back in service.go::loadAttempt. The
	//    audit-visible terminal write remains the jobs.status='SUCCEEDED'
	//    flip in step 2 — scan_test enforces the single-writer contract.

	// 5. Resolve delivery destinations via plan resolver or fallback.
	//    Stays inline because the per-destination set is computed by
	//    the resolver (or the fallback all-enabled scan) inside the
	//    same tx; splitting it out would force the writer to receive
	//    a destination slice — cosmetics, no separation win.
	var destIDs []string
	if cmd.DestinationID != "" {
		destIDs = []string{cmd.DestinationID}
	} else if r.planResolver != nil {
		resolved, rerr := r.planResolver.ResolveDestinations(ctx, cmd.JobID, cmd.ArtifactID)
		if rerr != nil {
			return nil, fmt.Errorf("artifacts: FinalizeVerified plan resolver: %w", rerr)
		}
		destIDs = resolved
	} else {
		rows, qerr := tx.QueryContext(ctx,
			`SELECT destination_id FROM delivery_destinations WHERE enabled = 1`)
		if qerr == nil {
			defer rows.Close()
			for rows.Next() {
				var did string
				if err := rows.Scan(&did); err == nil && did != "" {
					destIDs = append(destIDs, did)
				}
			}
		}
	}
	for _, destID := range destIDs {
		deliveryID, err := identity.NewHex128()
		if err != nil {
			return nil, fmt.Errorf("generate delivery ID: %w", err)
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO job_deliveries (delivery_id, artifact_id, destination_id, status, idempotency_key, created_at, updated_at)
			SELECT ?, ?, ?, 'PENDING', ?, ?, ?
			WHERE NOT EXISTS (
				SELECT 1 FROM job_deliveries
				WHERE artifact_id = ? AND destination_id = ?
			)`,
			deliveryID, cmd.ArtifactID, destID,
			cmd.ArtifactID+"_"+destID, nowStr, nowStr,
			cmd.ArtifactID, destID,
		)
		if err != nil {
			return nil, fmt.Errorf("artifacts: FinalizeVerified job_deliveries insert (dest=%s): %w", destID, err)
		}
	}

	// 7. artifact_uploads CAS: FINALIZING → COMPLETED (routed to
	//    upload_session_writer.go). Closing in-tx write of the
	//    verified-finalization tx.
	if err := casUploadSessionCompletedInTx(ctx, tx, cmd.UploadID, nowStr); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("artifacts: FinalizeVerified commit: %w", err)
	}
	committed = true

	// Re-load the post-update artifact for the caller (routed to
	// artifact_writer.go — the projection SELECT lives there).
	return loadArtifactProjection(ctx, r.db, cmd.ArtifactID)
}
