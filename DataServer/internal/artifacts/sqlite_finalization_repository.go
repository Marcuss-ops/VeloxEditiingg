// sql-allowlist: artifacts SQLiteFinalizationRepository — sole atomic-tx writer for jobs.status=SUCCEEDED (single-writer enforced by scan_test.go). Future refactor candidate for relocation into internal/store alongside the other typed repos.

// Package artifacts / sqlite_finalization_repository.go — PR 3.5-a impl.
//
// This is the ONLY legal writer of jobs.status=<terminal-state>.
// The scan test (scan_test.go) greps every .go under internal/ and
// rejects any single-quoted SQL writer of that terminal state outside
// the audited allowlist (see scan_test.go for the precise regex).
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

// SQLiteFinalizationRepository is the SQLite-backed implementation of
// FinalizationRepository. SQLite serializes writers, so concurrent
// FinalizeVerified callers on the same job_id race-free at the SQL
// layer; service-layer ENFORCES the state-machine legality (RECEIVED
// then FINALIZING then COMPLETED) before this code runs.
//
// Migration note: file-1/4 of the canonical-SQL-gateway migration
// moved `artifact_uploads` CRUD to store/artifact_uploads.go. This
// file remains in artifacts/ because the PR 3.5-a single-writer
// contract for jobs.status='SUCCEEDED' (audit §P0.2, enforced by
// scan_test.go) requires the *atomic* finalization tx to live
// adjacent to the Service that orchestrates it, AND the
// CreateArtifactAndUploadSession path inserts into
// artifact_uploads directly. The 4 helpers at the bottom of this
// file (nilOrString / nilOrStringPtr / formatTimePtr /
// parseTimeRFC3339) are an inline duplicate of the same-named
// private helpers in store/artifact_uploads.go — they will be
// retired together when file-3/4 migrates this repository to store.
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
// Replaces the previous two-step BeginUpload pattern (artifacts INSERT
// then repo.CreateUploadSession) where a failure between the two left
// STAGING rows orphaned.
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

	// Resolve storage provider (callers can opt in to e.g. "s3"; default "local").
	storageProvider := cmd.StorageProvider
	if storageProvider == "" {
		storageProvider = "local"
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

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO artifacts (id, job_id, attempt_id, type,
		                       storage_provider, status, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		cmd.ArtifactID, cmd.JobID, cmd.AttemptID, cmd.Kind,
		storageProvider, "STAGING", now.UTC().Format(time.RFC3339),
	); err != nil {
		return fmt.Errorf("artifacts: CreateArtifactAndUploadSession INSERT artifacts: %w", err)
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
		return fmt.Errorf("artifacts: CreateArtifactAndUploadSession INSERT artifact_uploads: %w", err)
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

	// 1. artifact_uploads must be in 'FINALIZING' state. The
	//    RECEIVED -> FINALIZING CAS happens in Service.Finalize BEFORE
	//    calling FinalizeVerified (see the orchestration contract in
	//    service.go::Finalize). Accepting 'RECEIVED' here would mask
	//    a missing orchestration step with a misleading late-stage
	//    ErrTransitionConflict from the step 7 flip; tightening to
	//    'FINALIZING' only surfaces the precondition failure here
	//    with the correct ErrUploadStateInvalid sentinel.
	var uploadStatus, uploadWorker, uploadLease string
	var uploadAttempt int
	if err := tx.QueryRowContext(ctx, `
		SELECT status, worker_id, lease_id, attempt_number
		FROM artifact_uploads WHERE upload_id = ?`, cmd.UploadID,
	).Scan(&uploadStatus, &uploadWorker, &uploadLease, &uploadAttempt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: upload_id=%s", ErrUploadNotFound, cmd.UploadID)
		}
		return nil, fmt.Errorf("artifacts: FinalizeVerified load upload: %w", err)
	}
	if uploadStatus != "FINALIZING" {
		return nil, fmt.Errorf("%w: upload=%s status=%s (expected FINALIZING — Service.Finalize must CAS RECEIVED->FINALIZING first)",
			ErrUploadStateInvalid, cmd.UploadID, uploadStatus)
	}
	if uploadWorker != cmd.WorkerID || uploadLease != cmd.LeaseID || uploadAttempt != cmd.AttemptNumber {
		return nil, fmt.Errorf("%w: auth upload=%s worker=%s->%s lease=%s->%s attempt=%d->%d",
			ErrTransitionConflict, cmd.UploadID,
			uploadWorker, cmd.WorkerID, uploadLease, cmd.LeaseID,
			uploadAttempt, cmd.AttemptNumber)
	}

	verifiedAt := cmd.VerifiedAt
	if verifiedAt.IsZero() {
		verifiedAt = time.Now().UTC()
	}
	nowStr := verifiedAt.UTC().Format(time.RFC3339)

	// 2. jobs CAS: RUNNING|AWAITING_ARTIFACT [+ revision if provided]
	// → SUCCEEDED.
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
	// Releasing the dropped columns in the SET clause was also dropped:
	// they no longer exist on the table — fixing the post-048 runtime
	// error "no such column: lease_id".
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

	// 3. artifacts CAS: STAGING → READY, master-stamp metadata.
	artRes, err := tx.ExecContext(ctx, `
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
	if n, _ := artRes.RowsAffected(); n != 1 {
		return nil, fmt.Errorf("%w: artifacts affected=%d upload=%s artifact=%s",
			ErrTransitionConflict, n, cmd.UploadID, cmd.ArtifactID)
	}

	// 4. job_attempts CAS: removed by cleanup/remove-job-attempts-runtime.
	//    Previously this step UPDATEd job_attempts SET status='SUCCEEDED'
	//    gated on (worker_id, lease_id, attempt_number) and
	//    UPPER(status) IN ('RENDER_FINISHED','PROCESSING'). Per the
	//    cleanup, the runtime CAS chain on job_attempts is retired;
	//    per-attempt close-out is now driven by taskingestion.Ingest
	//    via TransitionTaskToTerminalAtomic on task_attempts
	//    (canonical layer). Auth is still fully gated at step 1
	//    (artifact_uploads CAS: worker_id+lease_id+attempt_number) + the
	//    task_attempts read-back in service.go::loadAttempt. The
	//    audit-visible terminal write remains the jobs.status='SUCCEEDED'
	//    flip in step 2 — scan_test enforces the single-writer contract.

	// 5. Resolve delivery destinations via plan resolver or fallback.
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

	// 7. In-tx flip FINALIZING → COMPLETED — atomic with steps 1-6.
	upRes, err := tx.ExecContext(ctx, `
		UPDATE artifact_uploads
		SET status = 'COMPLETED',
		    completed_at = ?
		WHERE upload_id = ?
		  AND status = 'FINALIZING'`,
		nowStr, cmd.UploadID)
	if err != nil {
		return nil, fmt.Errorf("artifacts: FinalizeVerified upload COMPLETED flip: %w", err)
	}
	if n, _ := upRes.RowsAffected(); n != 1 {
		return nil, fmt.Errorf("%w: upload affected=%d upload=%s",
			ErrTransitionConflict, n, cmd.UploadID)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("artifacts: FinalizeVerified commit: %w", err)
	}
	committed = true

	// Re-load the post-update artifact for the caller.
	row := r.db.QueryRowContext(ctx, `
		SELECT id, job_id, COALESCE(attempt_id, 0), type, storage_provider,
		       COALESCE(storage_key, ''), COALESCE(storage_url, ''),
		       COALESCE(local_path, ''), COALESCE(sha256, ''),
		       COALESCE(size_bytes, 0), COALESCE(duration_seconds, 0),
		       status, COALESCE(verified_at, ''), created_at
		FROM artifacts WHERE id = ?`, cmd.ArtifactID)
	var out store.Artifact
	var verifiedAtStr string
	if scanErr := row.Scan(&out.ID, &out.JobID, &out.AttemptID, &out.Type, &out.StorageProvider,
		&out.StorageKey, &out.StorageURL, &out.LocalPath, &out.SHA256,
		&out.SizeBytes, &out.DurationSeconds, &out.Status, &verifiedAtStr, &out.CreatedAt); scanErr != nil {
		return nil, fmt.Errorf("artifacts: FinalizeVerified post-load: %w", scanErr)
	}
	return &out, nil
}

// ── package-local helpers (file-1/4 migration duplicate) ────────────────
//
// nilOrString / formatTimePtr mirror the unexported helpers in
// store/artifact_uploads.go. They are kept private to this file so
// the CreateArtifactAndUploadSession INSERTs that build the
// artifacts + artifact_uploads rows still compile after
// artifacts/uploads.go was deleted (file-1/4 of the
// canonical-SQL-gateway migration). They will dissolve together
// with this file when file-3/4 migrates SQLiteFinalizationRepository
// to store.

// nilOrString maps "" -> nil so the column stores NULL rather than "",
// matching the migration's nullable TEXT columns for expected_sha256.
func nilOrString(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
