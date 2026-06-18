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

	"velox-server/internal/store"
)

// SQLiteFinalizationRepository is the SQLite-backed implementation of
// FinalizationRepository. SQLite serializes writers, so concurrent
// FinalizeVerified callers on the same job_id race-free at the SQL
// layer; service-layer ENFORCES the state-machine legality (RECEIVED
// then FINALIZING then COMPLETED) before this code runs.
type SQLiteFinalizationRepository struct {
	db *sql.DB
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
			received_size_bytes, received_sha256,
			created_at, expires_at, completed_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		cmd.UploadID, cmd.ArtifactID, cmd.JobID, cmd.AttemptNumber,
		cmd.WorkerID, cmd.LeaseID,
		"CREATED", cmd.TemporaryStorageKey,
		cmd.ExpectedSizeBytes, nilOrString(cmd.ExpectedSHA256),
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
// jobs RUNNING → SUCCEEDED, artifacts STAGING → READY,
// job_attempts RENDER_FINISHED → SUCCEEDED, inserts outbox ARTIFACT_READY
// + JOB_SUCCEEDED + DELIVERY_CREATED events, inserts the per-destination
// job_deliveries rows (single primary destination today; commit 4.4
// will swap to a per-job destination plan), and in-tx flips
// artifact_uploads FINALIZING → COMPLETED.
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

	// 2. jobs CAS: RUNNING + owner + lease + revision → SUCCEEDED.
	//    Note: we no longer write jobs.output_sha256 here — that
	//    column is being retired (PR 3.5-b 4.3). The canonical SHA
	//    lives on the artifacts row.
	jobRes, err := tx.ExecContext(ctx, `
		UPDATE jobs
		SET status = 'SUCCEEDED',
		    completed_at = ?,
		    updated_at   = ?,
		    lease_id     = NULL,
		    lease_expiry = NULL,
		    revision     = revision + 1
		WHERE job_id = ?
		  AND status = 'RUNNING'
		  AND assigned_to = ?
		  AND lease_id = ?
		  AND revision = ?`,
		nowStr, nowStr,
		cmd.JobID,
		cmd.WorkerID, cmd.LeaseID, cmd.ExpectedRevision,
	)
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

	// 4. job_attempts CAS: RENDER_FINISHED + auth → SUCCEEDED.
	attRes, err := tx.ExecContext(ctx, `
		UPDATE job_attempts
		SET status = 'SUCCEEDED',
		    finished_at = ?
		WHERE job_id = ?
		  AND attempt_number = ?
		  AND worker_id = ?
		  AND lease_id = ?
		  AND status = 'RENDER_FINISHED'`,
		nowStr, cmd.JobID, cmd.AttemptNumber,
		cmd.WorkerID, cmd.LeaseID,
	)
	if err != nil {
		return nil, fmt.Errorf("artifacts: FinalizeVerified job_attempts CAS: %w", err)
	}
	if n, _ := attRes.RowsAffected(); n != 1 {
		return nil, fmt.Errorf("%w: job_attempts affected=%d upload=%s",
			ErrTransitionConflict, n, cmd.UploadID)
	}

	// 5. outbox ARTIFACT_READY + JOB_SUCCEEDED (transactional outbox).
	if err := r.emitOutboxTx(ctx, tx,
		"artifact", cmd.ArtifactID, "ARTIFACT_READY",
		fmt.Sprintf(`{"artifact_id":%q,"job_id":%q,"sha256":%q,"size_bytes":%d,"mime_type":%q,"storage_key":%q}`,
			cmd.ArtifactID, cmd.JobID, cmd.SHA256, cmd.SizeBytes, cmd.MIMEType, cmd.StorageKey),
	); err != nil {
		return nil, fmt.Errorf("artifacts: FinalizeVerified outbox ARTIFACT_READY: %w", err)
	}
	if err := r.emitOutboxTx(ctx, tx,
		"job", cmd.JobID, "JOB_SUCCEEDED",
		fmt.Sprintf(`{"job_id":%q,"artifact_id":%q,"sha256":%q}`,
			cmd.JobID, cmd.ArtifactID, cmd.SHA256),
	); err != nil {
		return nil, fmt.Errorf("artifacts: FinalizeVerified outbox JOB_SUCCEEDED: %w", err)
	}

	// 6. job_deliveries idempotent creation.
	//
	// TODO(PR-3.5-b-4.4): replace this single 'primary' INSERT with
	// iteration over a DeliveryPlanResolver-provided destination set
	// (GLOBAL destinations + per-job plans from delivery_destinations
	// + job_delivery_plans). For each destination_id, INSERT...
	// ON CONFLICT(artifact_id, destination_id) DO NOTHING and emit
	// DELIVERY_CREATED only when RowsAffected == 1. The current
	// single-INSERT is safe (UNIQUE protects against double FINALIZE)
	// but does not match the wider destination set that production
	// delivery runs require.
	delRes, err := tx.ExecContext(ctx, `
		INSERT INTO job_deliveries (artifact_id, destination_id, payload, status, created_at)
		SELECT ?, 'primary', ?, 'PENDING', ?
		WHERE NOT EXISTS (
			SELECT 1 FROM job_deliveries
			WHERE artifact_id = ? AND destination_id = 'primary'
		)`,
		cmd.ArtifactID,
		fmt.Sprintf(`{"artifact_id":%q,"storage_key":%q}`, cmd.ArtifactID, cmd.StorageKey),
		nowStr, cmd.ArtifactID,
	)
	if err != nil {
		return nil, fmt.Errorf("artifacts: FinalizeVerified job_deliveries insert: %w", err)
	}
	if delRes != nil {
		if n, _ := delRes.RowsAffected(); n == 1 {
			if err := r.emitOutboxTx(ctx, tx,
				"delivery", cmd.ArtifactID+":primary", "DELIVERY_CREATED",
				fmt.Sprintf(`{"artifact_id":%q,"destination_id":"primary"}`,
					cmd.ArtifactID),
			); err != nil {
				return nil, fmt.Errorf("artifacts: FinalizeVerified outbox DELIVERY_CREATED: %w", err)
			}
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

// emitOutboxTx inserts one outbox event row in the same tx as the
// caller. If the schema is missing outbox_events the tx DOES fail
// (commit 3.5-b 4.5 removes the previous soft-skip). The previous
// `if isNoSuchTable(err) { return nil }` pattern would silently drop
// delivery events on partial-migration schemas and was a defect.
func (r *SQLiteFinalizationRepository) emitOutboxTx(ctx context.Context, tx *sql.Tx, aggType, aggID, eventType, payload string) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload, status, created_at)
		VALUES (?, ?, ?, ?, 'PENDING', ?)`,
		aggType, aggID, eventType, payload,
		time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		return fmt.Errorf("artifacts: emitOutboxTx %s/%s: %w", aggID, eventType, err)
	}
	return nil
}
