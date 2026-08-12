package store

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"velox-server/internal/jobs"

	"velox-server/internal/deliverycontract"
	"velox-server/internal/identity"
)

// FinalizeVerifiedParams is the store-owned persistence projection for the
// verified artifact finalization transaction. Keeping it in store prevents
// the SQL gateway from importing the artifacts orchestration package.
type FinalizeVerifiedParams struct {
	UploadID             string
	ArtifactID           string
	JobID                string
	AttemptID            string
	WorkerID             string
	LeaseID              string
	AttemptNumber        int
	ExpectedRevision     int
	StorageProvider      string
	StorageKey           string
	SHA256               string
	SizeBytes            int64
	MIMEType             string
	VerifiedAt           time.Time
	DestinationID        string
	AsyncProbe           bool
	ExpectedAudioStreams int
}

// SQLiteArtifactFinalizer is the sole store-owned writer for the verified
// artifact finalization transaction. It owns the transaction boundary and
// exposes no *sql.Tx to application packages.
type SQLiteArtifactFinalizer struct {
	db       *sql.DB
	resolver deliverycontract.DeliveryPlanResolver
}

func NewSQLiteArtifactFinalizer(db *sql.DB, resolver deliverycontract.DeliveryPlanResolver) *SQLiteArtifactFinalizer {
	if db == nil {
		panic("store: NewSQLiteArtifactFinalizer requires a non-nil database")
	}
	return &SQLiteArtifactFinalizer{db: db, resolver: resolver}
}

// NewSQLiteArtifactFinalizerFromStore binds the finalizer to the canonical
// SQLiteStore. The transaction remains entirely store-owned while the
// application composition root no longer passes a raw database handle into
// an artifact adapter.
func NewSQLiteArtifactFinalizerFromStore(s *SQLiteStore, resolver deliverycontract.DeliveryPlanResolver) *SQLiteArtifactFinalizer {
	if s == nil || s.db == nil {
		panic("store: NewSQLiteArtifactFinalizerFromStore requires a non-nil SQLiteStore")
	}
	return &SQLiteArtifactFinalizer{db: s.db, resolver: resolver}
}

func isCanonicalSHA256(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func (w *SQLiteArtifactFinalizer) FinalizeVerified(ctx context.Context, p FinalizeVerifiedParams) (*Artifact, error) {
	if p.UploadID == "" || p.ArtifactID == "" || p.JobID == "" {
		return nil, fmt.Errorf("store: FinalizeVerified: upload/artifact/job ids are required")
	}
	// The final job transition is only legal after the verifier has produced
	// durable content-addressed metadata. Empty keys or hashes must fail
	// closed; a URL or worker declaration is not proof of a READY artifact.
	if p.StorageKey == "" || !isCanonicalSHA256(p.SHA256) {
		return nil, fmt.Errorf("store: FinalizeVerified: verified storage key and canonical sha256 are required")
	}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("store: FinalizeVerified begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	var status, workerID, leaseID, receivedSHA, expectedSHA string
	var receivedSize, expectedSize sql.NullInt64
	var attemptNumber int
	if err := tx.QueryRowContext(ctx, `
		SELECT status, worker_id, lease_id, COALESCE(received_sha256,''),
		       COALESCE(expected_sha256,''), received_size_bytes, expected_size_bytes,
		       attempt_number
		FROM artifact_uploads WHERE upload_id = ?`, p.UploadID).
		Scan(&status, &workerID, &leaseID, &receivedSHA, &expectedSHA, &receivedSize, &expectedSize, &attemptNumber); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: upload_id=%s", ErrUploadNotFound, p.UploadID)
		}
		return nil, fmt.Errorf("store: FinalizeVerified upload precondition: %w", err)
	}
	if status != "FINALIZING" {
		return nil, fmt.Errorf("%w: upload=%s status=%s (expected FINALIZING)", ErrUploadStateInvalid, p.UploadID, status)
	}
	if workerID != p.WorkerID || leaseID != p.LeaseID || attemptNumber != p.AttemptNumber {
		return nil, fmt.Errorf("%w: upload=%s auth mismatch", ErrTransitionConflict, p.UploadID)
	}
	if !isCanonicalSHA256(receivedSHA) || receivedSHA != p.SHA256 ||
		!isCanonicalSHA256(expectedSHA) || expectedSHA != receivedSHA ||
		!receivedSize.Valid || receivedSize.Int64 != p.SizeBytes ||
		!expectedSize.Valid || expectedSize.Int64 != receivedSize.Int64 {
		return nil, fmt.Errorf("%w: upload=%s expected/master-computed hash/size does not match verified metadata", ErrTransitionConflict, p.UploadID)
	}

	now := p.VerifiedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	nowStr := now.UTC().Format(time.RFC3339)
	var res sql.Result

	attemptID := strings.TrimSpace(p.AttemptID)
	if attemptID != "" {
		res, err = tx.ExecContext(ctx, `
			UPDATE tasks
			SET status = ?, completed_at = ?, updated_at = ?,
			    winning_attempt_id = ?, winning_attempt_committed_at = ?,
			    winning_attempt_terminal_pending = 0, revision = revision + 1
			WHERE job_id = ? AND attempt_id = ?
			  AND worker_id = ? AND lease_id = ?
			  AND status IN ('RUNNING', 'LEASED', 'PENDING')`,
			"SUCCEEDED", nowStr, nowStr, attemptID, nowStr,
			p.JobID, attemptID, p.WorkerID, p.LeaseID)
		if err != nil {
			return nil, fmt.Errorf("store: FinalizeVerified task winner CAS: %w", err)
		}
		n, rowsErr := readRowsAffected(res, "FinalizeVerified task winner CAS")
		if rowsErr != nil {
			return nil, rowsErr
		}
		if n != 1 {
			return nil, fmt.Errorf("%w: task winner affected=%d attempt=%s", ErrTransitionConflict, n, attemptID)
		}
		// Determinism chain (migration 148): stamp the master-computed
		// authoritative artifact SHA on the winning attempt. The worker
		// report may later declare a SHA hint at ingest; the ingest-side
		// gap-fill guard (persistAttemptRenderIdentity) never overwrites
		// this authoritative value.
		res, err = tx.ExecContext(ctx, `
			UPDATE task_attempts
			SET status = ?, completed_at = COALESCE(completed_at, ?), updated_at = ?,
			    artifact_sha256 = ?,
			    -- Determinism chain comparison (migration 149): preserve the
			    -- worker-declared hint (report gap-fill may have stamped it
			    -- first) and flag a discrepancy against the master-computed
			    -- authoritative value (potential ARTIFACT_TRANSFER_CORRUPTED).
			    worker_sha256 = CASE WHEN worker_sha256 = '' THEN artifact_sha256 ELSE worker_sha256 END,
			    artifact_sha256_mismatch = CASE
				    WHEN artifact_sha256 != '' AND artifact_sha256 != ? THEN 1
				    ELSE artifact_sha256_mismatch END
			WHERE id = ? AND task_id = (SELECT task_id FROM tasks WHERE job_id = ? AND attempt_id = ?)
			  AND worker_id = ? AND lease_id = ?
			  AND (status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT')
			       OR (status = 'SUCCEEDED' AND (artifact_sha256 = '' OR artifact_sha256 = ?)))`,
			"SUCCEEDED", nowStr, nowStr, p.SHA256, p.SHA256, attemptID, p.JobID, attemptID, p.WorkerID, p.LeaseID, p.SHA256)
		if err != nil {
			return nil, fmt.Errorf("store: FinalizeVerified attempt winner CAS: %w", err)
		}
		n, rowsErr = readRowsAffected(res, "FinalizeVerified attempt winner CAS")
		if rowsErr != nil {
			return nil, rowsErr
		}
		if n != 1 {
			return nil, fmt.Errorf("%w: attempt winner affected=%d attempt=%s", ErrTransitionConflict, n, attemptID)
		}
	} else {
		// Legacy finalize messages omit AttemptID. Keep compatibility with
		// pre-typed callers, but fence the fallback by the authenticated
		// worker/lease tuple and require exactly one task row. On the
		// canonical schema the tuple lives on task_attempts; older/minimal
		// fixtures keep it directly on tasks. The old job-only sweep could
		// let a stale lease terminalize the wrong task.
		res, err = tx.ExecContext(ctx, `
			UPDATE tasks SET status = ?, completed_at = ?, updated_at = ?
			WHERE job_id = ? AND worker_id = ? AND lease_id = ?
			  AND status IN ('RUNNING', 'LEASED', 'PENDING')`,
			"SUCCEEDED", nowStr, nowStr, p.JobID, p.WorkerID, p.LeaseID)
		if err != nil {
			return nil, fmt.Errorf("store: FinalizeVerified legacy task fence: %w", err)
		}
		n, rowsErr := readRowsAffected(res, "FinalizeVerified legacy task fence")
		if rowsErr != nil {
			return nil, rowsErr
		}
		var hasAttemptsTable int
		if n == 0 {
			if err := tx.QueryRowContext(ctx, `
				SELECT EXISTS(
					SELECT 1 FROM sqlite_master
					WHERE type = 'table' AND name = 'task_attempts'
				)`).Scan(&hasAttemptsTable); err != nil {
				return nil, fmt.Errorf("store: FinalizeVerified legacy attempt schema lookup: %w", err)
			}
			if hasAttemptsTable == 1 {
				// Since migration 048 the task row is deliberately not the
				// ownership authority. Resolve the legacy wire message through
				// the canonical attempt identity instead of weakening the fence.
				res, err = tx.ExecContext(ctx, `
					UPDATE tasks SET status = ?, completed_at = ?, updated_at = ?
					WHERE tasks.job_id = ?
					  AND tasks.status IN ('RUNNING', 'LEASED', 'PENDING')
					  AND EXISTS (
						  SELECT 1 FROM task_attempts
						  WHERE task_attempts.task_id = tasks.task_id
							AND task_attempts.attempt_number = ?
							AND task_attempts.worker_id = ?
							AND task_attempts.lease_id = ?
					  )`,
					"SUCCEEDED", nowStr, nowStr, p.JobID, p.AttemptNumber, p.WorkerID, p.LeaseID)
				if err != nil {
					return nil, fmt.Errorf("store: FinalizeVerified legacy attempt fence: %w", err)
				}
				n, rowsErr = readRowsAffected(res, "FinalizeVerified legacy attempt fence")
				if rowsErr != nil {
					return nil, rowsErr
				}
			}
		}
		if n != 1 {
			// Render-only/legacy fixtures may have no task row at all. An
			// already terminal task is also an idempotent replay, but only
			// when its persisted owner or canonical attempt still matches
			// this upload lease.
			var taskCount int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM tasks WHERE job_id = ?`, p.JobID).Scan(&taskCount); err != nil {
				return nil, fmt.Errorf("store: FinalizeVerified legacy task fence lookup: %w", err)
			}
			if taskCount > 0 {
				var currentStatus, currentWorker, currentLease string
				var hasMatchingAttempt int
				if err := tx.QueryRowContext(ctx, `SELECT status, COALESCE(worker_id, ''), COALESCE(lease_id, '') FROM tasks WHERE job_id = ?`, p.JobID).Scan(&currentStatus, &currentWorker, &currentLease); err != nil {
					return nil, fmt.Errorf("store: FinalizeVerified legacy task fence state: %w", err)
				}
				ownerMatches := currentWorker == p.WorkerID && currentLease == p.LeaseID
				if !ownerMatches {
					if err := tx.QueryRowContext(ctx, `
						SELECT EXISTS(
							SELECT 1 FROM sqlite_master
							WHERE type = 'table' AND name = 'task_attempts'
						)`).Scan(&hasAttemptsTable); err != nil {
						return nil, fmt.Errorf("store: FinalizeVerified legacy idempotency schema lookup: %w", err)
					}
					if hasAttemptsTable == 1 {
						if err := tx.QueryRowContext(ctx, `
							SELECT EXISTS(
								SELECT 1 FROM task_attempts
								WHERE task_id = (SELECT task_id FROM tasks WHERE job_id = ?)
								  AND attempt_number = ?
								  AND worker_id = ?
								  AND lease_id = ?
							)`, p.JobID, p.AttemptNumber, p.WorkerID, p.LeaseID).Scan(&hasMatchingAttempt); err != nil {
							return nil, fmt.Errorf("store: FinalizeVerified legacy attempt idempotency lookup: %w", err)
						}
						ownerMatches = hasMatchingAttempt == 1
					}
				}
				if currentStatus != "SUCCEEDED" || !ownerMatches {
					return nil, fmt.Errorf("%w: legacy task fence affected=%d upload=%s", ErrTransitionConflict, n, p.UploadID)
				}
			}
		}
	}

	if p.AsyncProbe {
		// The rendered task is complete, but the artifact is not publishable
		// until the dedicated media probe approves it. Keep the parent job in
		// AWAITING_ARTIFACT and do not create job_deliveries yet.
		res, err = tx.ExecContext(ctx, `
			UPDATE artifacts SET status = 'VERIFYING', storage_provider = ?, storage_key = ?,
			    sha256 = ?, size_bytes = ?, mime_type = ?, verified_at = ?
			WHERE id = ? AND job_id = ? AND status = 'STAGING'`,
			p.StorageProvider, p.StorageKey, p.SHA256, p.SizeBytes, p.MIMEType,
			nowStr, p.ArtifactID, p.JobID)
		if err != nil {
			return nil, fmt.Errorf("store: FinalizeVerified async artifacts CAS: %w", err)
		}
		n, rowsErr := readRowsAffected(res, "FinalizeVerified async artifacts CAS")
		if rowsErr != nil {
			return nil, rowsErr
		}
		if n != 1 {
			return nil, fmt.Errorf("%w: async artifacts affected=%d artifact=%s", ErrTransitionConflict, n, p.ArtifactID)
		}
		res, err = tx.ExecContext(ctx, `
			UPDATE artifact_uploads SET status = 'COMPLETED', completed_at = ?
			WHERE upload_id = ? AND status = 'FINALIZING'`, nowStr, p.UploadID)
		if err != nil {
			return nil, fmt.Errorf("store: FinalizeVerified async upload CAS: %w", err)
		}
		n, rowsErr = readRowsAffected(res, "FinalizeVerified async upload CAS")
		if rowsErr != nil {
			return nil, rowsErr
		}
		if n != 1 {
			return nil, fmt.Errorf("%w: async upload affected=%d upload=%s", ErrTransitionConflict, n, p.UploadID)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO media_probe_jobs			(artifact_id, sha256, storage_key, expected_audio_streams, destination_id, status,
			 attempt_count, max_attempts, next_attempt_at, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, 'PENDING', 0, 5, ?, ?, ?)

			ON CONFLICT(artifact_id, sha256) DO NOTHING`,
			p.ArtifactID, p.SHA256, p.StorageKey, p.ExpectedAudioStreams, p.DestinationID,
			nowStr, nowStr, nowStr); err != nil {
			return nil, fmt.Errorf("store: FinalizeVerified async probe enqueue: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("store: FinalizeVerified async commit: %w", err)
		}
		committed = true
		return readArtifact(ctx, w.db, p.ArtifactID)
	}

	// Resolve the explicit per-job delivery plan before deciding the job
	// terminal state. An absent plan is valid only for a job whose persisted
	// control-plane contract explicitly says render_only=true. Normal jobs
	// fail closed; they must never be routed to a global destination.
	var destinations []deliverycontract.DeliveryDestination
	if p.DestinationID != "" {
		destinations = []deliverycontract.DeliveryDestination{{DestinationID: p.DestinationID, MaxAttempts: 5}}
	} else if w.resolver != nil {
		resolved, resolveErr := w.resolver.ResolveDestinations(ctx, p.JobID, p.ArtifactID)
		if resolveErr != nil && !errors.Is(resolveErr, deliverycontract.ErrNoExplicitPlan) {
			return nil, fmt.Errorf("store: FinalizeVerified plan resolver: %w", resolveErr)
		}
		if resolveErr == nil {
			destinations = resolved
		}
	}

	if len(destinations) == 0 && p.DestinationID == "" {
		var requestJSON string
		if err := tx.QueryRowContext(ctx,
			`SELECT COALESCE(request_json, '{}') FROM jobs WHERE job_id = ?`, p.JobID).
			Scan(&requestJSON); err != nil {
			return nil, fmt.Errorf("store: FinalizeVerified render contract: %w", err)
		}
		var contract map[string]interface{}
		if err := json.Unmarshal([]byte(requestJSON), &contract); err != nil {
			return nil, fmt.Errorf("store: FinalizeVerified render contract: invalid request_json: %w", err)
		}
		renderOnly, _ := contract["render_only"].(bool)
		if !renderOnly {
			return nil, fmt.Errorf("%w: job_id=%s (set render_only=true or provide an explicit delivery plan)", deliverycontract.ErrNoExplicitPlan, p.JobID)
		}
	}

	if len(destinations) > 0 {
		jobQuery := `
			UPDATE jobs SET status = 'DELIVERING', completed_at = completed_at,
			    updated_at = ?, revision = revision + 1
			WHERE job_id = ? AND status = 'AWAITING_ARTIFACT'`
		args := []interface{}{nowStr, p.JobID}
		if p.ExpectedRevision != 0 {
			jobQuery += ` AND revision = ?`
			args = append(args, p.ExpectedRevision)
		}
		res, err = tx.ExecContext(ctx, jobQuery, args...)
		if err != nil {
			return nil, fmt.Errorf("store: FinalizeVerified jobs CAS: %w", err)
		}
		n, rowsErr := readRowsAffected(res, "FinalizeVerified jobs CAS")
		if rowsErr != nil {
			return nil, rowsErr
		}
		if n != 1 {
			var current string
			if scanErr := tx.QueryRowContext(ctx, `SELECT status FROM jobs WHERE job_id = ?`, p.JobID).Scan(&current); scanErr != nil || current != string(jobs.StatusDelivering) {
				return nil, fmt.Errorf("%w: jobs affected=%d upload=%s", ErrTransitionConflict, n, p.UploadID)
			}
		}
	} else {
		jobQuery := `
			UPDATE jobs SET status = 'SUCCEEDED', completed_at = ?, updated_at = ?, revision = revision + 1
			WHERE job_id = ? AND status = 'AWAITING_ARTIFACT'`
		args := []interface{}{nowStr, nowStr, p.JobID}
		if p.ExpectedRevision != 0 {
			jobQuery += ` AND revision = ?`
			args = append(args, p.ExpectedRevision)
		}
		res, err = tx.ExecContext(ctx, jobQuery, args...)
		if err != nil {
			return nil, fmt.Errorf("store: FinalizeVerified jobs CAS: %w", err)
		}
		n, rowsErr := readRowsAffected(res, "FinalizeVerified jobs CAS")
		if rowsErr != nil {
			return nil, rowsErr
		}
		if n != 1 {
			return nil, fmt.Errorf("%w: jobs affected=%d upload=%s", ErrTransitionConflict, n, p.UploadID)
		}
	}

	res, err = tx.ExecContext(ctx, `
		UPDATE artifacts SET status = 'READY', storage_provider = ?, storage_key = ?,
		    sha256 = ?, size_bytes = ?, mime_type = ?, verified_at = ?
		WHERE id = ? AND job_id = ? AND status = 'STAGING'`,
		p.StorageProvider, p.StorageKey, p.SHA256, p.SizeBytes, p.MIMEType,
		nowStr, p.ArtifactID, p.JobID)
	if err != nil {
		return nil, fmt.Errorf("store: FinalizeVerified artifacts CAS: %w", err)
	}
	n, rowsErr := readRowsAffected(res, "FinalizeVerified artifacts CAS")
	if rowsErr != nil {
		return nil, rowsErr
	}
	if n != 1 {
		return nil, fmt.Errorf("%w: artifacts affected=%d artifact=%s", ErrTransitionConflict, n, p.ArtifactID)
	}

	for _, dest := range destinations {
		maxAttempts := dest.MaxAttempts
		if maxAttempts < 0 {
			maxAttempts = 5
		}
		if err := insertPendingDelivery(ctx, tx, p.ArtifactID, dest.DestinationID, maxAttempts, nowStr); err != nil {
			return nil, err
		}
	}

	res, err = tx.ExecContext(ctx, `
		UPDATE artifact_uploads SET status = 'COMPLETED', completed_at = ?
		WHERE upload_id = ? AND status = 'FINALIZING'`, nowStr, p.UploadID)
	if err != nil {
		return nil, fmt.Errorf("store: FinalizeVerified artifact_uploads CAS: %w", err)
	}
	n, rowsErr = readRowsAffected(res, "FinalizeVerified artifact_uploads CAS")
	if rowsErr != nil {
		return nil, rowsErr
	}
	if n != 1 {
		return nil, fmt.Errorf("%w: upload affected=%d upload=%s", ErrTransitionConflict, n, p.UploadID)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("store: FinalizeVerified commit: %w", err)
	}
	committed = true

	return readArtifact(ctx, w.db, p.ArtifactID)
}

func insertPendingDelivery(ctx context.Context, tx *sql.Tx, artifactID, destinationID string, maxAttempts int, now string) error {
	deliveryID, err := identity.NewHex128()
	if err != nil {
		return fmt.Errorf("store: generate delivery ID: %w", err)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO job_deliveries (delivery_id, artifact_id, destination_id, status, max_attempts, idempotency_key, created_at, updated_at)
		VALUES (?, ?, ?, 'PENDING', ?, ?, ?, ?)
		ON CONFLICT(artifact_id, destination_id) DO NOTHING`,
		deliveryID, artifactID, destinationID, maxAttempts, artifactID+"_"+destinationID, now, now)
	if err != nil {
		return fmt.Errorf("store: FinalizeVerified job_deliveries insert (dest=%s): %w", destinationID, err)
	}
	return nil
}

func readArtifact(ctx context.Context, db *sql.DB, artifactID string) (*Artifact, error) {
	row := db.QueryRowContext(ctx, `
		SELECT id, job_id, COALESCE(attempt_id,0), type, storage_provider,
		       COALESCE(storage_key,''), COALESCE(storage_url,''), COALESCE(local_path,''),
		       COALESCE(sha256,''), COALESCE(size_bytes,0), COALESCE(duration_seconds,0),
		       COALESCE(duration_ms,0), COALESCE(mime_type,''), COALESCE(verified_at,''),
		       status, created_at FROM artifacts WHERE id = ?`, artifactID)
	var a Artifact
	if err := row.Scan(&a.ID, &a.JobID, &a.AttemptID, &a.Type, &a.StorageProvider,
		&a.StorageKey, &a.StorageURL, &a.LocalPath, &a.SHA256, &a.SizeBytes,
		&a.DurationSeconds, &a.DurationMs, &a.MimeType, &a.VerifiedAt,
		&a.Status, &a.CreatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrArtifactNotFound
		}
		return nil, fmt.Errorf("store: FinalizeVerified post-tx read: %w", err)
	}
	return &a, nil
}

var _ interface {
	FinalizeVerified(context.Context, FinalizeVerifiedParams) (*Artifact, error)
} = (*SQLiteArtifactFinalizer)(nil)
