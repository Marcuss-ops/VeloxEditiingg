// Package completion / sqlite_uow.go
//
// SQLite-backed Unit of Work adapter. Implements the six repository
// interfaces declared in unitofwork.go against the canonical
// migrations 010/030/039/041/045/061/062/014/022/013 schemas.
//
// sql-allowlist: completion UnitOfWork adapter — owns the six typed
// repositories (attempt_commits, task_attempts, tasks, jobs, deliveries,
// outbox); sole SQL gateway per the UnitOfWork pattern documented in
// docs/architecture/unit-of-work.md. The Coordinator speaks only typed
// Go parameters; no SQL leaks beyond this file's package boundary.
//
// Every method receives the underlying *sql.Tx at construction time
// (via sqliteUnitOfWork.WithTx) and holds it as private state. No
// SQL is exposed beyond this file's package boundary; the
// coordinator speaks only typed Go parameters.
//
// Tx lifecycle stays at the coordinator layer (open, commit, defer
// rollback). This file does not start or commit transactions.
// loadCommitResult is invoked by the coordinator BEFORE Commit() so
// the snapshot is part of the same LevelSerializable write lock.
package completion

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Compile-time assertion: sqliteUnitOfWork satisfies UnitOfWork.
var _ UnitOfWork = (*sqliteUnitOfWork)(nil)

// sqliteUnitOfWorkFactory produces UnitOfWork bundles bound to a tx.
type sqliteUnitOfWorkFactory struct {
	db *sql.DB
}

// NewSQLiteUnitOfWorkFactory constructs the canonical SQLite-backed
// UnitOfWorkFactory. Exposed for callers that want to opt out of the
// implicit factory creation in NewCoordinator (mostly tests).
func NewSQLiteUnitOfWorkFactory(db *sql.DB) UnitOfWorkFactory {
	return &sqliteUnitOfWorkFactory{db: db}
}

// WithTx returns a UnitOfWork bound to the supplied tx.
func (f *sqliteUnitOfWorkFactory) WithTx(tx *sql.Tx) UnitOfWork {
	return &sqliteUnitOfWork{tx: tx, db: f.db}
}

// sqliteUnitOfWork holds the tx shared by all six repos.
type sqliteUnitOfWork struct {
	tx *sql.Tx
	db *sql.DB
}

func (u *sqliteUnitOfWork) AttemptCommits() AttemptCommitRepository {
	return &sqliteAttemptCommitRepo{u: u}
}
func (u *sqliteUnitOfWork) TaskAttempts() TaskAttemptRepository {
	return &sqliteTaskAttemptRepo{u: u}
}
func (u *sqliteUnitOfWork) Tasks() TaskRepository {
	return &sqliteTaskRepo{u: u}
}
func (u *sqliteUnitOfWork) Jobs() JobFinalizationRepository {
	return &sqliteJobRepo{u: u}
}
func (u *sqliteUnitOfWork) Deliveries() DeliveryRepository {
	return &sqliteDeliveryRepo{u: u}
}
func (u *sqliteUnitOfWork) Outbox() OutboxRepository {
	return &sqliteOutboxRepo{u: u}
}

// ────────────────────────────────────────────────────────────────────────
// AttemptCommitRepository
// ────────────────────────────────────────────────────────────────────────

type sqliteAttemptCommitRepo struct {
	u *sqliteUnitOfWork
}

// Find implements AttemptCommitRepository.Find using the canonical
// attempt_commits SELECT projection. Returns (nil,
// ErrAttemptCommitNotFound) on a missing row.
func (r *sqliteAttemptCommitRepo) Find(ctx context.Context, commitID string) (*AttemptCommitRow, error) {
	if commitID == "" {
		return nil, fmt.Errorf("completion.AttemptCommitRepository.Find: commitID empty")
	}
	var row AttemptCommitRow
	err := r.u.tx.QueryRowContext(ctx,
		`SELECT commit_id, task_id, attempt_id, job_id, worker_id, lease_id,
		        status, required_output_count, ready_output_count,
		        COALESCE(commit_deadline_at, '')
		   FROM attempt_commits
		  WHERE commit_id = ?`,
		commitID,
	).Scan(
		&row.CommitID, &row.TaskID, &row.AttemptID, &row.JobID,
		&row.WorkerID, &row.LeaseID, &row.Status, &row.RequiredOutputCnt,
		&row.ReadyOutputCnt, &row.CommitDeadlineAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: commit_id=%s", ErrAttemptCommitNotFound, commitID)
		}
		return nil, fmt.Errorf("completion.AttemptCommitRepository.Find: %w", err)
	}
	return &row, nil
}

// UpdateProgress bumps last_progress_at + commit_deadline_at on the
// canonical commit_id row CAS-gated on status IN ('DECLARED','UPLOADING').
func (r *sqliteAttemptCommitRepo) UpdateProgress(ctx context.Context, commitID, nowStr, deadlineStr string) (int64, error) {
	res, err := r.u.tx.ExecContext(ctx,
		`UPDATE attempt_commits
		    SET last_progress_at = ?, commit_deadline_at = ?, updated_at = ?
		  WHERE commit_id = ?
		    AND status IN ('DECLARED', 'UPLOADING')`,
		nowStr, deadlineStr, nowStr, commitID,
	)
	if err != nil {
		return 0, fmt.Errorf("completion.AttemptCommitRepository.UpdateProgress: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// UpdateReadyCountExhaustive recomputes ready_output_count from the
// declarations × artifacts JOIN for the canonical fence, CAS on
// non-terminal status. Used by CompleteUpload step 4.
func (r *sqliteAttemptCommitRepo) UpdateReadyCountExhaustive(ctx context.Context, fence SQLFencer, nowStr string) error {
	_, err := r.u.tx.ExecContext(ctx,
		`UPDATE attempt_commits
		    SET ready_output_count = (
		        SELECT COUNT(*)
		          FROM task_output_declarations d
		          JOIN artifacts a ON a.id = d.artifact_id
		         WHERE d.commit_id = attempt_commits.commit_id
		           AND a.status = 'READY'
		    ),
		    updated_at = ?
		  WHERE commit_id IN (
		      SELECT commit_id FROM attempt_commits
		       WHERE `+fence.SQLWhere()+`
		  )
		    AND status IN ('DECLARED','UPLOADING','RECEIVED','VERIFYING')`,
		append([]any{nowStr}, fence.SQLArgs()...)...,
	)
	if err != nil {
		return fmt.Errorf("completion.AttemptCommitRepository.UpdateReadyCountExhaustive: %w", err)
	}
	return nil
}

// SetExpired transitions attempt_commits to EXPIRED for the canonical
// fence row, CAS on deadline-elapsed AND ready<required AND non-terminal.
func (r *sqliteAttemptCommitRepo) SetExpired(ctx context.Context, fence SQLFencer, nowStr string) error {
	_, err := r.u.tx.ExecContext(ctx,
		`UPDATE attempt_commits
		    SET status = 'EXPIRED',
		        rejected_code = 'COMMIT_DEADLINE_EXCEEDED',
		        rejected_message = 'deadline elapsed with incomplete ready set',
		        updated_at = ?
		  WHERE `+fence.SQLWhere()+`
		    AND commit_deadline_at < ?
		    AND ready_output_count < required_output_count
		    AND status IN ('DECLARED','UPLOADING','RECEIVED','VERIFYING')`,
		append([]any{nowStr}, append(fence.SQLArgs(), nowStr)...)...,
	)
	if err != nil {
		return fmt.Errorf("completion.AttemptCommitRepository.SetExpired: %w", err)
	}
	return nil
}

// SetExpiredByID transitions attempt_commits to EXPIRED by commit_id,
// CAS on non-terminal status. ReconcileAttempt's repair-forward path.
func (r *sqliteAttemptCommitRepo) SetExpiredByID(ctx context.Context, commitID, nowStr string) error {
	_, err := r.u.tx.ExecContext(ctx,
		`UPDATE attempt_commits
		    SET status = 'EXPIRED',
		        rejected_code = 'COMMIT_DEADLINE_EXCEEDED',
		        rejected_message = 'ReconcileAttempt: commit_deadline_at elapsed',
		        updated_at = ?
		  WHERE commit_id = ? AND status IN ('DECLARED','UPLOADING','RECEIVED')`,
		nowStr, commitID,
	)
	if err != nil {
		return fmt.Errorf("completion.AttemptCommitRepository.SetExpiredByID: %w", err)
	}
	return nil
}

// MarkCommitted transitions attempt_commits to COMMITTED, CAS on
// non-terminal status.
func (r *sqliteAttemptCommitRepo) MarkCommitted(ctx context.Context, commitID, nowStr string) error {
	_, err := r.u.tx.ExecContext(ctx,
		`UPDATE attempt_commits
		    SET status = 'COMMITTED', committed_at = ?, updated_at = ?
		  WHERE commit_id = ? AND status IN ('DECLARED','UPLOADING','RECEIVED','VERIFYING')`,
		nowStr, nowStr, commitID,
	)
	if err != nil {
		return fmt.Errorf("completion.AttemptCommitRepository.MarkCommitted: %w", err)
	}
	return nil
}

// GetArtifactUploadState reads the artifact_uploads row the
// CompleteUpload four-branch gate inspects. CAS gates are
// enforced by the caller, not here — this is a pure read.
func (r *sqliteAttemptCommitRepo) GetArtifactUploadState(ctx context.Context, uploadID string) (*ArtifactUploadState, error) {
	if uploadID == "" {
		return nil, fmt.Errorf("completion.AttemptCommitRepository.GetArtifactUploadState: uploadID empty")
	}
	var (
		expected  sql.NullString
		received  sql.NullString
		rowStatus string
	)
	err := r.u.tx.QueryRowContext(ctx,
		`SELECT expected_sha256, received_sha256, status
		   FROM artifact_uploads
		  WHERE upload_id = ?`,
		uploadID,
	).Scan(&expected, &received, &rowStatus)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: upload_id=%s", ErrAttemptCommitNotFound, uploadID)
		}
		return nil, fmt.Errorf("completion.AttemptCommitRepository.GetArtifactUploadState: %w", err)
	}
	return &ArtifactUploadState{
		UploadID:       uploadID,
		ExpectedSHA256: expected.String,
		ReceivedSHA256: received.String,
		Status:         rowStatus,
	}, nil
}

// CompleteArtifactUpload drives the artifact_uploads + artifacts CAS
// pair CompleteUpload's four-branch verdict resolves to. Branch D
// (SHA mismatch) is rejected upstream; this method only sees
// KeepVerifying or Ready verdicts.
//
// The artifacts.id link is via artifact_uploads.artifact_id (FK).
// The CAS guards are shared: artifact_uploads.status IN
// ('CREATED','UPLOADING','RECEIVED') AND
// artifacts.status IN ('STAGING','VERIFYING').
func (r *sqliteAttemptCommitRepo) CompleteArtifactUpload(
	ctx context.Context,
	verdict ArtifactCompletionVerdict,
	uploadID, serverSHA, nowStr string,
) error {
	switch verdict {
	case ArtifactReady:
		// Branch C: server SHA present and matches the canonical
		// reference. received_sha256 is stamped verbatim from
		// serverSHA so a stale chunked-handshake probe value
		// cannot survive a verified re-CAS.
		if _, err := r.u.tx.ExecContext(ctx,
			`UPDATE artifact_uploads
			    SET status = 'COMPLETED', completed_at = ?, received_sha256 = ?
			  WHERE upload_id = ? AND status IN ('CREATED','UPLOADING','RECEIVED')`,
			nowStr, serverSHA, uploadID,
		); err != nil {
			return fmt.Errorf("completion.AttemptCommitRepository.CompleteArtifactUpload(Ready) artifact_uploads: %w", err)
		}
		if _, err := r.u.tx.ExecContext(ctx,
			`UPDATE artifacts
			    SET status = 'READY', verified_at = ?
			  WHERE id = (SELECT artifact_id FROM artifact_uploads WHERE upload_id = ?)
			    AND status IN ('STAGING','VERIFYING')`,
			nowStr, uploadID,
		); err != nil {
			return fmt.Errorf("completion.AttemptCommitRepository.CompleteArtifactUpload(Ready) artifacts: %w", err)
		}
		return nil

	case ArtifactKeepVerifying:
		// Branch A or B: no master SHA, or master SHA disagrees
		// with declarative. received_sha256 is preserved via
		// COALESCE so a partial probe value from a previous
		// chunked-handshake tick survives.
		if _, err := r.u.tx.ExecContext(ctx,
			`UPDATE artifact_uploads
			    SET status = 'COMPLETED', completed_at = ?,
			        received_sha256 = COALESCE(received_sha256, ?)
			  WHERE upload_id = ? AND status IN ('CREATED','UPLOADING','RECEIVED')`,
			nowStr, serverSHA, uploadID,
		); err != nil {
			return fmt.Errorf("completion.AttemptCommitRepository.CompleteArtifactUpload(KeepVerifying) artifact_uploads: %w", err)
		}
		if _, err := r.u.tx.ExecContext(ctx,
			`UPDATE artifacts
			    SET status = 'VERIFYING', verified_at = ?
			  WHERE id = (SELECT artifact_id FROM artifact_uploads WHERE upload_id = ?)
			    AND status IN ('STAGING','VERIFYING')`,
			nowStr, uploadID,
		); err != nil {
			return fmt.Errorf("completion.AttemptCommitRepository.CompleteArtifactUpload(KeepVerifying) artifacts: %w", err)
		}
		return nil

	default:
		return fmt.Errorf("completion.AttemptCommitRepository.CompleteArtifactUpload: unknown verdict=%d", verdict)
	}
}

// GetCommitResult reads the post-update snapshot of attempt_commits
// joined with tasks + jobs + artifacts so the caller receives a
// self-contained CommitResult without a second roundtrip. Called
// by the Coordinator BEFORE tx.Commit() so the read is part of the
// same LevelSerializable write lock (Verdetto P1 #9 / tx-after-commit
// fix): the snapshot cannot drift from the just-written SUCCEEDED
// state under a concurrent writer.
//
// Returns (nil, ErrAttemptCommitNotFound) on a missing row.
func (r *sqliteAttemptCommitRepo) GetCommitResult(ctx context.Context, commitID string) (*CommitResult, error) {
	if commitID == "" {
		return nil, fmt.Errorf("completion.AttemptCommitRepository.GetCommitResult: commitID empty")
	}
	var (
		res           CommitResult
		committedAt   sql.NullString
		taskStatus    sql.NullString
		jobStatus     sql.NullString
	)
	err := r.u.tx.QueryRowContext(ctx,
		`SELECT ac.commit_id, ac.task_id, ac.attempt_id, ac.job_id,
		        COALESCE(t.status, ''), COALESCE(j.status, ''), ac.committed_at
		   FROM attempt_commits ac
		   LEFT JOIN tasks  t ON t.task_id  = ac.task_id
		   LEFT JOIN jobs   j ON j.job_id   = ac.job_id
		  WHERE ac.commit_id = ?`,
		commitID).Scan(&res.CommitID, &res.TaskID, &res.AttemptID, &res.JobID,
		&taskStatus, &jobStatus, &committedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: commit_id=%s", ErrAttemptCommitNotFound, commitID)
		}
		return nil, fmt.Errorf("completion.AttemptCommitRepository.GetCommitResult: %w", err)
	}
	res.TaskStatus = taskStatus.String
	res.JobStatus = jobStatus.String
	if committedAt.Valid && committedAt.String != "" {
		if t, perr := time.Parse(time.RFC3339Nano, committedAt.String); perr == nil {
			res.CommittedAt = &t
		}
	}
	rows, err := r.u.tx.QueryContext(ctx,
		`SELECT a.id FROM artifacts a
		   JOIN task_output_declarations d ON d.artifact_id = a.id
		   JOIN attempt_commits ac ON ac.commit_id = d.commit_id
		  WHERE ac.commit_id = ? AND a.status = 'READY'`,
		commitID)
	if err != nil {
		return nil, fmt.Errorf("completion.AttemptCommitRepository.GetCommitResult artifacts: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if sErr := rows.Scan(&id); sErr == nil {
			res.ArtifactIDs = append(res.ArtifactIDs, id)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("completion.AttemptCommitRepository.GetCommitResult artifacts rows: %w", err)
	}
	return &res, nil
}

// ────────────────────────────────────────────────────────────────────────
// TaskAttemptRepository
// ────────────────────────────────────────────────────────────────────────

type sqliteTaskAttemptRepo struct {
	u *sqliteUnitOfWork
}

// MarkSucceeded transitions task_attempts to SUCCEEDED CAS-gated on
// (attempt_id, worker_id, lease_id) AND status NOT IN terminal.
func (r *sqliteTaskAttemptRepo) MarkSucceeded(ctx context.Context, attemptID, workerID, leaseID, nowStr string) error {
	_, err := r.u.tx.ExecContext(ctx,
		`UPDATE task_attempts
		    SET status = 'SUCCEEDED', completed_at = COALESCE(completed_at, ?),
		        report_version = report_version + 1, updated_at = ?
		  WHERE id = ? AND worker_id = ? AND lease_id = ?
		    AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT')`,
		nowStr, nowStr, attemptID, workerID, leaseID,
	)
	if err != nil {
		return fmt.Errorf("completion.TaskAttemptRepository.MarkSucceeded: %w", err)
	}
	return nil
}

// ────────────────────────────────────────────────────────────────────────
// TaskRepository
// ────────────────────────────────────────────────────────────────────────

type sqliteTaskRepo struct {
	u *sqliteUnitOfWork
}

// MarkSucceeded transitions tasks to SUCCEEDED + stamps the winning
// attempt metadata for the canonical (task_id, attempt_id, worker_id,
// lease_id) tuple, status IN ('RUNNING','LEASED').
func (r *sqliteTaskRepo) MarkSucceeded(ctx context.Context, taskID, attemptID, workerID, leaseID, nowStr string) error {
	_, err := r.u.tx.ExecContext(ctx,
		`UPDATE tasks
		    SET status = 'SUCCEEDED', completed_at = ?, updated_at = ?,
		        winning_attempt_id = ?, winning_attempt_committed_at = ?,
		        winning_attempt_terminal_pending = 0, revision = revision + 1
		  WHERE task_id = ? AND attempt_id = ? AND worker_id = ? AND lease_id = ?
		    AND status IN ('RUNNING','LEASED')`,
		nowStr, nowStr, attemptID, nowStr,
		taskID, attemptID, workerID, leaseID,
	)
	if err != nil {
		return fmt.Errorf("completion.TaskRepository.MarkSucceeded: %w", err)
	}
	return nil
}

// ────────────────────────────────────────────────────────────────────────
// JobFinalizationRepository
// ────────────────────────────────────────────────────────────────────────

type sqliteJobRepo struct {
	u *sqliteUnitOfWork
}

// MarkSucceededIfTasksDone flips the job to SUCCEEDED only when every
// sibling task is also SUCCEEDED. 0 rows affected is benign when a
// task is still pending (the CAS guard on NOT EXISTS is the contract).
func (r *sqliteJobRepo) MarkSucceededIfTasksDone(ctx context.Context, jobID, nowStr string) error {
	_, err := r.u.tx.ExecContext(ctx,
		`UPDATE jobs
		    SET status = 'SUCCEEDED', completed_at = ?, updated_at = ?,
		        revision = revision + 1
		  WHERE job_id = ? AND status IN ('RUNNING','AWAITING_ARTIFACT')
		    AND NOT EXISTS (
		        SELECT 1 FROM tasks t
		         WHERE t.job_id = ? AND t.status != 'SUCCEEDED'
		    )`,
		nowStr, nowStr, jobID, jobID,
	)
	if err != nil {
		return fmt.Errorf("completion.JobFinalizationRepository.MarkSucceededIfTasksDone: %w", err)
	}
	return nil
}

// ────────────────────────────────────────────────────────────────────────
// DeliveryRepository
// ────────────────────────────────────────────────────────────────────────

type sqliteDeliveryRepo struct {
	u *sqliteUnitOfWork
}

// InsertDeliveriesForJob computes the (artifact × destination) cross
// product and idempotently INSERTs job_deliveries rows. The
// idempotency_key UNIQUE absorbs re-emission duplicates.
func (r *sqliteDeliveryRepo) InsertDeliveriesForJob(ctx context.Context, jobID, nowStr string) error {
	rows, err := r.u.tx.QueryContext(ctx,
		`SELECT a.id, dd.destination_id
		   FROM artifacts a
		   CROSS JOIN delivery_destinations dd
		  WHERE a.job_id = ?
		    AND a.status = 'READY'
		    AND dd.enabled = 1`,
		jobID,
	)
	if err != nil {
		return fmt.Errorf("completion.DeliveryRepository.InsertDeliveriesForJob: cross-join: %w", err)
	}
	defer rows.Close()

	type destKey struct{ Art, Dst string }
	seen := make(map[destKey]bool)
	for rows.Next() {
		var art, dst string
		if scanErr := rows.Scan(&art, &dst); scanErr != nil || art == "" || dst == "" {
			continue
		}
		k := destKey{art, dst}
		if seen[k] {
			continue
		}
		seen[k] = true
		id := "jbd_comp_" + art + "_" + dst
		if _, err := r.u.tx.ExecContext(ctx,
			`INSERT OR IGNORE INTO job_deliveries (
			    delivery_id, artifact_id, destination_id, status, idempotency_key,
			    created_at, updated_at
			) VALUES (?, ?, ?, 'PENDING', ?, ?, ?)`,
			id, art, dst, art+"_"+dst, nowStr, nowStr,
		); err != nil {
			return fmt.Errorf("completion.DeliveryRepository.InsertDeliveriesForJob INSERT %s:%s: %w", art, dst, err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("completion.DeliveryRepository.InsertDeliveriesForJob rows: %w", err)
	}
	return nil
}

// ────────────────────────────────────────────────────────────────────────
// OutboxRepository
// ────────────────────────────────────────────────────────────────────────

type sqliteOutboxRepo struct {
	u *sqliteUnitOfWork
}

// InsertEvent idempotently INSERTs an outbox row with the supplied
// event_id (UNIQUE on the primary key absorbs duplicates).
func (r *sqliteOutboxRepo) InsertEvent(ctx context.Context, eventID, aggregateType, aggregateID, eventType, payloadJSON, nowStr string) error {
	_, err := r.u.tx.ExecContext(ctx,
		`INSERT OR IGNORE INTO outbox_events (
		    event_id, aggregate_type, aggregate_id, event_type, payload_json,
		    status, available_at, attempt_count, created_at
		) VALUES (?, ?, ?, ?, ?, 'PENDING', ?, 0, ?)`,
		eventID, aggregateType, aggregateID, eventType, payloadJSON,
		nowStr, nowStr,
	)
	if err != nil {
		return fmt.Errorf("completion.OutboxRepository.InsertEvent: %w", err)
	}
	return nil
}
