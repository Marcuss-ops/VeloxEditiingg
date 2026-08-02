package completion

// sqlite_uow_attempt_commit.go: sqliteAttemptCommitRepo — the
// attempt_commits + artifact_uploads repository implementation of the
// SQLite-backed UnitOfWork. Split out of sqlite_uow.go; the factory +
// UnitOfWork wiring lives in sqlite_uow.go and the remaining small repos
// in sqlite_uow_repos.go.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

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
		expected            sql.NullString
		received            sql.NullString
		rowStatus           string
		artifactID          string
		temporaryStorageKey string
		mimeType            sql.NullString
		sizeBytes           sql.NullInt64
	)
	err := r.u.tx.QueryRowContext(ctx,
		`SELECT au.expected_sha256, au.received_sha256, au.status,
		        au.artifact_id, au.temporary_storage_key,
		        COALESCE(d.mime_type, a.type, ''),
		        COALESCE(au.received_size_bytes, au.expected_size_bytes, a.size_bytes, 0)
		   FROM artifact_uploads au
		   JOIN artifacts a ON a.id = au.artifact_id
		   LEFT JOIN task_output_declarations d ON d.upload_id = au.upload_id
		  WHERE au.upload_id = ?`,
		uploadID,
	).Scan(&expected, &received, &rowStatus, &artifactID, &temporaryStorageKey, &mimeType, &sizeBytes)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: upload_id=%s", ErrAttemptCommitNotFound, uploadID)
		}
		return nil, fmt.Errorf("completion.AttemptCommitRepository.GetArtifactUploadState: %w", err)
	}
	return &ArtifactUploadState{
		UploadID:            uploadID,
		ExpectedSHA256:      expected.String,
		ReceivedSHA256:      received.String,
		Status:              rowStatus,
		ArtifactID:          artifactID,
		TemporaryStorageKey: temporaryStorageKey,
		MimeType:            mimeType.String,
		SizeBytes:           sizeBytes.Int64,
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
			    SET status = 'READY', verified_at = ?,
			        output_kind = COALESCE(
			            (SELECT output_kind FROM task_output_declarations
			              WHERE artifact_id = artifacts.id
			              LIMIT 1),
			            output_kind)
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
		res         CommitResult
		committedAt sql.NullString
		taskStatus  sql.NullString
		jobStatus   sql.NullString
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
