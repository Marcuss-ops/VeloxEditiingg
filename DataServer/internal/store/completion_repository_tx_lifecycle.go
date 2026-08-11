package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"velox-server/internal/sqliteerr"
)

func (r *sqliteCompletionTx) ReadCompletionFence(ctx context.Context, f CompletionFence, allowMissing bool) (*CompletionAttemptState, error) {
	var commitID, status, worker, lease string
	var rev int
	err := r.tx.QueryRowContext(ctx, `SELECT commit_id,status,worker_id,lease_id,task_revision FROM attempt_commits WHERE task_id=? AND attempt_id=?`, f.TaskID, f.AttemptID).Scan(&commitID, &status, &worker, &lease, &rev)
	if errors.Is(err, sql.ErrNoRows) {
		if allowMissing {
			return nil, nil
		}
		return nil, fmt.Errorf("%w: task_id=%s attempt_id=%s", ErrCompletionAttemptNotFound, f.TaskID, f.AttemptID)
	}
	if err != nil {
		return nil, fmt.Errorf("store: completion fence read: %w", err)
	}
	if worker != f.WorkerID || lease != f.LeaseID || rev != f.Revision {
		return nil, fmt.Errorf("%w: stored worker/lease/revision mismatch", ErrCompletionTransitionConflict)
	}
	return &CompletionAttemptState{CommitID: commitID, Status: status, TaskRevision: rev}, nil
}

func (r *sqliteCompletionTx) InsertCompletionAttempt(ctx context.Context, p CompletionDeclareParams) (string, error) {
	res, err := r.tx.ExecContext(ctx, `INSERT OR IGNORE INTO attempt_commits (
		commit_id,task_id,attempt_id,job_id,worker_id,lease_id,task_revision,status,
		required_output_count,commit_token_hash,commit_deadline_at,last_progress_at,created_at,updated_at
	) VALUES (?,?,?,?,?,?,?,'DECLARED',?,?,?,?,?,?)`,
		p.CommitID, p.TaskID, p.AttemptID, p.JobID, p.WorkerID, p.LeaseID, p.Revision,
		p.RequiredOutputCount, p.TokenHash, p.Deadline, p.Now, p.Now, p.Now)
	if err != nil {
		return "", fmt.Errorf("store: insert completion attempt: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 1 {
		return p.CommitID, nil
	}
	var canonical string
	if err := r.tx.QueryRowContext(ctx, `SELECT commit_id FROM attempt_commits WHERE task_id=? AND attempt_id=?`, p.TaskID, p.AttemptID).Scan(&canonical); err != nil {
		return "", fmt.Errorf("store: resolve completion attempt race: %w", err)
	}
	return canonical, nil
}

func (r *sqliteCompletionTx) InsertCompletionDeclaration(ctx context.Context, p CompletionDeclarationParams) error {
	_, err := r.tx.ExecContext(ctx, `INSERT OR IGNORE INTO task_output_declarations (
		declaration_id,commit_id,task_id,attempt_id,output_kind,logical_name,mime_type,
		expected_size_bytes,expected_sha256,worker_spool_key,status,created_at,updated_at
	) VALUES (?,?,?,?,?,?,?,?,?,?,'DECLARED',?,?)`,
		p.DeclarationID, p.CommitID, p.TaskID, p.AttemptID, p.OutputKind, p.LogicalName, p.MimeType,
		p.SizeBytes, p.SHA256, p.WorkerSpoolKey, p.Now, p.Now)
	if err != nil {
		return fmt.Errorf("store: insert completion declaration: %w", err)
	}
	return nil
}

func (r *sqliteCompletionTx) GetCompletionDeclarationID(ctx context.Context, commitID, outputKind, logicalName string) (string, error) {
	var id string
	err := r.tx.QueryRowContext(ctx, `SELECT declaration_id FROM task_output_declarations WHERE commit_id=? AND output_kind=? AND logical_name=?`, commitID, outputKind, logicalName).Scan(&id)
	if err != nil {
		return "", fmt.Errorf("store: resolve completion declaration: %w", err)
	}
	return id, nil
}

func (r *sqliteCompletionTx) GetCompletionUploadState(ctx context.Context, uploadID string) (*CompletionUploadState, error) {
	var expected, received, mimeType sql.NullString
	var status, artifactID, storageKey string
	var size sql.NullInt64
	err := r.tx.QueryRowContext(ctx, `SELECT au.expected_sha256,au.received_sha256,au.status,au.artifact_id,au.temporary_storage_key,COALESCE(d.mime_type,a.type,''),COALESCE(au.received_size_bytes,au.expected_size_bytes,a.size_bytes,0) FROM artifact_uploads au JOIN artifacts a ON a.id=au.artifact_id LEFT JOIN task_output_declarations d ON d.upload_id=au.upload_id WHERE au.upload_id=?`, uploadID).Scan(&expected, &received, &status, &artifactID, &storageKey, &mimeType, &size)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: upload_id=%s", ErrCompletionAttemptNotFound, uploadID)
	}
	if err != nil {
		return nil, fmt.Errorf("store: read completion upload: %w", err)
	}
	return &CompletionUploadState{UploadID: uploadID, ExpectedSHA256: expected.String, ReceivedSHA256: received.String, Status: status, ArtifactID: artifactID, TemporaryStorageKey: storageKey, MimeType: mimeType.String, SizeBytes: size.Int64}, nil
}

func (r *sqliteCompletionTx) CompleteCompletionUpload(ctx context.Context, verdict CompletionArtifactVerdict, uploadID, serverSHA, now string) error {
	if verdict == CompletionReady {
		res, err := r.tx.ExecContext(ctx, `UPDATE artifact_uploads SET status='COMPLETED',completed_at=?,received_sha256=? WHERE upload_id=? AND status IN ('CREATED','UPLOADING','RECEIVED')`, now, serverSHA, uploadID)
		if err != nil {
			return fmt.Errorf("store: completion upload ready CAS: %w", err)
		}
		if n, rowsErr := res.RowsAffected(); rowsErr != nil {
			return fmt.Errorf("store: completion upload ready rows: %w", rowsErr)
		} else if n != 1 {
			return fmt.Errorf("%w: upload=%s ready rows=%d", ErrCompletionTransitionConflict, uploadID, n)
		}
		res, err = r.tx.ExecContext(ctx, `UPDATE artifacts SET status='READY',verified_at=?,output_kind=COALESCE((SELECT output_kind FROM task_output_declarations WHERE artifact_id=artifacts.id LIMIT 1),output_kind) WHERE id=(SELECT artifact_id FROM artifact_uploads WHERE upload_id=?) AND status IN ('STAGING','VERIFYING')`, now, uploadID)
		if err != nil {
			return fmt.Errorf("store: completion artifact ready CAS: %w", err)
		}
		if n, rowsErr := res.RowsAffected(); rowsErr != nil {
			return fmt.Errorf("store: completion artifact ready rows: %w", rowsErr)
		} else if n != 1 {
			return fmt.Errorf("%w: upload=%s artifact ready rows=%d", ErrCompletionTransitionConflict, uploadID, n)
		}
		return nil
	}
	if verdict == CompletionKeepVerifying {
		res, err := r.tx.ExecContext(ctx, `UPDATE artifact_uploads SET status='COMPLETED',completed_at=?,received_sha256=COALESCE(received_sha256,?) WHERE upload_id=? AND status IN ('CREATED','UPLOADING','RECEIVED')`, now, serverSHA, uploadID)
		if err != nil {
			return fmt.Errorf("store: completion upload verifying CAS: %w", err)
		}
		if n, rowsErr := res.RowsAffected(); rowsErr != nil {
			return fmt.Errorf("store: completion upload verifying rows: %w", rowsErr)
		} else if n != 1 {
			return fmt.Errorf("%w: upload=%s verifying rows=%d", ErrCompletionTransitionConflict, uploadID, n)
		}
		res, err = r.tx.ExecContext(ctx, `UPDATE artifacts SET status='VERIFYING',verified_at=? WHERE id=(SELECT artifact_id FROM artifact_uploads WHERE upload_id=?) AND status IN ('STAGING','VERIFYING')`, now, uploadID)
		if err != nil {
			return fmt.Errorf("store: completion artifact verifying CAS: %w", err)
		}
		if n, rowsErr := res.RowsAffected(); rowsErr != nil {
			return fmt.Errorf("store: completion artifact verifying rows: %w", rowsErr)
		} else if n != 1 {
			return fmt.Errorf("%w: upload=%s artifact verifying rows=%d", ErrCompletionTransitionConflict, uploadID, n)
		}
		return nil
	}
	return fmt.Errorf("store: unknown completion artifact verdict %d", verdict)
}

func (r *sqliteCompletionTx) StampCompletionArtifact(ctx context.Context, artifactID, storageKey, sha string, size int64) error {
	_, err := r.tx.ExecContext(ctx, `UPDATE artifacts SET storage_provider='local',storage_key=?,sha256=?,size_bytes=? WHERE id=?`, storageKey, sha, size, artifactID)
	if err != nil {
		if sqliteerr.IsUniqueConstraint(err) {
			return fmt.Errorf("%w: %v", ErrCompletionCanonicalConflict, err)
		}
		return fmt.Errorf("store: stamp completion artifact: %w", err)
	}
	return nil
}

func (r *sqliteCompletionTx) UpdateCompletionProgress(ctx context.Context, commitID, now, deadline string) (int64, error) {
	res, err := r.tx.ExecContext(ctx, `UPDATE attempt_commits SET last_progress_at=?,commit_deadline_at=?,updated_at=? WHERE commit_id=? AND status IN ('DECLARED','UPLOADING')`, now, deadline, now, commitID)
	if err != nil {
		return 0, fmt.Errorf("store: completion progress: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (r *sqliteCompletionTx) UpdateCompletionUploadedBytes(ctx context.Context, f CompletionFence, uploadID string, uploadedBytes int64, now string) error {
	res, err := r.tx.ExecContext(ctx, `UPDATE task_output_declarations SET uploaded_bytes=MAX(uploaded_bytes,?),updated_at=MAX(updated_at,?) WHERE commit_id IN (SELECT commit_id FROM attempt_commits WHERE task_id=? AND attempt_id=? AND worker_id=? AND lease_id=?) AND upload_id=?`, uploadedBytes, now, f.TaskID, f.AttemptID, f.WorkerID, f.LeaseID, uploadID)
	if err != nil {
		return fmt.Errorf("store: completion uploaded bytes: %w", err)
	}
	if n, rowsErr := res.RowsAffected(); rowsErr != nil {
		return fmt.Errorf("store: completion uploaded bytes rows: %w", rowsErr)
	} else if n != 1 {
		return fmt.Errorf("%w: upload=%s uploaded bytes rows=%d", ErrCompletionTransitionConflict, uploadID, n)
	}
	return nil
}

func (r *sqliteCompletionTx) UpdateCompletionReadyCount(ctx context.Context, f CompletionFence, now string) error {
	res, err := r.tx.ExecContext(ctx, `UPDATE attempt_commits SET ready_output_count=(SELECT COUNT(*) FROM task_output_declarations d JOIN artifacts a ON a.id=d.artifact_id WHERE d.commit_id=attempt_commits.commit_id AND a.status='READY'),updated_at=? WHERE commit_id IN (SELECT commit_id FROM attempt_commits WHERE task_id=? AND attempt_id=? AND worker_id=? AND lease_id=? AND task_revision=?) AND status IN ('DECLARED','UPLOADING','RECEIVED','VERIFYING')`, now, f.TaskID, f.AttemptID, f.WorkerID, f.LeaseID, f.Revision)
	if err != nil {
		return fmt.Errorf("store: completion ready count: %w", err)
	}
	if n, rowsErr := res.RowsAffected(); rowsErr != nil {
		return fmt.Errorf("store: completion ready count rows: %w", rowsErr)
	} else if n != 1 {
		return fmt.Errorf("%w: task=%s attempt=%s ready count rows=%d", ErrCompletionTransitionConflict, f.TaskID, f.AttemptID, n)
	}
	return nil
}

func (r *sqliteCompletionTx) ExpireCompletionAttempt(ctx context.Context, f CompletionFence, now string) error {
	_, err := r.tx.ExecContext(ctx, `UPDATE attempt_commits SET status='EXPIRED',rejected_code='COMMIT_DEADLINE_EXCEEDED',rejected_message='deadline elapsed with incomplete ready set',updated_at=? WHERE task_id=? AND attempt_id=? AND worker_id=? AND lease_id=? AND task_revision=? AND commit_deadline_at<? AND ready_output_count<required_output_count AND status IN ('DECLARED','UPLOADING','RECEIVED','VERIFYING')`, now, f.TaskID, f.AttemptID, f.WorkerID, f.LeaseID, f.Revision, now)
	if err != nil {
		return fmt.Errorf("store: expire completion attempt: %w", err)
	}
	return nil
}

func (r *sqliteCompletionTx) ExpireCompletionAttemptByID(ctx context.Context, commitID, now string) error {
	_, err := r.tx.ExecContext(ctx, `UPDATE attempt_commits SET status='EXPIRED',rejected_code='COMMIT_DEADLINE_EXCEEDED',rejected_message='ReconcileAttempt: commit_deadline_at elapsed',updated_at=? WHERE commit_id=? AND status IN ('DECLARED','UPLOADING','RECEIVED')`, now, commitID)
	if err != nil {
		return fmt.Errorf("store: expire completion attempt by id: %w", err)
	}
	return nil
}
