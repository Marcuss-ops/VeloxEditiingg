package completionstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

func (s *SQLiteCompletionStore) ListCompletionUploadBindings(ctx context.Context, commitID string) ([]CompletionUploadBinding, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT d.declaration_id,d.commit_id,COALESCE(d.upload_id,''),COALESCE(d.artifact_id,''),d.task_id,d.attempt_id,ac.worker_id,ac.lease_id,ac.task_revision,d.output_kind,d.logical_name FROM task_output_declarations d JOIN attempt_commits ac ON ac.commit_id=d.commit_id WHERE d.commit_id=? ORDER BY d.rowid`, commitID)
	if err != nil {
		return nil, fmt.Errorf("store: list completion bindings: %w", err)
	}
	defer rows.Close()
	var out []CompletionUploadBinding
	for rows.Next() {
		var b CompletionUploadBinding
		if err := rows.Scan(&b.DeclarationID, &b.CommitID, &b.UploadID, &b.ArtifactID, &b.TaskID, &b.AttemptID, &b.WorkerID, &b.LeaseID, &b.Revision, &b.OutputKind, &b.LogicalName); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (s *SQLiteCompletionStore) GetCompletionUploadBinding(ctx context.Context, uploadID string) (*CompletionUploadBinding, error) {
	var b CompletionUploadBinding
	err := s.db.QueryRowContext(ctx, `SELECT d.declaration_id,d.commit_id,d.upload_id,COALESCE(d.artifact_id,''),d.task_id,d.attempt_id,ac.worker_id,ac.lease_id,ac.task_revision,d.output_kind,d.logical_name FROM task_output_declarations d JOIN attempt_commits ac ON ac.commit_id=d.commit_id WHERE d.upload_id=?`, uploadID).Scan(&b.DeclarationID, &b.CommitID, &b.UploadID, &b.ArtifactID, &b.TaskID, &b.AttemptID, &b.WorkerID, &b.LeaseID, &b.Revision, &b.OutputKind, &b.LogicalName)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("store: completion upload binding not found: %w", ErrCompletionAttemptNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("store: get completion binding: %w", err)
	}
	return &b, nil
}

func (s *SQLiteCompletionStore) BindCompletionUpload(ctx context.Context, declarationID, uploadID, artifactID string) error {
	result, err := s.db.ExecContext(ctx, `UPDATE task_output_declarations SET upload_id=?,artifact_id=?,updated_at=? WHERE declaration_id=? AND (upload_id IS NULL OR upload_id=?) AND (artifact_id IS NULL OR artifact_id=?)`, uploadID, artifactID, time.Now().UTC().Format(time.RFC3339Nano), declarationID, uploadID, artifactID)
	if err != nil {
		return fmt.Errorf("store: bind completion upload: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: bind completion upload rows affected: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("%w: declaration_id=%s", ErrCompletionBindingConflict, declarationID)
	}
	return nil
}

func (s *SQLiteCompletionStore) GetCompletionCommitTokenHash(ctx context.Context, commitID string) (string, error) {
	var h string
	err := s.db.QueryRowContext(ctx, `SELECT commit_token_hash FROM attempt_commits WHERE commit_id=?`, commitID).Scan(&h)
	if err != nil {
		return "", fmt.Errorf("store: get completion token hash: %w", err)
	}
	return h, nil
}
