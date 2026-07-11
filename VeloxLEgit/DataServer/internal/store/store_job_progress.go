package store

import (
	"context"
	"database/sql"
)

// UpsertJobProgress writes a progress snapshot (upsert by job_id).
func (s *SQLiteStore) UpsertJobProgress(ctx context.Context, jobID string, attemptNumber int, percent float64, stage string, currentItem, totalItems int, message, updatedAt string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO job_progress (job_id, attempt_number, percent, stage, current_item, total_items, message, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(job_id) DO UPDATE SET
			attempt_number = excluded.attempt_number,
			percent = excluded.percent,
			stage = excluded.stage,
			current_item = excluded.current_item,
			total_items = excluded.total_items,
			message = excluded.message,
			updated_at = excluded.updated_at
	`, jobID, attemptNumber, percent, stage, currentItem, totalItems, message, updatedAt)
	return err
}

// GetJobProgress returns the current progress for a job.
func (s *SQLiteStore) GetJobProgress(ctx context.Context, jobID string) (*ProgressSnapshot, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT job_id, attempt_number, percent, stage, current_item, total_items, message, updated_at
		FROM job_progress WHERE job_id = ?
	`, jobID)

	var ps ProgressSnapshot
	var updatedAt string
	err := row.Scan(&ps.JobID, &ps.AttemptNumber, &ps.Percent, &ps.Stage,
		&ps.CurrentItem, &ps.TotalItems, &ps.Message, &updatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &ps, nil
}
