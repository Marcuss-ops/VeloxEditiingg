package store

import (
	"context"
	"fmt"
	"time"
)

// ── CAS Job Transition ──

// TransitionJobStatus atomically transitions a job from expectedStatus to newStatus
// using optimistic locking via the revision column.
// Returns (newRevision, error). Error is non-nil if the transition fails.
func (s *SQLiteStore) TransitionJobStatus(ctx context.Context, jobID string, expectedStatus, newStatus string, revision int) (int, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	newRevision := revision + 1

	result, err := s.db.Exec(
		`UPDATE jobs
		 SET status = ?, revision = ?, updated_at = ?
		 WHERE job_id = ? AND status = ? AND revision = ?`,
		newStatus, newRevision, now, jobID, expectedStatus, revision,
	)
	if err != nil {
		return 0, fmt.Errorf("transition exec: %w", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("transition rows affected: %w", err)
	}
	if affected == 0 {
		return 0, fmt.Errorf("transition conflict: job %s expected status %q revision %d: %w",
			jobID, expectedStatus, revision, ErrTransitionConflict)
	}

	return newRevision, nil
}

// UpdateJobSupplementary updates non-CAS fields on a job after a successful transition.
func (s *SQLiteStore) UpdateJobSupplementary(jobID string, fields map[string]interface{}) error {
	if len(fields) == 0 {
		return nil
	}

	setClauses := []string{}
	args := []interface{}{}

	for key, value := range fields {
		switch key {
		case "completed_at", "last_error", "error_message", "failed_at", "failed_by",
			"lease_id", "lease_expiry", "assigned_to", "claimed_by", "started_at",
			"artifact_id", "output_sha256", "upload_idempotency_key":
			setClauses = append(setClauses, key+" = ?")
			args = append(args, value)
		}
	}

	if len(setClauses) == 0 {
		return nil
	}

	query := "UPDATE jobs SET "
	for i, clause := range setClauses {
		if i > 0 {
			query += ", "
		}
		query += clause
	}
	query += " WHERE job_id = ?"
	args = append(args, jobID)

	_, err := s.db.Exec(query, args...)
	return err
}

// GetJobRevision returns the current revision of a job.
func (s *SQLiteStore) GetJobRevision(jobID string) (int, error) {
	var revision int
	err := s.db.QueryRow(`SELECT revision FROM jobs WHERE job_id=?`, jobID).Scan(&revision)
	if err != nil {
		return 0, err
	}
	return revision, nil
}
