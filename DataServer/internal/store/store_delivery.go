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

// GetJobRevision returns the current revision of a job.
func (s *SQLiteStore) GetJobRevision(jobID string) (int, error) {
	var revision int
	err := s.db.QueryRow(`SELECT revision FROM jobs WHERE job_id=?`, jobID).Scan(&revision)
	if err != nil {
		return 0, err
	}
	return revision, nil
}
