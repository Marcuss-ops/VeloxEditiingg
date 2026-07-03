// Package artifacts / service_auth.go
//
// AuthReader is the read-only persistence surface Service.BeginUpload
// uses for job / attempt / uniqueness auth. It hides *sql.DB from
// Service so the production composition root never injects a raw
// database handle into the auth path.
//
// Method set kept narrow: BeginUpload consumes exactly three queries.
// Adding more would grow the interface beyond its scope; future
// read-only concerns should land on a purpose-built reader (e.g.
// QuarantineReader) instead of AuthReader.
//
// ErrNoRows semantics on each method:
//
//   - LoadJob: returns (nil, nil). "No row" is a soft miss — BeginUpload
//     decides whether to surface it as ErrJobNotRunning.
//   - LoadAttempt: returns ErrAttemptMismatch-wrapped error. "No
//     attempt row" is treated as an auth failure, matching the
//     pre-split behavior of loadAttempt in service.go and the rest of
//     the auth chain's "absent == failed" semantics.
//   - FindExistingReadyArtifact: returns ("", nil). Soft miss —
//     BeginUpload decides whether to surface it as
//     ErrDuplicateReadyArtifact.
package artifacts

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// JobState captures the columns BeginUpload needs from `jobs`.
// Loaded with a single SELECT in AuthReader.LoadJob, then checked
// against the BeginUpload auth fields by the caller.
//
// Worker / lease identity does NOT live on the jobs row anymore —
// those columns were dropped by migration 048. Identity is verified
// at the task_attempts layer via AuthReader.LoadAttempt and at the
// artifact_uploads CAS chain in FinalizeVerified.
type JobState struct {
	Status   string
	Revision int
}

// AuthReader returns the auth-shaped projections Service requires.
// Read-only by contract — implementations MUST NOT add a write method
// without breaking the "auth is read-only" invariant.
type AuthReader interface {
	// LoadJob returns the JobState for jobID, or (nil, nil) when no
	// row matches.
	LoadJob(ctx context.Context, jobID string) (*JobState, error)
	// LoadAttempt returns (task_attempts.status, worker_id, lease_id)
	// for the (job_id, attempt_number) pair. A missing row returns a
	// ErrAttemptMismatch-wrapped error (a missing attempt is an auth
	// failure, not a soft miss).
	LoadAttempt(ctx context.Context, jobID string, attemptNumber int) (status, workerID, leaseID string, err error)
	// FindExistingReadyArtifact returns the artifact id of any READY
	// artifact of the given kind for the given job, or "" when none.
	// Used by BeginUpload's uniqueness gate (ErrDuplicateReadyArtifact).
	FindExistingReadyArtifact(ctx context.Context, jobID, kind string) (string, error)
}

// SQLiteAuthReader is the production AuthReader implementation.
//
// All methods tolerate sql.ErrNoRows (returning the canonical
// (nil, nil), zero-value, or ErrAttemptMismatch-wrapped error
// depending on the method) so BeginUpload's auth chain can
// distinguish "row is missing" from "DB error".
type SQLiteAuthReader struct {
	db *sql.DB
}

// NewSQLiteAuthReader constructs a SQLiteAuthReader. db must outlive
// the reader.
func NewSQLiteAuthReader(db *sql.DB) *SQLiteAuthReader {
	if db == nil {
		panic("artifacts: NewSQLiteAuthReader requires a non-nil *sql.DB")
	}
	return &SQLiteAuthReader{db: db}
}

// LoadJob reads auth-relevant columns of the `jobs` row.
func (r *SQLiteAuthReader) LoadJob(ctx context.Context, jobID string) (*JobState, error) {
	if jobID == "" {
		return nil, fmt.Errorf("artifacts: AuthReader.LoadJob: empty jobID")
	}
	row := r.db.QueryRowContext(ctx,
		`SELECT status, COALESCE(revision, 0) FROM jobs WHERE job_id = ?`, jobID)
	var j JobState
	if err := row.Scan(&j.Status, &j.Revision); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("artifacts: AuthReader.LoadJob: %w", err)
	}
	return &j, nil
}

// LoadAttempt reads auth-relevant columns of the task_attempts row
// (joined to tasks by job_id). The function name retains the historical
// "Attempt" suffix even though the underlying table is task_attempts
// post-migration 048.
//
// A missing attempt row returns ErrAttemptMismatch wrapped with
// (jobID, attemptNumber) so BeginUpload propagates the same sentinel
// it did before the split.
func (r *SQLiteAuthReader) LoadAttempt(ctx context.Context, jobID string, attemptNumber int) (status, workerID, leaseID string, err error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT COALESCE(ta.status, ''), COALESCE(ta.worker_id, ''), COALESCE(ta.lease_id, '')
		 FROM task_attempts ta
		 JOIN tasks t ON ta.task_id = t.task_id
		 WHERE t.job_id = ? AND ta.attempt_number = ?`,
		jobID, attemptNumber)
	if scanErr := row.Scan(&status, &workerID, &leaseID); scanErr != nil {
		if errors.Is(scanErr, sql.ErrNoRows) {
			return "", "", "", fmt.Errorf("%w: job=%s n=%d",
				ErrAttemptMismatch, jobID, attemptNumber)
		}
		return "", "", "", fmt.Errorf("artifacts: AuthReader.LoadAttempt: %w", scanErr)
	}
	return status, workerID, leaseID, nil
}

// FindExistingReadyArtifact returns the id of any READY artifact of
// the given kind for the given job, or "" when none exists.
func (r *SQLiteAuthReader) FindExistingReadyArtifact(ctx context.Context, jobID, kind string) (string, error) {
	if jobID == "" || kind == "" {
		return "", fmt.Errorf("artifacts: AuthReader.FindExistingReadyArtifact: empty jobID or kind")
	}
	var id string
	if err := r.db.QueryRowContext(ctx,
		`SELECT id FROM artifacts
		 WHERE job_id = ? AND type = ? AND status = 'READY'
		 LIMIT 1`, jobID, kind).Scan(&id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("artifacts: AuthReader.FindExistingReadyArtifact: %w", err)
	}
	return id, nil
}
