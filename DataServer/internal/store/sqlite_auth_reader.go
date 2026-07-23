package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// JobState captures the auth-relevant columns of the jobs row.
// Lives in the store package because it is the projection returned
// by the SQLiteAuthReader adapter; the consumer-owned AuthReader
// interface in internal/artifacts aliases this type so the consumer
// contract stays close to the domain while avoiding an import cycle.
type JobState struct {
	Status   string
	Revision int
}

// ErrAuthAttemptMismatch is returned by SQLiteAuthReader.LoadAttempt when
// the requested (job_id, attempt_number) pair cannot be found. The sentinel
// is aliased as artifacts.ErrAttemptMismatch so existing callers that use
// errors.Is(err, artifacts.ErrAttemptMismatch) keep matching.
var ErrAuthAttemptMismatch = errors.New("store: auth attempt mismatch")

// SQLiteAuthReader is the production implementation of the consumer-owned
// AuthReader port on top of *sql.DB.
//
// The interface itself lives in the consumer package (internal/artifacts)
// so the consumer owns the contract. This package does not import that
// consumer package to avoid an import cycle; instead, a local anonymous
// interface provides a compile-time assertion that the concrete type
// satisfies the contract's method shape (structural interface matching).
type SQLiteAuthReader struct {
	db *sql.DB
}

// NewSQLiteAuthReader constructs a SQLiteAuthReader. db must outlive
// the reader.
func NewSQLiteAuthReader(db *sql.DB) *SQLiteAuthReader {
	if db == nil {
		panic("store: NewSQLiteAuthReader requires a non-nil *sql.DB")
	}
	return &SQLiteAuthReader{db: db}
}

// Compile-time assertion using an anonymous interface. The consumer-owned
// port in internal/artifacts has the identical method signatures, so
// SQLiteAuthReader satisfies it structurally without forcing an import cycle.
var _ interface {
	LoadJob(ctx context.Context, jobID string) (*JobState, error)
	LoadAttempt(ctx context.Context, jobID string, attemptNumber int) (status, workerID, leaseID string, err error)
	FindExistingReadyArtifact(ctx context.Context, jobID, kind string) (string, error)
} = (*SQLiteAuthReader)(nil)

// LoadJob reads auth-relevant columns of the `jobs` row.
func (r *SQLiteAuthReader) LoadJob(ctx context.Context, jobID string) (*JobState, error) {
	if jobID == "" {
		return nil, fmt.Errorf("store: AuthReader.LoadJob: empty jobID")
	}
	row := r.db.QueryRowContext(ctx,
		`SELECT status, COALESCE(revision, 0) FROM jobs WHERE job_id = ?`, jobID)
	var j JobState
	if err := row.Scan(&j.Status, &j.Revision); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("store: AuthReader.LoadJob: %w", err)
	}
	return &j, nil
}

// LoadAttempt reads auth-relevant columns of the task_attempts row
// (joined to tasks by job_id).
func (r *SQLiteAuthReader) LoadAttempt(ctx context.Context, jobID string, attemptNumber int) (status, workerID, leaseID string, err error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT COALESCE(ta.status, ''), COALESCE(ta.worker_id, ''), COALESCE(ta.lease_id, '')
		 FROM task_attempts ta
		 JOIN tasks t ON ta.task_id = t.task_id
		 WHERE t.job_id = ? AND ta.attempt_number = ?`,
		jobID, attemptNumber)
	if scanErr := row.Scan(&status, &workerID, &leaseID); scanErr != nil {
		if errors.Is(scanErr, sql.ErrNoRows) {
			return "", "", "", fmt.Errorf("%w: job=%s n=%d", ErrAuthAttemptMismatch, jobID, attemptNumber)
		}
		return "", "", "", fmt.Errorf("store: AuthReader.LoadAttempt: %w", scanErr)
	}
	return status, workerID, leaseID, nil
}

// FindExistingReadyArtifact returns the id of any READY artifact of
// the given kind for the given job, or "" when none exists.
func (r *SQLiteAuthReader) FindExistingReadyArtifact(ctx context.Context, jobID, kind string) (string, error) {
	if jobID == "" || kind == "" {
		return "", fmt.Errorf("store: AuthReader.FindExistingReadyArtifact: empty jobID or kind")
	}
	var id string
	if err := r.db.QueryRowContext(ctx,
		`SELECT id FROM artifacts
		 WHERE job_id = ? AND type = ? AND status = 'READY'
		 LIMIT 1`, jobID, kind).Scan(&id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("store: AuthReader.FindExistingReadyArtifact: %w", err)
	}
	return id, nil
}
