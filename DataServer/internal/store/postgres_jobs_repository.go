// Package store / postgres_jobs_repository.go
//
// Postgres-side implementation of jobs.Repository.  Writer methods and
// Reader methods (Get / List / Counts) are inherited from the embedded
// baseJobRepository (via the postgresDialect which uses $n placeholders
// and no-op audit hooks).
//
// ClaimNext uses UPDATE … RETURNING (Postgres-specific).

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"velox-server/internal/costmodel"
	"velox-server/internal/jobs"
	"velox-server/internal/platform/database"
)

// PostgresJobRepository implements jobs.Repository on a *database.Handle.
type PostgresJobRepository struct {
	baseJobRepository
	handle *database.Handle
}

var _ jobs.Repository = (*PostgresJobRepository)(nil)

// NewPostgresJobRepository wraps a *database.Handle as a jobs.Repository.
func NewPostgresJobRepository(handle *database.Handle) *PostgresJobRepository {
	return &PostgresJobRepository{
		baseJobRepository: baseJobRepository{
			db:      handle.DB,
			dialect: postgresDialect{},
		},
		handle: handle,
	}
}

// ── ClaimNext (Postgres-specific: UPDATE … RETURNING) ──────────────────

func (r *PostgresJobRepository) ClaimNext(ctx context.Context, workerID string, allowedJobTypes []string) (*jobs.ClaimNextResult, error) {
	if workerID == "" {
		return nil, fmt.Errorf("postgres jobs: empty workerID in ClaimNext")
	}
	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339)

	tx, err := r.handle.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("postgres jobs: ClaimNext begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	row := tx.QueryRowContext(ctx,
		`UPDATE jobs
		   SET status = 'LEASED',
		       assigned_at = $1,
		       claimed_at = $1,
		       updated_at = $1,
		       revision = COALESCE(revision, 0) + 1
		 WHERE job_id = (
		     SELECT job_id FROM jobs
		     WHERE UPPER(COALESCE(status, '')) = 'PENDING'
		     ORDER BY COALESCE(updated_at, created_at) ASC
		     FOR UPDATE SKIP LOCKED
		     LIMIT 1
		 )
		 RETURNING job_id, COALESCE(attempt, 0)`,
		nowStr)

	var jobID string
	var attemptReturned int
	if err := row.Scan(&jobID, &attemptReturned); err != nil {
		if errors.Is(err, sql.ErrNoRows) || errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNoClaimableJob
		}
		return nil, fmt.Errorf("postgres jobs: ClaimNext scan: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("postgres jobs: ClaimNext commit: %w", err)
	}
	_ = allowedJobTypes
	return &jobs.ClaimNextResult{
		JobID: jobID, Attempt: attemptReturned,
		LeaseID: "", LeaseExpires: now.Add(30 * time.Minute),
	}, nil
}

func (r *PostgresJobRepository) ClaimNextForProfile(
	ctx context.Context, workerID string, allowedJobTypes []string,
	profile costmodel.WorkerProfile, maxCandidates int,
) (*jobs.ClaimNextResult, error) {
	_ = workerID
	_ = allowedJobTypes
	_ = profile
	_ = maxCandidates
	return nil, ErrNoClaimableJob
}
