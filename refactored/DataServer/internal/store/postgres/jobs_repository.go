package postgres

import (
	"context"
	"time"

	"velox-server/internal/store"
)

// JobRepository is the upcoming Postgres implementation of JobRepository
// (spec §5). Scaffolding only — see package doc.
//
// Full PR is deferred (PR-2): the spec calls for CreateJob / ClaimNext /
// Transition / ListByStatus. To avoid forcing package store to expose types
// it doesn't yet own, we declare minimal local types here; PR-2 will move
// them to package store once CreateJobParams / ClaimParams / etc. become
// cross-package contracts.

// CreateJobParams (staging) — final home will be package store; PR-2 will
// re-export these along with the SQLite impl.
type CreateJobParams struct {
	JobID     string
	VideoName string
	ProjectID string
	Payload   map[string]interface{}
}

// ClaimParams (staging).
type ClaimParams struct {
	WorkerID        string
	AllowedJobTypes []string
	Now             time.Time
}

// ClaimResult (staging).
type ClaimResult struct {
	JobID        string
	ResultJSON   []byte
	Attempt      int
	LeaseID      string
	LeaseExpires time.Time
}

// TransitionParams (staging). Models a CAS-style status change.
type TransitionParams struct {
	JobID          string
	ExpectedStatus string
	NewStatus      string
	Revision       int
}

// JobRow (staging). Minimal projection of the jobs table.
type JobRow struct {
	JobID    string
	Status   string
	Revision int
}

// JobRepository is a Postgres-backed placeholder.
type JobRepository struct {
	dsn string
}

// NewJobRepository is a placeholder constructor.
func NewJobRepository(dsn string) *JobRepository { return &JobRepository{dsn: dsn} }

// CreateJob returns ErrNotImplemented.
func (r *JobRepository) CreateJob(ctx context.Context, params CreateJobParams) error {
	_, _ = ctx, params
	return store.ErrNotImplemented
}

// ClaimNext returns ErrNotImplemented.
func (r *JobRepository) ClaimNext(ctx context.Context, claim ClaimParams) (*ClaimResult, error) {
	_, _ = ctx, claim
	return nil, store.ErrNotImplemented
}

// Transition returns ErrNotImplemented.
func (r *JobRepository) Transition(ctx context.Context, t TransitionParams) error {
	_, _ = ctx, t
	return store.ErrNotImplemented
}

// ListByStatus returns ErrNotImplemented.
func (r *JobRepository) ListByStatus(ctx context.Context, statuses []string, limit int) ([]JobRow, error) {
	_, _, _ = ctx, statuses, limit
	return nil, store.ErrNotImplemented
}
