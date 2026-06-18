package postgres

import (
	"context"
	"time"

	"velox-server/internal/store"
)

// JobRepository is the Postgres implementation of store.JobRepository
// (spec §5). Currently returns store.ErrNotImplemented until §5b lands a
// pgx-backed driver. The compile-time guard below pins the contract so that
// any drift between this stub and store.JobRepository is caught at build time.
//
// Implementation roadmap per method (referenced verbatim by method name so
// reviewers can grep):
//
//   CreateJob    → INSERT INTO jobs (...) VALUES (..., ?, ...); commit.
//   GetJob       → SELECT job_id, status, … FROM jobs WHERE job_id = $1.
//   ClaimNext    → BEGIN; SELECT … FOR UPDATE SKIP LOCKED LIMIT 1; UPDATE …; INSERT INTO job_attempts; COMMIT.
//   Transition   → UPDATE jobs SET status=$newStatus, revision=revision+1 WHERE job_id=$id AND status=$expected AND revision=$rev;
//                  IF ROW_COUNT = 0: raise store.ErrTransitionConflict, rollback semantic via no-op.
//   ListByStatus → SELECT … FROM jobs WHERE status = ANY($1) ORDER BY updated_at DESC LIMIT $2.
//   StartJob     → CAS via UPDATE jobs SET status='RUNNING', revision=revision+1 WHERE job_id=$1 AND UPPER(status)='LEASED' AND worker_id=$2 AND lease_id=$3 AND attempt=$4 AND revision=$5.
//                  Returns ErrTransitionConflict on predicate mismatch.
//   CompleteJob  → CAS via UPDATE jobs SET status=$finalStatus, completed_at=NOW() WHERE job_id=$1 AND UPPER(status) IN ('RUNNING','LEASED','RETRY_WAIT') AND worker_id=$2 AND lease_id=$3 AND attempt=$4 AND revision=$5.
//                  Returns ErrTransitionConflict on predicate mismatch.
//
// Atomicity stays inside each method; callers never see Begin/Commit.
type JobRepository struct {
	dsn string
}

// NewJobRepository constructs a Postgres-backed JobRepository stub. When the
// pgxpool is wired, the constructor will accept a *pgxpool.Pool and stash it
// in an unexported field; until then the dsn is the only state.
func NewJobRepository(dsn string) *JobRepository {
	return &JobRepository{dsn: dsn}
}

// CreateJob — TODO §5b: see roadmap above.
func (r *JobRepository) CreateJob(ctx context.Context, params store.CreateJobParams) error {
	_, _, _ = ctx, params, r.dsn
	return store.ErrNotImplemented
}

// GetJob returns (nil, nil) on missing AND (nil, ErrNotImplemented) when the
// backend is unavailable — distinguish via errors.Is(err, ErrNotImplemented)
// if you need to fall back to the SQLite mirror.
func (r *JobRepository) GetJob(ctx context.Context, jobID string) (*store.Job, error) {
	_, _, _ = ctx, jobID, r.dsn
	return nil, store.ErrNotImplemented
}

// ClaimNext — TODO §5b: use FOR UPDATE SKIP LOCKED so concurrent workers do
// not contend. See roadmap comment at top of file.
func (r *JobRepository) ClaimNext(ctx context.Context, claim store.ClaimParams) (*store.ClaimResult, error) {
	_, _ = ctx, claim
	_ = r.dsn
	return nil, store.ErrNotImplemented
}

// Transition — TODO §5b: CAS via `status = $expected AND revision = $rev`.
// Raise ErrTransitionConflict when zero rows affected.
func (r *JobRepository) Transition(ctx context.Context, t store.TransitionParams) error {
	_, _, _ = ctx, t, r.dsn
	return store.ErrNotImplemented
}

// ListByStatus — TODO §5b: SELECT ANY(statuses).
func (r *JobRepository) ListByStatus(ctx context.Context, statuses []store.JobStatus, limit int) ([]store.Job, error) {
	_, _, _ = ctx, statuses, limit
	_ = r.dsn
	return nil, store.ErrNotImplemented
}

// RenewLease — TODO §5b: UPDATE jobs SET lease_id=$1, lease_expiry=$2, … WHERE id=$3 AND status IN (…).
// Must raise ErrTransitionConflict when zero rows affected.
func (r *JobRepository) RenewLease(ctx context.Context, params store.RenewLeaseParams) error {
	_, _, _ = ctx, params, r.dsn
	return store.ErrNotImplemented
}

// LeaseJob — TODO §5b: UPDATE jobs SET status='LEASED', lease_id=…, assigned_to=… WHERE id=$1 AND status='PENDING'.
func (r *JobRepository) LeaseJob(ctx context.Context, jobID, workerID string) error {
	_, _, _ = ctx, jobID, workerID
	_ = r.dsn
	return store.ErrNotImplemented
}

// ReleaseClaim — TODO §5b: UPDATE SET status='PENDING' WHERE id=$1 AND status IN ('LEASED','RUNNING').
func (r *JobRepository) ReleaseClaim(ctx context.Context, jobID string) error {
	_, _ = ctx, jobID
	_ = r.dsn
	return store.ErrNotImplemented
}

// RequeueZombieJobs — TODO §5b: UPDATE SET status='PENDING', … WHERE status IN('LEASED','RUNNING') AND lease_expiry < NOW().
func (r *JobRepository) RequeueZombieJobs(ctx context.Context, timeout time.Duration) (int, error) {
	_, _, _ = ctx, timeout, r.dsn
	return 0, store.ErrNotImplemented
}

// UpdateJobResult — TODO §5b: UPDATE jobs SET result_json=$1 WHERE job_id=$2.
func (r *JobRepository) UpdateJobResult(ctx context.Context, jobID string, resultJSON []byte) error {
	_, _, _ = ctx, jobID, resultJSON
	_ = r.dsn
	return store.ErrNotImplemented
}

// StartJob — TODO §5b: CAS via UPDATE jobs SET status='RUNNING', revision=revision+1 WHERE job_id=$1 AND UPPER(status)='LEASED' AND worker_id=$2 AND lease_id=$3 AND attempt=$4 AND revision=$5.
// Returns ErrTransitionConflict on predicate mismatch. Defined here so the
// compile-time JobRepository interface check (`var _ store.JobRepository`)
// passes; the §5b driver swap is the only diff vs the SQLite-side impl.
func (r *JobRepository) StartJob(ctx context.Context, params store.StartJobParams) error {
	_, _, _ = ctx, params, r.dsn
	return store.ErrNotImplemented
}

// CompleteJob — TODO §5b: CAS via UPDATE jobs SET status=$finalStatus, completed_at=NOW() WHERE job_id=$1 AND UPPER(status) IN ('RUNNING','LEASED','RETRY_WAIT') AND worker_id=$2 AND lease_id=$3 AND attempt=$4 AND revision=$5.
// Returns ErrTransitionConflict on predicate mismatch. Defined here so the
// compile-time JobRepository interface check (`var _ store.JobRepository`)
// passes; the §5b driver swap is the only diff vs the SQLite-side impl.
func (r *JobRepository) CompleteJob(ctx context.Context, params store.CompleteJobParams) error {
	_, _, _ = ctx, params, r.dsn
	return store.ErrNotImplemented
}

// Compile-time guard — keeps PostgreSQL implementation honest with the
// SQLite-side contract. PR-1's README promised this; PR-2 delivers it.
var _ store.JobRepository = (*JobRepository)(nil)

// dead-code removed: dsnAccessor() and TimeNow used to expose hooks for §5b's
// test factories and time injection. They were unreachable from production
// paths and would have leaked the DSN via the helper. Reintroduce only when
// the pgx-backed driver lands and the test harness actually needs them.

// ── PR 3 stub implementations ─────────────────────────────────────────────
//
// Each returns ErrNotImplemented until the pgx-backed driver lands. They
// are listed here so the compile-time JobRepository/interface check (`var _
// store.JobRepository` at the bottom of this file) keeps passing after
// the PR 3 surface expansion.

func (r *JobRepository) PR3Start(ctx context.Context, cmd store.StartCommand) error {
	_, _ = ctx, cmd
	_ = r.dsn
	return store.ErrNotImplemented
}

func (r *JobRepository) PR3RenewLease(ctx context.Context, cmd store.RenewLeaseCommand) error {
	_, _ = ctx, cmd
	_ = r.dsn
	return store.ErrNotImplemented
}

func (r *JobRepository) PR3RecordRenderFinished(ctx context.Context, cmd store.RecordRenderFinishedCommand) error {
	_, _ = ctx, cmd
	_ = r.dsn
	return store.ErrNotImplemented
}

func (r *JobRepository) PR3Fail(ctx context.Context, cmd store.FailCommand) error {
	_, _ = ctx, cmd
	_ = r.dsn
	return store.ErrNotImplemented
}

func (r *JobRepository) PR3ScheduleRetry(ctx context.Context, cmd store.RetryCommand) error {
	_, _, _ = ctx, cmd, r.dsn
	return store.ErrNotImplemented
}

func (r *JobRepository) PR3Cancel(ctx context.Context, cmd store.CancelCommand) error {
	_, _, _ = ctx, cmd, r.dsn
	return store.ErrNotImplemented
}

func (r *JobRepository) PR3RequeueExpiredLeases(ctx context.Context, now time.Time, limit int) ([]store.RequeueResult, error) {
	_, _, _ = ctx, now, limit
	_ = r.dsn
	return nil, store.ErrNotImplemented
}

func (r *JobRepository) PR3MarkSucceeded(ctx context.Context, cmd store.MarkSucceededCommand) error {
	_, _, _ = ctx, cmd, r.dsn
	return store.ErrNotImplemented
}
