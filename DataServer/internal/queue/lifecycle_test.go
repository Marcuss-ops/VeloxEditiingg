package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"velox-server/internal/jobs"
	"velox-server/internal/platform/clock"
	"velox-server/internal/store"
)

// postgresStubRepo is a minimal store.JobRepository used for nil validation tests.
var postgresStubRepo store.JobRepository = &storePostgresStub{}

// postgresJobsRepo is the same stub satisfying jobs.Repository for dual-stack tests.
var postgresJobsRepo jobs.Repository = &storePostgresStub{}

type storePostgresStub struct{}

func (s *storePostgresStub) CreateJob(ctx context.Context, params store.CreateJobParams) error {
	return errNotImplemented
}
func (s *storePostgresStub) GetJob(ctx context.Context, jobID string) (*store.Job, error) {
	return nil, errNotImplemented
}
func (s *storePostgresStub) ClaimNext(ctx context.Context, claim store.ClaimParams) (*store.ClaimResult, error) {
	return nil, errNotImplemented
}
func (s *storePostgresStub) Transition(ctx context.Context, t store.TransitionParams) error {
	return errNotImplemented
}
func (s *storePostgresStub) ListByStatus(ctx context.Context, statuses []store.JobStatus, limit int) ([]store.Job, error) {
	return nil, errNotImplemented
}
func (s *storePostgresStub) RenewLease(ctx context.Context, params store.RenewLeaseParams) error {
	return errNotImplemented
}
func (s *storePostgresStub) LeaseJob(ctx context.Context, jobID, workerID string) error {
	return errNotImplemented
}
func (s *storePostgresStub) ReleaseClaim(ctx context.Context, jobID string) error {
	return errNotImplemented
}
func (s *storePostgresStub) RequeueZombieJobs(ctx context.Context, timeout time.Duration) (int, error) {
	return 0, errNotImplemented
}
func (s *storePostgresStub) CompleteJob(ctx context.Context, params store.CompleteJobParams) error {
	return errNotImplemented
}
func (s *storePostgresStub) StartJob(ctx context.Context, params store.StartJobParams) error {
	return errNotImplemented
}
func (s *storePostgresStub) RecordRenderFinished(ctx context.Context, cmd store.RecordRenderFinishedCommand) error {
	return errNotImplemented
}
func (s *storePostgresStub) PR3Start(ctx context.Context, cmd store.StartCommand) error {
	return errNotImplemented
}
func (s *storePostgresStub) PR3RenewLease(ctx context.Context, cmd store.RenewLeaseCommand) error {
	return errNotImplemented
}
func (s *storePostgresStub) PR3RecordRenderFinished(ctx context.Context, cmd store.RecordRenderFinishedCommand) error {
	return errNotImplemented
}
func (s *storePostgresStub) PR3Fail(ctx context.Context, cmd store.FailCommand) error {
	return errNotImplemented
}
func (s *storePostgresStub) PR3ScheduleRetry(ctx context.Context, cmd store.RetryCommand) error {
	return errNotImplemented
}
func (s *storePostgresStub) PR3Cancel(ctx context.Context, cmd store.CancelCommand) error {
	return errNotImplemented
}
func (s *storePostgresStub) PR3RequeueExpiredLeases(ctx context.Context, now time.Time, limit int) ([]store.RequeueResult, error) {
	return nil, errNotImplemented
}

// ── jobs.Reader ─────────────────────────────────────────────────────────────

func (s *storePostgresStub) Get(ctx context.Context, id string) (*jobs.Job, error) {
	return nil, errNotImplemented
}
func (s *storePostgresStub) List(ctx context.Context, filter jobs.Filter) ([]jobs.Job, error) {
	return nil, errNotImplemented
}
func (s *storePostgresStub) Counts(ctx context.Context) (jobs.Counts, error) {
	return nil, errNotImplemented
}

// ── jobs.Writer ─────────────────────────────────────────────────────────────

func (s *storePostgresStub) Create(ctx context.Context, job *jobs.Job) error {
	return errNotImplemented
}
func (s *storePostgresStub) SetStatus(ctx context.Context, id string, from, to jobs.Status) error {
	return errNotImplemented
}
func (s *storePostgresStub) Lease(ctx context.Context, id, workerID string) error {
	return errNotImplemented
}
func (s *storePostgresStub) Fail(ctx context.Context, id, reason string) error {
	return errNotImplemented
}

// errNotImplemented is a local sentinel for unimplemented stub methods.
var errNotImplemented = errors.New("not implemented")

func TestNewLifecycleService_RefusesNilRepository(t *testing.T) {
	t.Parallel()
	_, err := NewLifecycleService(nil, nil, clock.System{})
	if err == nil {
		t.Fatal("expected error when repo is nil")
	}
}

func TestNewLifecycleService_RefusesNilClock(t *testing.T) {
	t.Parallel()
	_, err := NewLifecycleService(postgresStubRepo, postgresJobsRepo, nil)
	if err == nil {
		t.Fatal("expected error when clock is nil")
	}
}

func TestNewLifecycleService_Succeeds(t *testing.T) {
	t.Parallel()
	svc, err := NewLifecycleService(postgresStubRepo, postgresJobsRepo, clock.System{})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if svc == nil {
		t.Fatal("expected non-nil LifecycleService")
	}
}
