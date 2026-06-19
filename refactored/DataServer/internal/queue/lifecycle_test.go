package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"velox-server/internal/store"
)

// postgresStubRepo is a minimal JobRepository used only for nil validation tests.
var postgresStubRepo store.JobRepository = &storePostgresStub{}

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

// errNotImplemented is a local sentinel for unimplemented stub methods.
var errNotImplemented = errors.New("not implemented")

func TestNewLifecycleService_RefusesNilRepository(t *testing.T) {
	t.Parallel()
	_, err := NewLifecycleService(nil, RealClock{})
	if err == nil {
		t.Fatal("expected error when repo is nil")
	}
}

func TestNewLifecycleService_RefusesNilClock(t *testing.T) {
	t.Parallel()
	_, err := NewLifecycleService(postgresStubRepo, nil)
	if err == nil {
		t.Fatal("expected error when clock is nil")
	}
}

func TestNewLifecycleService_Succeeds(t *testing.T) {
	t.Parallel()
	svc, err := NewLifecycleService(postgresStubRepo, RealClock{})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if svc == nil {
		t.Fatal("expected non-nil LifecycleService")
	}
}
