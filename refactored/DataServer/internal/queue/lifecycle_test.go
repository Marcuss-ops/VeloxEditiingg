package queue

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/store"
)

// postgresStubRepo is a minimal JobRepository used only for nil validation tests.
var postgresStubRepo store.JobRepository = &storePostgresStub{}

type storePostgresStub struct{}

func (s *storePostgresStub) CreateJob(ctx context.Context, params store.CreateJobParams) error {
	return store.ErrNotImplemented
}
func (s *storePostgresStub) GetJob(ctx context.Context, jobID string) (*store.Job, error) {
	return nil, store.ErrNotImplemented
}
func (s *storePostgresStub) ClaimNext(ctx context.Context, claim store.ClaimParams) (*store.ClaimResult, error) {
	return nil, store.ErrNotImplemented
}
func (s *storePostgresStub) Transition(ctx context.Context, t store.TransitionParams) error {
	return store.ErrNotImplemented
}
func (s *storePostgresStub) ListByStatus(ctx context.Context, statuses []store.JobStatus, limit int) ([]store.Job, error) {
	return nil, store.ErrNotImplemented
}
func (s *storePostgresStub) RenewLease(ctx context.Context, params store.RenewLeaseParams) error {
	return store.ErrNotImplemented
}
func (s *storePostgresStub) LeaseJob(ctx context.Context, jobID, workerID string) error {
	return store.ErrNotImplemented
}
func (s *storePostgresStub) ReleaseClaim(ctx context.Context, jobID string) error {
	return store.ErrNotImplemented
}
func (s *storePostgresStub) RequeueZombieJobs(ctx context.Context, timeout time.Duration) (int, error) {
	return 0, store.ErrNotImplemented
}
func (s *storePostgresStub) UpdateJobResult(ctx context.Context, jobID string, resultJSON []byte) error {
	return store.ErrNotImplemented
}
func (s *storePostgresStub) CompleteJob(ctx context.Context, params store.CompleteJobParams) error {
	return store.ErrNotImplemented
}
func (s *storePostgresStub) StartJob(ctx context.Context, params store.StartJobParams) error {
	return store.ErrNotImplemented
}
func (s *storePostgresStub) RecordRenderFinished(ctx context.Context, cmd store.RecordRenderFinishedCommand) error {
	return store.ErrNotImplemented
}

func TestNewLifecycleService_RefusesNilRepository(t *testing.T) {
	t.Parallel()
	_, err := NewLifecycleService(nil, nil)
	if err == nil {
		t.Fatal("expected error when repo is nil")
	}
	if err.Error() != "job repository is required" {
		t.Fatalf("expected 'job repository is required', got %q", err.Error())
	}
}

func TestNewLifecycleService_RefusesNilEventStore(t *testing.T) {
	t.Parallel()
	// Non-nil repo, nil eventStore must fail on the second check.
	_, err := NewLifecycleService(postgresStubRepo, nil)
	if err == nil {
		t.Fatal("expected error when eventStore is nil")
	}
	if err.Error() != "event store is required" {
		t.Fatalf("expected 'event store is required', got %q", err.Error())
	}
}

// eventStoreStub implements store.EventStore for testing.
type eventStoreStub struct{}

func (e *eventStoreStub) LogJobEvent(jobID, eventType string, extra map[string]interface{}) error { return nil }
func (e *eventStoreStub) UpdateJobSupplementary(jobID string, fields map[string]interface{}) error { return nil }
func (e *eventStoreStub) AddJobHistory(jobID, status, workerID, message string, extra map[string]interface{}) error { return nil }
func (e *eventStoreStub) AddJobLog(jobID, message, workerID string, isError bool) error { return nil }
func (e *eventStoreStub) SetJobRequest(jobID string, requestJSON []byte) error { return nil }
func (e *eventStoreStub) UpsertJobResult(jobID string, resultJSON []byte) error { return nil }
func (e *eventStoreStub) GetJob(ctx context.Context, jobID string) (map[string]interface{}, error) { return nil, nil }
func (e *eventStoreStub) GetActiveJobs() (map[string]map[string]interface{}, error) { return nil, nil }
func (e *eventStoreStub) JobCounts(ctx context.Context) (map[string]int64, error) { return nil, nil }
func (e *eventStoreStub) ListJobsByStatus(statuses []string, limit int) ([]map[string]interface{}, error) { return nil, nil }
func (e *eventStoreStub) DeleteJob(jobID string) error { return nil }
func (e *eventStoreStub) ArchiveOldJobs(olderThan time.Time) (int64, error) { return 0, nil }
func (e *eventStoreStub) TransitionJobStatus(ctx context.Context, jobID, expected, newStatus string, revision int) (int, error) { return 0, nil }
func (e *eventStoreStub) UpdateArtifactStatus(ctx context.Context, artifactID, status string) error { return nil }
func (e *eventStoreStub) CompleteJobTx(_ context.Context, _ string, _ int64, _ string, _ string, _ int) error { return nil }

var _ store.EventStore = (*eventStoreStub)(nil)

func TestNewLifecycleService_SucceedsWithBothDeps(t *testing.T) {
	t.Parallel()
	svc, err := NewLifecycleService(postgresStubRepo, &eventStoreStub{})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if svc == nil {
		t.Fatal("expected non-nil LifecycleService")
	}
}
