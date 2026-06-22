package jobs

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"velox-server/internal/costmodel"
	"velox-server/internal/platform/clock"
)

// stubRepo is a minimal jobs.Repository used for nil validation and delegation tests.
var stubRepo Repository = &stubImpl{}

type stubImpl struct{}

// ── jobs.Reader ─────────────────────────────────────────────────────────────

func (s *stubImpl) Get(ctx context.Context, id string) (*Job, error) { return nil, errNotImplemented }
func (s *stubImpl) List(ctx context.Context, filter Filter) ([]Job, error) {
	return nil, errNotImplemented
}
func (s *stubImpl) Counts(ctx context.Context) (Counts, error) { return nil, errNotImplemented }

// ── jobs.Writer ─────────────────────────────────────────────────────────────
func (s *stubImpl) SetStatus(ctx context.Context, id string, from, to Status) error {
	return errNotImplemented
}
func (s *stubImpl) Lease(ctx context.Context, id, workerID string) error { return errNotImplemented }
func (s *stubImpl) Fail(ctx context.Context, id, reason string) error    { return errNotImplemented }
func (s *stubImpl) Start(ctx context.Context, id, workerID, leaseID string, attempt, revision int) error {
	return errNotImplemented
}
func (s *stubImpl) RenewLease(ctx context.Context, id, workerID, leaseID string, expiry time.Time, emitEvent bool, revision int) error {
	return errNotImplemented
}
func (s *stubImpl) FailWithRetry(ctx context.Context, id, errorCode, errorMessage string, retryable bool, revision int) error {
	return errNotImplemented
}
func (s *stubImpl) Cancel(ctx context.Context, id, reason string, revision int) error {
	return errNotImplemented
}
func (s *stubImpl) RequeueExpiredLeases(ctx context.Context, now time.Time, limit int) ([]RequeueResult, error) {
	return nil, errNotImplemented
}
func (s *stubImpl) ClaimNext(ctx context.Context, workerID string, allowedJobTypes []string) (*ClaimNextResult, error) {
	return nil, errNotImplemented
}
func (s *stubImpl) ReleaseLease(ctx context.Context, id string) error { return errNotImplemented }
func (s *stubImpl) RecordRenderFinished(ctx context.Context, id, workerID, leaseID string, attempt, revision int) error {
	return errNotImplemented
}
func (s *stubImpl) Delete(ctx context.Context, id string) error { return errNotImplemented }

// PR-04.6: cost-rank sibling of ClaimNext. stub returns the sentinel
// so test code that exercises the rank path branches without the rank
// backend in place stays green; the real impl lives on SQLite (and
// Postgres returns ErrNoClaimableJob in Phase 2). Profile parameter
// typed as interface{} so this test stub does not need to import
// costmodel — it never inspects the value.
func (s *stubImpl) ClaimNextForProfile(ctx context.Context, workerID string, allowedJobTypes []string, profile costmodel.WorkerProfile, maxCandidates int) (*ClaimNextResult, error) {
	_ = workerID
	_ = allowedJobTypes
	_ = profile
	_ = maxCandidates
	return nil, errNotImplemented
}

var errNotImplemented = errors.New("not implemented")

// ── Constructor ─────────────────────────────────────────────────────────────

func TestNewLifecycleService_RefusesNilRepository(t *testing.T) {
	t.Parallel()
	_, err := NewLifecycleService(nil, clock.System{})
	if err == nil {
		t.Fatal("expected error when jobsRepo is nil")
	}
}

func TestNewLifecycleService_RefusesNilClock(t *testing.T) {
	t.Parallel()
	_, err := NewLifecycleService(stubRepo, nil)
	if err == nil {
		t.Fatal("expected error when clock is nil")
	}
}

func TestNewLifecycleService_Succeeds(t *testing.T) {
	t.Parallel()
	svc, err := NewLifecycleService(stubRepo, clock.System{})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if svc == nil {
		t.Fatal("expected non-nil LifecycleService")
	}
}

// ── Accessors ───────────────────────────────────────────────────────────────

func TestLifecycleService_Accessors(t *testing.T) {
	t.Parallel()
	clk := clock.System{}
	svc, err := NewLifecycleService(stubRepo, clk)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if svc.Jobs() != stubRepo {
		t.Fatal("Jobs() should return the exact instance passed to NewLifecycleService")
	}
	if svc.Clock() != clk {
		t.Fatal("Clock() should return the exact instance passed to NewLifecycleService")
	}
}

// ── PR3 mutator pre-validation ──────────────────────────────────────────────

func TestLifecycleService_Start_EmptyID(t *testing.T) {
	t.Parallel()
	svc, _ := NewLifecycleService(stubRepo, clock.System{})

	err := svc.Start(context.Background(), "", "w1", "lease-1", 0, 0)
	if err == nil {
		t.Fatal("expected validation error for empty JobID; got nil")
	}
	if !strings.Contains(err.Error(), "missing job/worker/lease identity") {
		t.Fatalf("expected validation message about missing identity; got: %v", err)
	}
}

func TestLifecycleService_Start_Stubbed(t *testing.T) {
	t.Parallel()
	svc, _ := NewLifecycleService(stubRepo, clock.System{})

	err := svc.Start(context.Background(), "job-1", "w1", "lease-1", 1, 5)
	if !errors.Is(err, errNotImplemented) {
		t.Fatalf("expected errNotImplemented from stub delegation; got %v", err)
	}
}

func TestLifecycleService_Queries_Stubbed(t *testing.T) {
	t.Parallel()
	svc, _ := NewLifecycleService(stubRepo, clock.System{})

	_, err := svc.GetJobsByStatus(context.Background(), StatusPending)
	if !errors.Is(err, errNotImplemented) {
		t.Fatalf("GetJobsByStatus: expected errNotImplemented; got %v", err)
	}
}
