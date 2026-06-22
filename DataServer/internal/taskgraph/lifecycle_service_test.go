package taskgraph

import (
	"context"
	"errors"
	"testing"
	"time"

	"velox-server/internal/taskattempts"
)

// stubRepo is a minimal Repository stub for the RequeueExpiredLeases
// delegation tests. Only the methods exercised by the new tests are wired;
// everything else hits a panic so any accidental call is loud. PR-04
// added AcceptTaskAtomic + TransitionTaskToTerminalAtomic to the Writer
// interface; both are wired here as panics so the stub still satisfies
// the full Repository contract.
type stubRepo struct {
	requeueCalls    int
	requeueNowStr   string
	requeueLimitArg int
	requeueReply    []string
	requeueErr      error
}

func (s *stubRepo) Create(ctx context.Context, t *Task) error               { panic("stubRepo.Create") }
func (s *stubRepo) Get(ctx context.Context, id string) (*Task, error)       { panic("stubRepo.Get") }
func (s *stubRepo) GetByJobID(_ context.Context, _ string) (*Task, error)   { panic("stubRepo.GetByJobID") }
func (s *stubRepo) List(ctx context.Context, f Filter) ([]Task, error)        { panic("stubRepo.List") }
func (s *stubRepo) SetStatus(ctx context.Context, id string, from, to Status, revision int) error {
	panic("stubRepo.SetStatus")
}
func (s *stubRepo) Lease(ctx context.Context, id, workerID, leaseID string) error {
	panic("stubRepo.Lease")
}
func (s *stubRepo) ClaimNextReadyTask(_ context.Context, _, _ string) (*TaskWithSpec, error) {
	panic("stubRepo.ClaimNextReadyTask")
}
func (s *stubRepo) ReleaseLease(_ context.Context, _ string) error {
	panic("stubRepo.ReleaseLease")
}
func (s *stubRepo) Start(ctx context.Context, id, workerID, leaseID string, attempt, revision int) error {
	panic("stubRepo.Start")
}
func (s *stubRepo) Fail(ctx context.Context, id, reason string, revision int) error {
	panic("stubRepo.Fail")
}
func (s *stubRepo) IncrementAttempt(_ context.Context, _ string) error {
	panic("stubRepo.IncrementAttempt")
}
func (s *stubRepo) AreDependenciesSatisfied(ctx context.Context, deps []string) (bool, error) {
	panic("stubRepo.AreDependenciesSatisfied")
}
func (s *stubRepo) AcceptTaskAtomic(_ context.Context, _ *taskattempts.TaskAttempt, _ int) error {
	panic("stubRepo.AcceptTaskAtomic")
}
func (s *stubRepo) TransitionTaskToTerminalAtomic(_ context.Context, _ string, _ string, _ string, _ Status, _ taskattempts.AttemptStatus, _ string, _ string) error {
	panic("stubRepo.TransitionTaskToTerminalAtomic")
}
func (s *stubRepo) Delete(_ context.Context, _ string) error {
	panic("stubRepo.Delete")
}

func (s *stubRepo) RequeueExpiredLeases(_ context.Context, nowStr string, limit int) ([]string, error) {
	s.requeueCalls++
	s.requeueNowStr = nowStr
	s.requeueLimitArg = limit
	return s.requeueReply, s.requeueErr
}

// RenewLease is not exercised by the RequeueExpiredLeases delegation
// tests; a panic keeps the stub surface honest so any accidental
// call from an expanded test fails loud.
func (s *stubRepo) RenewLease(_ context.Context, _, _, _ string, _ time.Time, _ int) error {
	panic("stubRepo.RenewLease: not used in this test scope")
}

// TestLifecycleService_RequeueExpiredLeases_DelegatesToRepo: the
// supervisor-facing wrapper passes through (nowStr, limit) and returns
// whatever the repo returned.
func TestLifecycleService_RequeueExpiredLeases_DelegatesToRepo(t *testing.T) {
	repo := &stubRepo{requeueReply: []string{"t1", "t2"}}
	svc, err := NewLifecycleService(repo)
	if err != nil {
		t.Fatalf("NewLifecycleService: %v", err)
	}
	got, err := svc.RequeueExpiredLeases(context.Background(), "2026-06-22T12:00:00Z", 25)
	if err != nil {
		t.Fatalf("RequeueExpiredLeases: %v", err)
	}
	if repo.requeueCalls != 1 {
		t.Errorf("RequeueExpiredLeases call count: want 1 got %d", repo.requeueCalls)
	}
	if repo.requeueNowStr != "2026-06-22T12:00:00Z" {
		t.Errorf("nowStr not threaded through: got %q", repo.requeueNowStr)
	}
	if repo.requeueLimitArg != 25 {
		t.Errorf("limit not threaded through: got %d", repo.requeueLimitArg)
	}
	if len(got) != 2 || got[0] != "t1" || got[1] != "t2" {
		t.Errorf("returned IDs not propagated: %v", got)
	}
}

// TestLifecycleService_RequeueExpiredLeases_RejectsEmptyNow: nil/empty
// nowRFC3339 is rejected before the repo is touched.
func TestLifecycleService_RequeueExpiredLeases_RejectsEmptyNow(t *testing.T) {
	repo := &stubRepo{}
	svc, _ := NewLifecycleService(repo)
	if _, err := svc.RequeueExpiredLeases(context.Background(), "", 10); err == nil {
		t.Fatalf("empty nowRFC3339 should return error")
	}
	if repo.requeueCalls != 0 {
		t.Errorf("repo should not be called when now is empty: got %d calls", repo.requeueCalls)
	}
}

// TestLifecycleService_RequeueExpiredLeases_DefaultsLimitWhenZeroOrNegative:
// protects against the supervisor forgetting to set a limit.
func TestLifecycleService_RequeueExpiredLeases_DefaultsLimitWhenZeroOrNegative(t *testing.T) {
	for _, in := range []int{0, -1, -1000} {
		repo := &stubRepo{}
		svc, _ := NewLifecycleService(repo)
		if _, err := svc.RequeueExpiredLeases(context.Background(), "2026-06-22T12:00:00Z", in); err != nil {
			t.Fatalf("limit %d: unexpected error %v", in, err)
		}
		if repo.requeueLimitArg != 100 {
			t.Errorf("limit %d did not default to 100: got %d", in, repo.requeueLimitArg)
		}
	}
}

// TestLifecycleService_RequeueExpiredLeases_PropagatesError: repo errors
// surface to the supervisor without being swallowed.
func TestLifecycleService_RequeueExpiredLeases_PropagatesError(t *testing.T) {
	wantErr := errors.New("repo transient")
	repo := &stubRepo{requeueErr: wantErr}
	svc, _ := NewLifecycleService(repo)
	got, err := svc.RequeueExpiredLeases(context.Background(), "2026-06-22T12:00:00Z", 10)
	if !errors.Is(err, wantErr) {
		t.Fatalf("error not propagated: got %v want %v", err, wantErr)
	}
	if got != nil {
		t.Errorf("on error: expected nil slice, got %v", got)
	}
}
