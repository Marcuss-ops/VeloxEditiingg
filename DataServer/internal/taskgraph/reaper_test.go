package taskgraph

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"velox-server/internal/placement"
	"velox-server/internal/taskattempts"
)

// =====================================================================
// PR-05 follow-up: TaskLeaseReaper extracted as a named runner.
// PR-04 follow-up: reaper now calls LifecycleService.ExpireTaskLease
// per candidate, so the reaper_test surface is split: countingRepo
// stubs ExpireTaskLeaseAtomic on the Repository surface (no candidates
// iterates through the real LifecycleService path).
// =====================================================================

// countingRepo records each RequeueExpiredLeases call's nowStr argument
// and reply slice. Used to assert tick cadence without depending on
// the SQLite layer.
type countingRepo struct {
	mu        sync.Mutex
	calls     int
	lastNow   string
	reply     []RequeueCandidate
	errOnCall *int // if non-nil and matches calls-1, return err.
}

func (c *countingRepo) Get(_ context.Context, _ string) (*Task, error)        { panic("not used") }
func (c *countingRepo) List(_ context.Context, _ Filter) ([]Task, error)      { panic("not used") }
func (c *countingRepo) GetByJobID(_ context.Context, _ string) (*Task, error) { panic("not used") }
func (c *countingRepo) Create(_ context.Context, _ *Task) error               { panic("not used") }
func (c *countingRepo) SetStatus(_ context.Context, _ string, _, _ Status, _ int) error {
	panic("not used")
}
func (c *countingRepo) Lease(_ context.Context, _, _, _ string) error        { panic("not used") }
func (c *countingRepo) ReleaseLease(_ context.Context, _, _, _ string) error { panic("not used") }
func (c *countingRepo) ClaimNextReadyTask(_ context.Context, _, _ string) (*TaskWithSpec, error) {
	panic("not used")
}
func (c *countingRepo) Start(_ context.Context, _, _, _ string, _, _ int) error { panic("not used") }
func (c *countingRepo) RequeueExpiredLeases(_ context.Context, nowStr string, _ int) ([]RequeueCandidate, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	c.lastNow = nowStr
	if c.errOnCall != nil && *(c.errOnCall) == c.calls {
		return nil, errRepo
	}
	return c.reply, nil
}
func (c *countingRepo) ExpireTaskLeaseAtomic(_ context.Context, _, _, _ string, _ int) (ExpireResult, error) {
	// Default stub: reply with a "no-op" result so callers see success
	// without asserting any specific rows mutated.
	return ExpireResult{}, nil
}
func (c *countingRepo) AcceptTaskAtomic(_ context.Context, _ *taskattempts.TaskAttempt, _ int) error {
	panic("not used")
}
func (c *countingRepo) AreDependenciesSatisfied(_ context.Context, _ []string) (bool, error) {
	panic("not used")
}
func (c *countingRepo) Fail(_ context.Context, _, _ string, _ int) error   { panic("not used") }
func (c *countingRepo) IncrementAttempt(_ context.Context, _ string) error { panic("not used") }
func (c *countingRepo) TransitionTaskToTerminalAtomic(
	_ context.Context, _, _, _ string, _ Status,
	_ taskattempts.AttemptStatus, _, _ string,
) error {
	panic("not used")
}
func (c *countingRepo) Delete(_ context.Context, _ string) error { panic("not used") }
func (c *countingRepo) RenewLease(_ context.Context, _, _, _ string, _ time.Time, _ int) error {
	panic("not used")
}
func (c *countingRepo) IngestTaskResultAtomic(_ context.Context, _ IngestResultCommand) error {
	panic("not used")
}

// ClaimNextWithAttemptAtomic is the PR-2 (canonical-attempt-identity)
// sibling of ClaimNextReadyTask; the reaper test reuses countingRepo
// against LifecycleService-only paths, so this is a panic-on-call
// sentinel — any accidental fan-out into the new path from an
// expanded test fails loud rather than silently returning zero
// values.
func (c *countingRepo) ClaimNextWithAttemptAtomic(_ context.Context, _, _ string) (*TaskWithSpec, *taskattempts.TaskAttempt, error) {
	panic("countingRepo.ClaimNextWithAttemptAtomic: not used in this test scope")
}

func (c *countingRepo) ListReadyCandidates(_ context.Context, _ int) ([]placement.TaskCandidate, error) {
	panic("countingRepo.ListReadyCandidates: not used in this test scope")
}

func (c *countingRepo) ClaimTaskForWorkerAtomic(_ context.Context, _ ClaimTaskForWorkerCommand) (*TaskWithSpec, *taskattempts.TaskAttempt, error) {
	panic("countingRepo.ClaimTaskForWorkerAtomic: not used in this test scope")
}

var errRepo = errors.New("fake repo error")

// stubReaperLifecycle wires a real LifecycleService to a countingRepo
// so the TickReadiness delegation tests still pass against the renumbered
// surface. The reaper tests that exercise Run() via LifecycleService now
// need to pass *LifecycleService directly.
func stubReaperLifecycle(repo *countingRepo) *LifecycleService {
	lc, _ := NewLifecycleService(repo)
	return lc
}

// TestTaskLeaseReaper_ConstructionDefaults: zero-value ticker / limit
// get safe defaults.
func TestTaskLeaseReaper_ConstructionDefaults(t *testing.T) {
	repo := &countingRepo{}
	lc := stubReaperLifecycle(repo)
	r := NewTaskLeaseReaperWithConfig(lc, 0, 0)
	if r == nil {
		t.Fatal("NewTaskLeaseReaperWithConfig returned nil")
	}
	if r.ticker != 30*time.Second {
		t.Errorf("default ticker = %v; want 30s", r.ticker)
	}
	if r.limit != 100 {
		t.Errorf("default limit = %d; want 100", r.limit)
	}
}

// TestTaskLeaseReaper_NilLifecyclePanics: nil lifecycle is a programming
// error — the constructor fails loud rather than returning a reaper
// that silently no-ops.
func TestTaskLeaseReaper_NilLifecyclePanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on nil lifecycle")
		}
	}()
	_ = NewTaskLeaseReaperWithConfig(nil, 30*time.Second, 100)
}

// TestTaskLeaseReaper_RunTicksUntilContextCancel: with a sub-second
// ticker, the reaper fires multiple times before the test cancels
// its context. Assert the called count is >=2 (>=1 is race-tricky).
func TestTaskLeaseReaper_RunTicksUntilContextCancel(t *testing.T) {
	repo := &countingRepo{}
	lc := stubReaperLifecycle(repo)
	r := NewTaskLeaseReaperWithConfig(lc, 5*time.Millisecond, 50)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	// Let the reaper fire a few times.
	time.Sleep(40 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Errorf("Run should return ctx.Err() on cancel, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.calls < 2 {
		t.Errorf("expected at least 2 sweep ticks before cancel; got %d", repo.calls)
	}
	if repo.lastNow == "" {
		t.Error("lastNow was never populated; reaper never called repo")
	}
}

// TestTaskLeaseReaper_RepoErrorDoesNotKillLoop: a transient repo error
// on one tick must NOT terminate Run — the next tick should still fire.
func TestTaskLeaseReaper_RepoErrorDoesNotKillLoop(t *testing.T) {
	repo := &countingRepo{}
	// Inject a fake error on call #2. Calls >=3 succeed.
	failOn := 2
	repo.errOnCall = &failOn

	lc := stubReaperLifecycle(repo)
	r := NewTaskLeaseReaperWithConfig(lc, 5*time.Millisecond, 50)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	time.Sleep(40 * time.Millisecond)
	cancel()
	<-done

	repo.mu.Lock()
	defer repo.mu.Unlock()
	// Three or more ticks should have completed: pre-fail, fail, post-fail.
	if repo.calls < 3 {
		t.Errorf("expected at least 3 sweep ticks (incl. one with repo error); got %d", repo.calls)
	}
}

// TestTaskLeaseReaper_SetClockReplacesNow: SetClock injection lets tests
// run deterministic time. We pin now() to a fixed value and assert the
// repo receives that exact RFC3339 string.
func TestTaskLeaseReaper_SetClockReplacesNow(t *testing.T) {
	repo := &countingRepo{}
	lc := stubReaperLifecycle(repo)
	r := NewTaskLeaseReaperWithConfig(lc, 5*time.Millisecond, 50)

	fixed := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	r.SetClock(func() time.Time { return fixed })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	repo.mu.Lock()
	defer repo.mu.Unlock()
	wantNow := fixed.Format(time.RFC3339)
	if repo.lastNow != wantNow {
		t.Errorf("lastNow = %q; want %q (SetClock should have pinned time)", repo.lastNow, wantNow)
	}
}

// TestTaskLeaseReaper_SetClockNilNoOp: passing nil to SetClock must
// leave the previous clock function untouched (no nil dereference
// panic on the next tick).
func TestTaskLeaseReaper_SetClockNilNoOp(t *testing.T) {
	repo := &countingRepo{}
	lc := stubReaperLifecycle(repo)
	r := NewTaskLeaseReaperWithConfig(lc, 5*time.Millisecond, 50)
	r.SetClock(nil) // must not panic

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	time.Sleep(15 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.calls < 1 {
		t.Error("expected at least 1 tick")
	}
}
