package jobs

import (
	"context"
	"errors"
	"testing"

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
func (s *stubImpl) Fail(ctx context.Context, id, reason string) error { return errNotImplemented }
func (s *stubImpl) Cancel(ctx context.Context, id, reason string, revision int) error {
	return errNotImplemented
}
func (s *stubImpl) Delete(ctx context.Context, id string) error { return errNotImplemented }

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

func TestLifecycleService_Queries_Stubbed(t *testing.T) {
	t.Parallel()
	svc, _ := NewLifecycleService(stubRepo, clock.System{})

	_, err := svc.GetJobsByStatus(context.Background(), StatusPending)
	if !errors.Is(err, errNotImplemented) {
		t.Fatalf("GetJobsByStatus: expected errNotImplemented; got %v", err)
	}
}
