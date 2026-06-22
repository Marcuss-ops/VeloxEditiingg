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

func (s *stubImpl) Create(ctx context.Context, job *Job) error { return errNotImplemented }
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

func TestLifecycleService_Fail_EmptyID(t *testing.T) {
	t.Parallel()
	svc, _ := NewLifecycleService(stubRepo, clock.System{})

	err := svc.Fail(context.Background(), "", "err", "msg", true, 0)
	if err == nil {
		t.Fatal("expected validation error for empty JobID on Fail; got nil")
	}
	if !strings.Contains(err.Error(), "lifecycle.Fail") {
		t.Fatalf("expected Fail validation message; got: %v", err)
	}
}

func TestLifecycleService_Fail_Stubbed(t *testing.T) {
	t.Parallel()
	svc, _ := NewLifecycleService(stubRepo, clock.System{})

	err := svc.Fail(context.Background(), "job-1", "test_failure", "test message", true, 0)
	if !errors.Is(err, errNotImplemented) {
		t.Fatalf("expected errNotImplemented from stub delegation; got %v", err)
	}
}

func TestLifecycleService_Cancel_EmptyID(t *testing.T) {
	t.Parallel()
	svc, _ := NewLifecycleService(stubRepo, clock.System{})

	err := svc.Cancel(context.Background(), "", "reason", 0)
	if err == nil {
		t.Fatal("expected validation error for empty JobID on Cancel; got nil")
	}
	if !strings.Contains(err.Error(), "lifecycle.Cancel") {
		t.Fatalf("expected Cancel validation message; got: %v", err)
	}
}

func TestLifecycleService_Cancel_Stubbed(t *testing.T) {
	t.Parallel()
	svc, _ := NewLifecycleService(stubRepo, clock.System{})

	err := svc.Cancel(context.Background(), "job-1", "operator requested", 3)
	if !errors.Is(err, errNotImplemented) {
		t.Fatalf("expected errNotImplemented from stub delegation; got %v", err)
	}
}

// ── Reaper & queries ────────────────────────────────────────────────────────

type limitRecordingStub struct {
	stubImpl
	lastLimit int
	lastNow   time.Time
	calls     int
}

func (l *limitRecordingStub) RequeueExpiredLeases(ctx context.Context, now time.Time, limit int) ([]RequeueResult, error) {
	l.lastLimit = limit
	l.lastNow = now
	l.calls++
	return nil, errNotImplemented
}

func TestLifecycleService_RequeueExpiredLeases_DefaultsLimitWhenZeroOrNegative(t *testing.T) {
	t.Parallel()
	rec := &limitRecordingStub{}
	svc, _ := NewLifecycleService(rec, clock.System{})

	for _, in := range []int{0, -1, -100} {
		_, err := svc.RequeueExpiredLeases(context.Background(), in)
		if !errors.Is(err, errNotImplemented) {
			t.Fatalf("limit=%d: expected errNotImplemented from stub delegation; got %v", in, err)
		}
		if rec.lastLimit != 100 {
			t.Fatalf("limit=%d: expected lastLimit=100 (default); got %d", in, rec.lastLimit)
		}
		if rec.calls != 1 {
			t.Fatalf("expected exactly 1 call per iteration; got %d", rec.calls)
		}
		rec.calls = 0
	}
}

func TestLifecycleService_RequeueExpiredLeases_KeepsPositiveLimit(t *testing.T) {
	t.Parallel()
	rec := &limitRecordingStub{}
	svc, _ := NewLifecycleService(rec, clock.System{})

	_, err := svc.RequeueExpiredLeases(context.Background(), 42)
	if !errors.Is(err, errNotImplemented) {
		t.Fatalf("got non-stub error: %v", err)
	}
	if rec.lastLimit != 42 {
		t.Fatalf("positive limit must be preserved; got %d", rec.lastLimit)
	}
	if rec.calls != 1 {
		t.Fatalf("expected exactly 1 call; got %d", rec.calls)
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

// ── Internal helper ─────────────────────────────────────────────────────────

func TestLifecycleService_Now_Helper(t *testing.T) {
	t.Parallel()
	svc, _ := NewLifecycleService(stubRepo, clock.System{})

	// non-zero: returned verbatim and UTC-normalized
	pst := time.FixedZone("PST", -8*3600)
	in := time.Date(2026, 6, 20, 12, 0, 0, 0, pst)
	got := svc.now(in)
	if !got.Equal(in) {
		t.Fatalf("non-zero input value not preserved: in=%v got=%v", in, got)
	}
	if got.Location() != time.UTC {
		t.Fatalf("non-zero input not UTC-normalized: loc=%v", got.Location())
	}

	// zero: falls back to clock.Now(), UTC-normalized
	got = svc.now(time.Time{})
	if got.IsZero() {
		t.Fatal("zero input should fall back to clock.Now(); got zero time")
	}
	if got.Location() != time.UTC {
		t.Fatalf("zero input not UTC-normalized: loc=%v", got.Location())
	}
}

// ── PR-13: Job-side reaper gate (post-048) ─────────────────────────────────

func TestLifecycleService_ReaperDisabled_DefaultsFalse(t *testing.T) {
	t.Parallel()
	svc, _ := NewLifecycleService(stubRepo, clock.System{})
	if svc.ReaperDisabled() {
		t.Fatal("ReaperDisabled() should default to false")
	}
}

func TestLifecycleService_DisableReaper_FlipsFlagIdempotent(t *testing.T) {
	t.Parallel()
	svc, _ := NewLifecycleService(stubRepo, clock.System{})
	svc.DisableReaper()
	if !svc.ReaperDisabled() {
		t.Fatal("ReaperDisabled() must be true after DisableReaper()")
	}
	// Second call is a no-op (idempotent).
	svc.DisableReaper()
	if !svc.ReaperDisabled() {
		t.Fatal("DisableReaper() must remain idempotent on repeated calls")
	}
}

func TestLifecycleService_RequeueExpiredLeasesSafe_NoOpsWhenDisabled(t *testing.T) {
	t.Parallel()
	rec := &limitRecordingStub{}
	svc, _ := NewLifecycleService(rec, clock.System{})
	svc.DisableReaper()

	results, err := svc.RequeueExpiredLeasesSafe(context.Background(), 42)
	if err != nil {
		t.Fatalf("Safe variant should not error when disabled; got %v", err)
	}
	if results != nil {
		t.Fatalf("Safe variant should return nil results when disabled; got %v", results)
	}
	if rec.calls != 0 {
		t.Fatalf("Safe variant must not invoke the underlying repo when disabled; calls=%d", rec.calls)
	}
	if rec.lastLimit != 0 {
		t.Fatalf("Safe variant must not record limit when disabled; lastLimit=%d", rec.lastLimit)
	}
}

func TestLifecycleService_RequeueExpiredLeasesSafe_DelegatesWhenEnabled(t *testing.T) {
	t.Parallel()
	rec := &limitRecordingStub{}
	svc, _ := NewLifecycleService(rec, clock.System{})
	// ReaperDisabled() == false (default).

	if _, err := svc.RequeueExpiredLeasesSafe(context.Background(), 42); !errors.Is(err, errNotImplemented) {
		t.Fatalf("expected errNotImplemented from stub delegation; got %v", err)
	}
	if rec.calls != 1 {
		t.Fatalf("Safe variant must delegate to repo when enabled; calls=%d", rec.calls)
	}
	if rec.lastLimit != 42 {
		t.Fatalf("Safe variant must propagate limit to repo; lastLimit=%d", rec.lastLimit)
	}
}

// TestJobReaper_DisabledPost048 is the integration-level assertion
// required by PR-13. It guarantees the supervisor goroutine never
// invokes the underlying repo once DisableReaper() has been called,
// regardless of how many tick iterations elapse.
func TestJobReaper_DisabledPost048(t *testing.T) {
	t.Parallel()
	rec := &limitRecordingStub{}
	svc, _ := NewLifecycleService(rec, clock.System{})
	svc.DisableReaper()

	for tick := 0; tick < 10; tick++ {
		results, err := svc.RequeueExpiredLeasesSafe(context.Background(), 100)
		if err != nil {
			t.Fatalf("tick=%d: unexpected error from Safe variant: %v", tick, err)
		}
		if results != nil {
			t.Fatalf("tick=%d: expected nil results when reaper disabled; got %v", tick, results)
		}
	}
	if rec.calls != 0 {
		t.Fatalf("after 10 ticks with disabled reaper, repo calls=%d (want 0)", rec.calls)
	}
	if rec.lastLimit != 0 {
		t.Fatalf("after 10 disabled ticks, lastLimit=%d (want 0 — limit must not leak)", rec.lastLimit)
	}
}
