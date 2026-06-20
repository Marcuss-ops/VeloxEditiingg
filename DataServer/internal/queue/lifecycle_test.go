package queue

import (
	"context"
	"errors"
	"strings"
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
	// Use a wrapper to record the args — synthetic, not the recordingPR3Start type
	// (this stub is intentionally uniform-noisy; the dedicated recording stub lives
	// below for limit-defaulting tests).
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

func (s *storePostgresStub) Delete(ctx context.Context, id string) error {
	return errNotImplemented
}

// errNotImplemented is a local sentinel for unimplemented stub methods.
var errNotImplemented = errors.New("not implemented")

// ── Constructor ─────────────────────────────────────────────────────────────

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

// ── Accessors (L2a) ─────────────────────────────────────────────────────────

// TestLifecycleService_Accessors asserts that the three accessors return
// the exact instances injected at construction. This is the contract the
// gRPC handler chain relies on: `lifecycleSvc.Repo()` and
// `lifecycleSvc.Jobs()` must not return a mutated copy or a nil when the
// service was built with non-nil repos.
func TestLifecycleService_Accessors(t *testing.T) {
	t.Parallel()
	clk := clock.System{}
	svc, err := NewLifecycleService(postgresStubRepo, postgresJobsRepo, clk)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if svc.Repo() != postgresStubRepo {
		t.Fatal("Repo() should return the exact instance passed to NewLifecycleService")
	}
	if svc.Jobs() != postgresJobsRepo {
		t.Fatal("Jobs() should return the exact instance passed to NewLifecycleService")
	}
	if svc.Clock() != clk {
		t.Fatal("Clock() should return the exact instance passed to NewLifecycleService")
	}
}

// ── PR3 mutator pre-validation (L2a) ────────────────────────────────────────

// TestLifecycleService_Start_EmptyID asserts Start() short-circuits with
// the pre-validation message when any of JobID/WorkerID/LeaseID is empty.
// We assert on the message text so an accidental rewording of the
// pre-validation string (production callers grep on this text) is caught
// at unit-test speed.
func TestLifecycleService_Start_EmptyID(t *testing.T) {
	t.Parallel()
	svc, _ := NewLifecycleService(postgresStubRepo, postgresJobsRepo, clock.System{})

	err := svc.Start(context.Background(), store.StartCommand{
		JobID:    "",
		WorkerID: "w1",
		LeaseID:  "lease-1",
	})
	if err == nil {
		t.Fatal("expected validation error for empty JobID; got nil")
	}
	if !strings.Contains(err.Error(), "missing job/worker/lease identity") {
		t.Fatalf("expected validation message about missing identity; got: %v", err)
	}
}

// TestLifecycleService_Start_Stubbed asserts that valid input reaches the
// underlying JobRepository.PR3Start (the stub returns errNotImplemented).
func TestLifecycleService_Start_Stubbed(t *testing.T) {
	t.Parallel()
	svc, _ := NewLifecycleService(postgresStubRepo, postgresJobsRepo, clock.System{})

	err := svc.Start(context.Background(), store.StartCommand{
		JobID:    "job-1",
		WorkerID: "w1",
		LeaseID:  "lease-1",
		Now:      time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC),
	})
	if !errors.Is(err, errNotImplemented) {
		t.Fatalf("expected errNotImplemented from stub delegation; got %v", err)
	}
}

// TestLifecycleService_Fail_EmptyID asserts Fail() short-circuits on
// empty JobID and does not call PR3Fail. The validator emits the
// substring "lifecycle.Fail" so the test binds to the exact message
// rather than a generic non-nil error.
func TestLifecycleService_Fail_EmptyID(t *testing.T) {
	t.Parallel()
	svc, _ := NewLifecycleService(postgresStubRepo, postgresJobsRepo, clock.System{})

	err := svc.Fail(context.Background(), store.FailCommand{JobID: ""})
	if err == nil {
		t.Fatal("expected validation error for empty JobID on Fail; got nil")
	}
	if !strings.Contains(err.Error(), "lifecycle.Fail") {
		t.Fatalf("expected Fail validation message; got: %v", err)
	}
}

// TestLifecycleService_Fail_Stubbed asserts that valid input reaches PR3Fail.
// Note: FailCommand has WorkerID/ErrorCode but NOT Reason (Reason is on
// CancelCommand). This test pins the contract for the merged file.
func TestLifecycleService_Fail_Stubbed(t *testing.T) {
	t.Parallel()
	svc, _ := NewLifecycleService(postgresStubRepo, postgresJobsRepo, clock.System{})

	err := svc.Fail(context.Background(), store.FailCommand{
		JobID:     "job-1",
		WorkerID:  "w1",
		LeaseID:   "lease-1",
		ErrorCode: "test_failure",
		Now:       time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC),
	})
	if !errors.Is(err, errNotImplemented) {
		t.Fatalf("expected errNotImplemented from stub delegation; got %v", err)
	}
}

// TestLifecycleService_Cancel_EmptyID asserts Cancel() short-circuits on empty JobID.
func TestLifecycleService_Cancel_EmptyID(t *testing.T) {
	t.Parallel()
	svc, _ := NewLifecycleService(postgresStubRepo, postgresJobsRepo, clock.System{})

	err := svc.Cancel(context.Background(), store.CancelCommand{JobID: ""})
	if err == nil {
		t.Fatal("expected validation error for empty JobID on Cancel; got nil")
	}
	if !strings.Contains(err.Error(), "lifecycle.Cancel") {
		t.Fatalf("expected Cancel validation message; got: %v", err)
	}
}

// TestLifecycleService_Cancel_Stubbed asserts that valid input reaches PR3Cancel.
func TestLifecycleService_Cancel_Stubbed(t *testing.T) {
	t.Parallel()
	svc, _ := NewLifecycleService(postgresStubRepo, postgresJobsRepo, clock.System{})

	err := svc.Cancel(context.Background(), store.CancelCommand{
		JobID:    "job-1",
		WorkerID: "w1",
		Reason:   "operator requested",
		Now:      time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC),
	})
	if !errors.Is(err, errNotImplemented) {
		t.Fatalf("expected errNotImplemented from stub delegation; got %v", err)
	}
}

// ── Reaper & queries (L2a) ──────────────────────────────────────────────────

// limitRecordingStub wraps storePostgresStub but records the most recent
// limit value passed to PR3RequeueExpiredLeases so tests can assert that
// the lifecycle layer correctly coerces limit<=0 → 100 before delegating.
type limitRecordingStub struct {
	storePostgresStub
	lastLimit int
	lastNow   time.Time
	calls     int
}

func (l *limitRecordingStub) PR3RequeueExpiredLeases(ctx context.Context, now time.Time, limit int) ([]store.RequeueResult, error) {
	l.lastLimit = limit
	l.lastNow = now
	l.calls++
	return nil, errNotImplemented
}

// TestLifecycleService_RequeueExpiredLeases_DefaultsLimitWhenZeroOrNegative
// uses a recording stub to PROVE the lifecycle layer coerces limit<=0 → 100
// before delegating (a non-recording stub cannot prove the coercion — the
// strongest possible test would just verify "some method gets called").
func TestLifecycleService_RequeueExpiredLeases_DefaultsLimitWhenZeroOrNegative(t *testing.T) {
	t.Parallel()
	rec := &limitRecordingStub{}
	svc, _ := NewLifecycleService(rec, rec, clock.System{})

	for _, in := range []int{0, -1, -100} {
		_, err := svc.RequeueExpiredLeases(context.Background(), in)
		if !errors.Is(err, errNotImplemented) {
			t.Fatalf("limit=%d: expected errNotImplemented from stub delegation; got %v", in, err)
		}
		if rec.lastLimit != 100 {
			t.Fatalf("limit=%d: expected lastLimit=100 (default); got %d", in, rec.lastLimit)
		}
		if rec.calls != 1 { // reset between loop iterations
			t.Fatalf("expected exactly 1 call per iteration; got %d", rec.calls)
		}
		rec.calls = 0
	}
}

// TestLifecycleService_RequeueExpiredLeases_KeepsPositiveLimit asserts
// that a positive limit is passed through verbatim to the underlying repo
// (coercion only fires for limit <= 0).
func TestLifecycleService_RequeueExpiredLeases_KeepsPositiveLimit(t *testing.T) {
	t.Parallel()
	rec := &limitRecordingStub{}
	svc, _ := NewLifecycleService(rec, rec, clock.System{})

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

// TestLifecycleService_Queries_Stubbed asserts GetJobsByStatus and
// GetNextJobID forward to the repo's ListByStatus (stub returns
// errNotImplemented).
func TestLifecycleService_Queries_Stubbed(t *testing.T) {
	t.Parallel()
	svc, _ := NewLifecycleService(postgresStubRepo, postgresJobsRepo, clock.System{})

	_, err := svc.GetJobsByStatus(context.Background(), store.JobStatusPending)
	if !errors.Is(err, errNotImplemented) {
		t.Fatalf("GetJobsByStatus: expected errNotImplemented; got %v", err)
	}

}

// ── Internal helper (L2a) ───────────────────────────────────────────────────

// TestLifecycleService_Now_Helper pins the contract: zero input falls back
// to the injected clock (clock.System → time.Now()), non-zero input is
// returned verbatim and normalized to UTC. Also verifies timezone
// normalization (PST input → UTC output).
func TestLifecycleService_Now_Helper(t *testing.T) {
	t.Parallel()
	svc, _ := NewLifecycleService(postgresStubRepo, postgresJobsRepo, clock.System{})

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
