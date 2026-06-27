package taskgraph

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"velox-server/internal/jobs"
	"velox-server/internal/taskattempts"
)

// =====================================================================
// ExpireTaskLease end-to-end coverage (audit §P0.4 + §P0.6 closure).
//
// The job-aggregate wire-up in LifecycleService::ExpireTaskLease runs
// OUTSIDE the atomic (post-commit). When ExpireTaskLeaseAtomic replies
// with AttemptsExhausted=true, the service must call
// JobsRetryQuerier.FailWithRetry exactly once with the canonical
// LEASE_EXPIRED tuple so the Job aggregate sees the FAILED-attempt
// count. The stub fixture below records FailWithRetry arguments so
// the assertion can pin every field (errorCode, errorMessage,
// retryable, revision) of the audit-mandated tuple.
//
// Untested variants (deliberately out of scope here, exercised in
// the SQLite reaper tests):
//
//   * lease_id mismatch at the atomic layer (sqlite_task_reaper_test)
//   * lease_expires_at mismatch at the atomic layer (P0#6; see the
//     TestExpireTaskLeaseAtomic_StaleExpiryReturnsConflict case in
//     sqlite_task_reaper_test.go)
//   * status-not-LEASED/RUNNING mismatch
//
// These are SQL-gate rejections; the lifecycle wrapper just wraps
// their ErrTransitionConflict. The audit's end-to-end check is the
// FailWithRetry propagation captured below.
// =====================================================================

// recordingJobsRepo is a JobsRetryQuerier stub that records the
// (args, callCount) of every FailWithRetry invocation. Get returns
// the seeded Job (or nil to force the no-FailWithRetry-branch path).
// Satisfies JobsRetryQuerier so SetJobsRepo can wire it directly.
//
// Concurrency: LifecycleService.ExpireTaskLease runs FailWithRetry
// synchronously in the test scope, so no mutex is required. Mirrors
// the lock-free stubRepo pattern already in this file.
type recordingJobsRepo struct {
	// Seeding controls for Get.
	replyJob *jobs.Job
	replyErr error

	// FailWithRetry recording.
	fwrCalls      int
	lastJobID     string
	lastErrCode   string
	lastErrMsg    string
	lastRetryable bool
	lastRevision  int
}

func (r *recordingJobsRepo) Get(_ context.Context, _ string) (*jobs.Job, error) {
	if r.replyErr != nil {
		return nil, r.replyErr
	}
	if r.replyJob == nil {
		return nil, nil
	}
	cp := *r.replyJob
	return &cp, nil
}

func (r *recordingJobsRepo) FailWithRetry(_ context.Context, jobID, errorCode, errorMessage string, retryable bool, revision int) error {
	r.fwrCalls++
	r.lastJobID = jobID
	r.lastErrCode = errorCode
	r.lastErrMsg = errorMessage
	r.lastRetryable = retryable
	r.lastRevision = revision
	return nil
}

// exhaustedExpireStubRepo is a minimal Repository stub that wires
// the two methods ExpireTaskLease invokes (ExpireTaskLeaseAtomic +
// Get). All other Repository methods panic so accidental fan-out
// into undefined territory fails loud. Composition over inheritance:
// reused methods from the existing stubRepo are NOT re-exported
// here — each test that needs ExpireTaskLease wiring creates its
// own exhaustedExpireStubRepo instance.
type exhaustedExpireStubRepo struct {
	expireResult   ExpireResult
	expireErr      error
	expireCalls    int
	lastExpireArgs struct {
		taskID, leaseID, observedExp string
		maxRetries                   int
	}

	getReply *Task
	getErr   error
	getCalls int
}

func (s *exhaustedExpireStubRepo) ExpireTaskLeaseAtomic(_ context.Context, taskID, leaseID, observedExp string, maxRetries int) (ExpireResult, error) {
	s.expireCalls++
	s.lastExpireArgs.taskID = taskID
	s.lastExpireArgs.leaseID = leaseID
	s.lastExpireArgs.observedExp = observedExp
	s.lastExpireArgs.maxRetries = maxRetries
	if s.expireErr != nil {
		return ExpireResult{}, s.expireErr
	}
	cp := s.expireResult
	cp.TaskID = taskID
	return cp, nil
}

func (s *exhaustedExpireStubRepo) Get(_ context.Context, _ string) (*Task, error) {
	s.getCalls++
	if s.getErr != nil {
		return nil, s.getErr
	}
	if s.getReply == nil {
		return nil, nil
	}
	cp := *s.getReply
	return &cp, nil
}

// Unused methods panic so any unexpected path is loud.
func (s *exhaustedExpireStubRepo) Create(context.Context, *Task) error {
	panic("exhaustedExpireStubRepo.Create: not exercised by ExpireTaskLease tests")
}
func (s *exhaustedExpireStubRepo) GetByJobID(context.Context, string) (*Task, error) {
	panic("exhaustedExpireStubRepo.GetByJobID: not exercised by ExpireTaskLease tests")
}
func (s *exhaustedExpireStubRepo) List(context.Context, Filter) ([]Task, error) {
	panic("exhaustedExpireStubRepo.List: not exercised by ExpireTaskLease tests")
}
func (s *exhaustedExpireStubRepo) SetStatus(context.Context, string, Status, Status, int) error {
	panic("exhaustedExpireStubRepo.SetStatus: not exercised by ExpireTaskLease tests")
}
func (s *exhaustedExpireStubRepo) Lease(context.Context, string, string, string) error {
	panic("exhaustedExpireStubRepo.Lease: not exercised by ExpireTaskLease tests")
}
func (s *exhaustedExpireStubRepo) ClaimNextReadyTask(context.Context, string, string) (*TaskWithSpec, error) {
	panic("exhaustedExpireStubRepo.ClaimNextReadyTask: not exercised by ExpireTaskLease tests")
}
func (s *exhaustedExpireStubRepo) ClaimNextWithAttemptAtomic(context.Context, string, string) (*TaskWithSpec, *taskattempts.TaskAttempt, error) {
	panic("exhaustedExpireStubRepo.ClaimNextWithAttemptAtomic: not exercised by ExpireTaskLease tests")
}
func (s *exhaustedExpireStubRepo) ReleaseLease(context.Context, string, string, string) error {
	panic("exhaustedExpireStubRepo.ReleaseLease: not exercised by ExpireTaskLease tests")
}
func (s *exhaustedExpireStubRepo) Start(context.Context, string, string, string, int, int) error {
	panic("exhaustedExpireStubRepo.Start: not exercised by ExpireTaskLease tests")
}
func (s *exhaustedExpireStubRepo) Fail(context.Context, string, string, int) error {
	panic("exhaustedExpireStubRepo.Fail: not exercised by ExpireTaskLease tests")
}
func (s *exhaustedExpireStubRepo) IncrementAttempt(context.Context, string) error {
	panic("exhaustedExpireStubRepo.IncrementAttempt: not exercised by ExpireTaskLease tests")
}
func (s *exhaustedExpireStubRepo) AreDependenciesSatisfied(context.Context, []string) (bool, error) {
	panic("exhaustedExpireStubRepo.AreDependenciesSatisfied: not exercised by ExpireTaskLease tests")
}
func (s *exhaustedExpireStubRepo) AcceptTaskAtomic(context.Context, *taskattempts.TaskAttempt, int) error {
	panic("exhaustedExpireStubRepo.AcceptTaskAtomic: not exercised by ExpireTaskLease tests")
}
func (s *exhaustedExpireStubRepo) TransitionTaskToTerminalAtomic(context.Context, string, string, string, Status, taskattempts.AttemptStatus, string, string) error {
	panic("exhaustedExpireStubRepo.TransitionTaskToTerminalAtomic: not exercised by ExpireTaskLease tests")
}
func (s *exhaustedExpireStubRepo) RenewLease(context.Context, string, string, string, time.Time, int) error {
	panic("exhaustedExpireStubRepo.RenewLease: not exercised by ExpireTaskLease tests")
}
func (s *exhaustedExpireStubRepo) RequeueExpiredLeases(context.Context, string, int) ([]RequeueCandidate, error) {
	panic("exhaustedExpireStubRepo.RequeueExpiredLeases: not exercised by ExpireTaskLease tests")
}
func (s *exhaustedExpireStubRepo) Delete(context.Context, string) error {
	panic("exhaustedExpireStubRepo.Delete: not exercised by ExpireTaskLease tests")
}

// compile-time assertion that both stubs satisfy their interfaces.
var _ Repository = (*exhaustedExpireStubRepo)(nil)
var _ JobsRetryQuerier = (*recordingJobsRepo)(nil)

// TestLifecycleService_ExpireTaskLease_CallsFailWithRetryOnExhausted:
// audit-P0#4 end-to-end coverage. When ExpireTaskLeaseAtomic replies
// with AttemptsExhausted=true, the lifecycle wrapper MUST post-commit
// invoke JobsRetryQuerier.FailWithRetry exactly once with the
// LEASE_EXPIRED_RETRIES_EXHAUSTED tuple:
//
//   - errorCode  = "LEASE_EXPIRED_RETRIES_EXHAUSTED"
//   - errorMessage must contain the candidate task_id
//   - retryable  = false (terminal state, NOT a retryable failure)
//   - revision   = the Job.Revision read at FailWithRetry time
//
// The Job wire-up runs OUTSIDE the reap-atomic so the audit-strict
// transactional boundary (Task + Attempt in one tx) is preserved; a
// failure of FailWithRetry does NOT roll back the Task reap. The
// post-condition here asserts the call was made (not its outcome);
// the wrap-with-`_ = l.jobsRepo.FailWithRetry(...)` block tests the
// no-rollback invariant at the smoke-test level.
func TestLifecycleService_ExpireTaskLease_CallsFailWithRetryOnExhausted(t *testing.T) {
	const (
		taskID   = "T-exh"
		jobID    = "J-exh"
		leaseID  = "L-exh"
		observed = "2026-06-22T12:00:00Z"
		wantRev  = 42
	)

	taskRepo := &exhaustedExpireStubRepo{
		expireResult: ExpireResult{
			TaskStatus:        StatusFailed,
			AttemptsExhausted: true,
			AttemptID:         "A-exh",
			AttemptClosed:     true,
		},
		getReply: &Task{ID: taskID, JobID: jobID},
	}
	jobsRepo := &recordingJobsRepo{
		replyJob: &jobs.Job{
			ID:         jobID,
			MaxRetries: 3,
			Status:     jobs.StatusRunning, // non-terminal
			Revision:   wantRev,
		},
	}
	svc, err := NewLifecycleService(taskRepo)
	if err != nil {
		t.Fatalf("NewLifecycleService: %v", err)
	}
	svc.SetJobsRepo(jobsRepo)

	res, err := svc.ExpireTaskLease(context.Background(), RequeueCandidate{
		ID:             taskID,
		LeaseID:        leaseID,
		LeaseExpiresAt: observed,
		WorkerID:       "w-exh",
		AttemptCount:   4, // already past budget at SELECT time
	})
	if err != nil {
		t.Fatalf("ExpireTaskLease (happy-path exhausted): %v", err)
	}
	if !res.AttemptsExhausted {
		t.Fatalf("AttempsExhausted = false; want true (audit P0#4 path)")
	}
	if res.TaskStatus != StatusFailed {
		t.Errorf("TaskStatus = %s; want FAILED", res.TaskStatus)
	}

	// Atomic was called once with the observed lease fields.
	if taskRepo.expireCalls != 1 {
		t.Errorf("ExpireTaskLeaseAtomic calls = %d; want 1", taskRepo.expireCalls)
	}
	if taskRepo.lastExpireArgs.taskID != taskID {
		t.Errorf("atomic taskID arg = %q; want %q", taskRepo.lastExpireArgs.taskID, taskID)
	}
	if taskRepo.lastExpireArgs.leaseID != leaseID {
		t.Errorf("atomic leaseID arg = %q; want %q", taskRepo.lastExpireArgs.leaseID, leaseID)
	}
	if taskRepo.lastExpireArgs.observedExp != observed {
		t.Errorf("atomic observed lease_expires_at arg = %q; want %q",
			taskRepo.lastExpireArgs.observedExp, observed)
	}
	if taskRepo.lastExpireArgs.maxRetries != 3 {
		t.Errorf("atomic maxRetries arg = %d; want 3 (from jobsRepo.Get.Job.MaxRetries)",
			taskRepo.lastExpireArgs.maxRetries)
	}

	// The job-aggregate wire-up MUST have fired exactly once.
	if jobsRepo.fwrCalls != 1 {
		t.Fatalf("JobsRetryQuerier.FailWithRetry calls = %d; want 1 (audit P0#4 end-to-end)",
			jobsRepo.fwrCalls)
	}

	// Assert every field of the canonical LEASE_EXPIRED_RETRIES_EXHAUSTED
	// tuple — a future regression that drops or renames the errorCode
	// surfaces here.
	if jobsRepo.lastJobID != jobID {
		t.Errorf("FailWithRetry jobID = %q; want %q", jobsRepo.lastJobID, jobID)
	}
	if jobsRepo.lastErrCode != "LEASE_EXPIRED_RETRIES_EXHAUSTED" {
		t.Errorf("FailWithRetry errorCode = %q; want LEASE_EXPIRED_RETRIES_EXHAUSTED",
			jobsRepo.lastErrCode)
	}
	if !strings.Contains(jobsRepo.lastErrMsg, taskID) {
		t.Errorf("FailWithRetry errorMessage = %q; want substring %q",
			jobsRepo.lastErrMsg, taskID)
	}
	if jobsRepo.lastRetryable {
		t.Errorf("FailWithRetry retryable = true; want false (FAILED is terminal, not a retryable failure)")
	}
	if jobsRepo.lastRevision != wantRev {
		t.Errorf("FailWithRetry revision = %d; want %d (Job.Revision captured at read-time)",
			jobsRepo.lastRevision, wantRev)
	}
}

// TestLifecycleService_ExpireTaskLease_NoFailWithRetryOnNonExhausted:
// the negative complement of the test above. When the atomic replies
// with AttemptsExhausted=false, the lifecycle wrapper MUST NOT call
// FailWithRetry. The task is re-claimed (READDY) and the Job stays
// RUNNING — the supervisor's next tick observes the re-claimable
// Task and re-decides without altering the Job aggregate.
func TestLifecycleService_ExpireTaskLease_NoFailWithRetryOnNonExhausted(t *testing.T) {
	const (
		taskID  = "T-recoverable"
		jobID   = "J-recoverable"
		leaseID = "L-recoverable"
	)

	taskRepo := &exhaustedExpireStubRepo{
		expireResult: ExpireResult{
			TaskStatus:        StatusReady, // re-claimable
			AttemptsExhausted: false,
		},
		getReply: &Task{ID: taskID, JobID: jobID},
	}
	jobsRepo := &recordingJobsRepo{
		replyJob: &jobs.Job{ID: jobID, Status: jobs.StatusRunning, Revision: 7, MaxRetries: 3},
	}
	svc, _ := NewLifecycleService(taskRepo)
	svc.SetJobsRepo(jobsRepo)

	if _, err := svc.ExpireTaskLease(context.Background(), RequeueCandidate{
		ID:             taskID,
		LeaseID:        leaseID,
		LeaseExpiresAt: "2026-06-22T12:00:00Z",
	}); err != nil {
		t.Fatalf("ExpireTaskLease (non-exhausted): %v", err)
	}
	if jobsRepo.fwrCalls != 0 {
		t.Errorf("FailWithRetry calls = %d; want 0 on AttemptsExhausted=false",
			jobsRepo.fwrCalls)
	}
	// Even though the Job is wired, FailWithRetry is NOT called for
	// non-exhausted reaps; Job stays RUNNING for the next supervise tick.
}

// TestLifecycleService_ExpireTaskLease_NoFailWithRetryOnTerminalJob:
// even when the task reaps exhausted, a Job already in a terminal
// state SHOULD NOT be re-flipped. The lifecycle wrapper's guard
// `if !job.Status.IsTerminal()` short-circuits the FailWithRetry call
// so the audit (Job-roll-up) path is idempotent against retry-class
// noise. Reference audit §P0#4 idempotency: the audit-mandated retry
// classification must never overwrite a Job already FAILED /
// SUCCEEDED / CANCELLED.
func TestLifecycleService_ExpireTaskLease_NoFailWithRetryOnTerminalJob(t *testing.T) {
	const (
		taskID  = "T-terminal"
		jobID   = "J-terminal"
		leaseID = "L-terminal"
	)

	taskRepo := &exhaustedExpireStubRepo{
		expireResult: ExpireResult{
			TaskStatus:        StatusFailed,
			AttemptsExhausted: true, // task itself is exhausted
		},
		getReply: &Task{ID: taskID, JobID: jobID},
	}
	jobsRepo := &recordingJobsRepo{
		replyJob: &jobs.Job{
			ID:       jobID,
			Status:   jobs.StatusFailed, // already terminal
			Revision: 99,
		},
	}
	svc, _ := NewLifecycleService(taskRepo)
	svc.SetJobsRepo(jobsRepo)

	if _, err := svc.ExpireTaskLease(context.Background(), RequeueCandidate{
		ID: taskID, LeaseID: leaseID,
		LeaseExpiresAt: "2026-06-22T12:00:00Z",
	}); err != nil {
		t.Fatalf("ExpireTaskLease: %v", err)
	}
	if jobsRepo.fwrCalls != 0 {
		t.Errorf("FailWithRetry calls = %d; want 0 when Job.IsTerminal() (idempotency guard §P0#4)",
			jobsRepo.fwrCalls)
	}
}

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
	requeueReply    []RequeueCandidate
	requeueErr      error
}

func (s *stubRepo) Create(ctx context.Context, t *Task) error         { panic("stubRepo.Create") }
func (s *stubRepo) Get(ctx context.Context, id string) (*Task, error) { panic("stubRepo.Get") }
func (s *stubRepo) GetByJobID(_ context.Context, _ string) (*Task, error) {
	panic("stubRepo.GetByJobID")
}
func (s *stubRepo) List(ctx context.Context, f Filter) ([]Task, error) { panic("stubRepo.List") }
func (s *stubRepo) SetStatus(ctx context.Context, id string, from, to Status, revision int) error {
	panic("stubRepo.SetStatus")
}
func (s *stubRepo) Lease(ctx context.Context, id, workerID, leaseID string) error {
	panic("stubRepo.Lease")
}
func (s *stubRepo) ClaimNextReadyTask(_ context.Context, _, _ string) (*TaskWithSpec, error) {
	panic("stubRepo.ClaimNextReadyTask")
}
func (s *stubRepo) ReleaseLease(_ context.Context, _, _, _ string) error {
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

func (s *stubRepo) RequeueExpiredLeases(_ context.Context, nowStr string, limit int) ([]RequeueCandidate, error) {
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

// ExpireTaskLeaseAtomic is not exercised by the RequeueExpiredLeases
// delegation tests; a panic keeps the stub surface honest so any
// accidental call from an expanded test fails loud.
func (s *stubRepo) ExpireTaskLeaseAtomic(_ context.Context, _, _, _ string, _ int) (ExpireResult, error) {
	panic("stubRepo.ExpireTaskLeaseAtomic: not used in this test scope")
}

// ClaimNextWithAttemptAtomic is the PR-2 (canonical-attempt-identity)
// sibling of ClaimNextReadyTask: returns (*TaskWithSpec, *TaskAttempt)
// so the canonical attempt_id minted at Claim time is in hand by the
// time TaskOffer is constructed. Not exercised by the
// RequeueExpiredLeases delegation tests; panic on call so any
// accidental fan-out into the new path from an expanded test fails
// loud.
func (s *stubRepo) ClaimNextWithAttemptAtomic(_ context.Context, _, _ string) (*TaskWithSpec, *taskattempts.TaskAttempt, error) {
	panic("stubRepo.ClaimNextWithAttemptAtomic: not used in this test scope")
}

// TestLifecycleService_RequeueExpiredLeases_DelegatesToRepo: the
// supervisor-facing wrapper passes through (nowStr, limit) and returns
// whatever the repo returned. PR-04 expanded the return shape to
// RequeueCandidate (carrying observed lease_id/lease_expires_at for the
// subsequent atomic reap), so the test compares on the ID field of the
// first candidate.
func TestLifecycleService_RequeueExpiredLeases_DelegatesToRepo(t *testing.T) {
	repo := &stubRepo{requeueReply: []RequeueCandidate{{ID: "t1", LeaseID: "l1", LeaseExpiresAt: "2026-06-22T12:30:00Z"}, {ID: "t2", LeaseID: "l1", LeaseExpiresAt: "2026-06-22T12:30:00Z"}}}
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
	if len(got) != 2 || got[0].ID != "t1" || got[1].ID != "t2" {
		t.Errorf("returned candidates not propagated: %+v", got)
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
