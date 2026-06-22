package ingest

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"velox-server/internal/costmodel"
	"velox-server/internal/jobs"
	"velox-server/internal/store"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
	"velox-server/internal/taskoutput_artifacts"
)

// =====================================================================
// fix/task-native-artifact-bridge: stubs for TaskReportIngestionService.
// =====================================================================

// stubIngestTaskRepo implements taskgraph.Repository with the minimum
// surface needed by TaskReportIngestionService: TransitionTaskToTerminalAtomic,
// Get, List, plus the rest as panics to surface accidental calls.
//
// RenewLease must match the exact taskgraph.Repository.RenewLease
// signature (time.Time 6th param) for the stub to satisfy the interface.
type stubIngestTaskRepo struct {
	mu sync.Mutex

	transitionCalls   int
	transitionErr     error
	transitionedTask  string
	transitionedState taskgraph.Status

	listTasks []taskgraph.Task

	nowTask taskgraph.Task
	nowErr  error
}

func (s *stubIngestTaskRepo) Get(_ context.Context, id string) (*taskgraph.Task, error) {
	if s.nowTask.ID == id {
		return &s.nowTask, s.nowErr
	}
	return nil, errors.New("not found (stub)")
}

func (s *stubIngestTaskRepo) List(_ context.Context, _ taskgraph.Filter) ([]taskgraph.Task, error) {
	return s.listTasks, nil
}

func (s *stubIngestTaskRepo) TransitionTaskToTerminalAtomic(
	_ context.Context, taskID, _, _ string,
	taskStatus taskgraph.Status, _ taskattempts.AttemptStatus, _, _ string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.transitionCalls++
	s.transitionedTask = taskID
	s.transitionedState = taskStatus
	if s.transitionErr != nil {
		return s.transitionErr
	}
	for i := range s.listTasks {
		if s.listTasks[i].ID == taskID {
			s.listTasks[i].Status = taskStatus
		}
	}
	return nil
}

// Panics for every other method so any unexpected call is loud.
func (s *stubIngestTaskRepo) Create(_ context.Context, _ *taskgraph.Task) error {
	panic("stubIngestTaskRepo.Create")
}
func (s *stubIngestTaskRepo) ListByJobID(_ context.Context, _ string) (*taskgraph.Task, error) {
	panic("stubIngestTaskRepo.ListByJobID")
}
func (s *stubIngestTaskRepo) SetStatus(_ context.Context, _ string, _, _ taskgraph.Status, _ int) error {
	panic("stubIngestTaskRepo.SetStatus")
}
func (s *stubIngestTaskRepo) Lease(_ context.Context, _, _, _ string) error {
	panic("stubIngestTaskRepo.Lease")
}
func (s *stubIngestTaskRepo) GetByJobID(_ context.Context, jobID string) (*taskgraph.Task, error) {
	for i := range s.listTasks {
		if s.listTasks[i].JobID == jobID {
			cp := s.listTasks[i]
			return &cp, nil
		}
	}
	return nil, nil
}
func (s *stubIngestTaskRepo) ClaimNextReadyTask(_ context.Context, _, _ string) (*taskgraph.TaskWithSpec, error) {
	panic("stubIngestTaskRepo.ClaimNextReadyTask")
}
func (s *stubIngestTaskRepo) ReleaseLease(_ context.Context, _ string) error {
	panic("stubIngestTaskRepo.ReleaseLease")
}
func (s *stubIngestTaskRepo) Start(_ context.Context, _, _, _ string, _, _ int) error {
	panic("stubIngestTaskRepo.Start")
}
func (s *stubIngestTaskRepo) Fail(_ context.Context, _, _ string, _ int) error {
	panic("stubIngestTaskRepo.Fail")
}
func (s *stubIngestTaskRepo) IncrementAttempt(_ context.Context, _ string) error {
	panic("stubIngestTaskRepo.IncrementAttempt")
}
func (s *stubIngestTaskRepo) AreDependenciesSatisfied(_ context.Context, _ []string) (bool, error) {
	panic("stubIngestTaskRepo.AreDependenciesSatisfied")
}
func (s *stubIngestTaskRepo) AcceptTaskAtomic(_ context.Context, _ *taskattempts.TaskAttempt, _ int) error {
	panic("stubIngestTaskRepo.AcceptTaskAtomic")
}
func (s *stubIngestTaskRepo) RenewLease(_ context.Context, _, _, _ string, _ time.Time, _ int) error {
	panic("stubIngestTaskRepo.RenewLease")
}
func (s *stubIngestTaskRepo) ExpireTaskLeaseAtomic(_ context.Context, _, _, _ string, _ int) (taskgraph.ExpireResult, error) {
	panic("stubIngestTaskRepo.ExpireTaskLeaseAtomic")
}
func (s *stubIngestTaskRepo) RequeueExpiredLeases(_ context.Context, _ string, _ int) ([]taskgraph.RequeueCandidate, error) {
	panic("stubIngestTaskRepo.RequeueExpiredLeases")
}
func (s *stubIngestTaskRepo) Delete(_ context.Context, _ string) error {
	panic("stubIngestTaskRepo.Delete")
}

// mirror of stubRepo.ClaimNextWithAttemptAtomic (PR-2). Not exercised
// by any current ingest service test; panic on call so any accidental
// fan-out from an expanded test fails loud rather than silently
// returning a zero-valued (*TaskWithSpec, *TaskAttempt).
func (s *stubIngestTaskRepo) ClaimNextWithAttemptAtomic(_ context.Context, _, _ string) (*taskgraph.TaskWithSpec, *taskattempts.TaskAttempt, error) {
	panic("stubIngestTaskRepo.ClaimNextWithAttemptAtomic: not used in this test scope")
}

// stubIngestJobsRepo implements jobs.Repository with the minimum surface.
type stubIngestJobsRepo struct {
	mu sync.Mutex

	getJob         *jobs.Job
	setStatusCalls int
	lastFrom, lastTo jobs.Status
	lastSetErr     error
}

func (s *stubIngestJobsRepo) Get(_ context.Context, _ string) (*jobs.Job, error) {
	if s.getJob == nil {
		return nil, errors.New("job not found")
	}
	cp := *s.getJob
	return &cp, nil
}

// Counts is required by jobs.Reader; the ingest service does not call
// it directly but the stub must satisfy the interface.
func (s *stubIngestJobsRepo) Counts(_ context.Context) (jobs.Counts, error) {
	return jobs.Counts{}, nil
}

// List is required by jobs.Reader; the ingest service does not call it
// directly but the stub must satisfy the interface.
func (s *stubIngestJobsRepo) List(_ context.Context, _ jobs.Filter) ([]jobs.Job, error) {
	if s.getJob == nil {
		return nil, nil
	}
	return []jobs.Job{*s.getJob}, nil
}

func (s *stubIngestJobsRepo) SetStatus(_ context.Context, _ string, from, to jobs.Status) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setStatusCalls++
	s.lastFrom = from
	s.lastTo = to
	if s.lastSetErr != nil {
		return s.lastSetErr
	}
	if s.getJob != nil {
		s.getJob.Status = to
	}
	return nil
}

// Cancel is required by jobs.Writer (signature: Cancel(ctx, id, reason, revision)).
func (s *stubIngestJobsRepo) Cancel(_ context.Context, _ string, _ string, _ int) error {
	panic("stubIngestJobsRepo.Cancel")
}

// All remaining jobs.Writer methods are stubbed as panic-on-call sentinels
// so any accidental write path surfaces loud while satisfying the
// jobs.Repository interface.
func (s *stubIngestJobsRepo) Lease(_ context.Context, _, _ string) error {
	panic("stubIngestJobsRepo.Lease")
}
func (s *stubIngestJobsRepo) Fail(_ context.Context, _ string, _ string) error {
	panic("stubIngestJobsRepo.Fail")
}
func (s *stubIngestJobsRepo) Start(_ context.Context, _, _, _ string, _, _ int) error {
	panic("stubIngestJobsRepo.Start")
}
func (s *stubIngestJobsRepo) RenewLease(_ context.Context, _, _, _ string, _ time.Time, _ bool, _ int) error {
	panic("stubIngestJobsRepo.RenewLease")
}
func (s *stubIngestJobsRepo) FailWithRetry(_ context.Context, _, _, _ string, _ bool, _ int) error {
	panic("stubIngestJobsRepo.FailWithRetry")
}
func (s *stubIngestJobsRepo) RequeueExpiredLeases(_ context.Context, _ time.Time, _ int) ([]jobs.RequeueResult, error) {
	panic("stubIngestJobsRepo.RequeueExpiredLeases")
}
func (s *stubIngestJobsRepo) ClaimNext(_ context.Context, _ string, _ []string) (*jobs.ClaimNextResult, error) {
	panic("stubIngestJobsRepo.ClaimNext")
}
func (s *stubIngestJobsRepo) ClaimNextForProfile(_ context.Context, _ string, _ []string, _ costmodel.WorkerProfile, _ int) (*jobs.ClaimNextResult, error) {
	panic("stubIngestJobsRepo.ClaimNextForProfile")
}
func (s *stubIngestJobsRepo) ReleaseLease(_ context.Context, _ string) error {
	panic("stubIngestJobsRepo.ReleaseLease")
}
func (s *stubIngestJobsRepo) RecordRenderFinished(_ context.Context, _, _, _ string, _, _ int) error {
	panic("stubIngestJobsRepo.RecordRenderFinished")
}
func (s *stubIngestJobsRepo) Delete(_ context.Context, _ string) error {
	panic("stubIngestJobsRepo.Delete")
}

// stubIngestOutputArtifacts tracks Register calls; duplicates return
// ErrAlreadyRegistered.
type stubIngestOutputArtifacts struct {
	mu    sync.Mutex
	items map[string]taskoutput_artifacts.OutputArtifact
}

func newStubIngestOutputArtifacts() *stubIngestOutputArtifacts {
	return &stubIngestOutputArtifacts{items: map[string]taskoutput_artifacts.OutputArtifact{}}
}

func (s *stubIngestOutputArtifacts) Register(_ context.Context, a taskoutput_artifacts.OutputArtifact) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := a.TaskID + "|" + a.ArtifactID
	if _, exists := s.items[key]; exists {
		return taskoutput_artifacts.ErrAlreadyRegistered
	}
	s.items[key] = a
	return nil
}

func (s *stubIngestOutputArtifacts) ListByTask(_ context.Context, taskID string) ([]taskoutput_artifacts.OutputArtifact, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []taskoutput_artifacts.OutputArtifact{}
	for _, a := range s.items {
		if a.TaskID == taskID {
			out = append(out, a)
		}
	}
	return out, nil
}

// stubIngestAttemptRepo implements taskattempts.Repository with the
// minimum surface used by IngestTaskResult: GetByTaskIDAndWorkerAndLease.
// Everything else panics-on-call as a sentinel against accidental writes.
type stubIngestAttemptRepo struct {
	mu sync.Mutex

	// attempts is keyed by composite (task_id + "|" + worker_id + "|" + lease_id)
	// -> *taskattempts.TaskAttempt. Tests seed entries for the happy path;
	// leave empty (or omit the key) to simulate IMPERSONATION / stale lease.
	attempts map[string]*taskattempts.TaskAttempt

	lookupErr error // injected error for store-side wire-fallback lookup
}

// seedAttempt inserts an entry into attempts as the canonical
// non-terminal attempt for (taskID, workerID, leaseID). The default
// AttemptID/JobID/AttemptNumber match the fixture IngestCommand
// values (T1/J1/A1/1) so the wire-fallback strict-compare passes
// for tests that do not override.
func (s *stubIngestAttemptRepo) seedAttempt(taskID, workerID, leaseID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attempts == nil {
		s.attempts = map[string]*taskattempts.TaskAttempt{}
	}
	key := taskID + "|" + workerID + "|" + leaseID
	s.attempts[key] = &taskattempts.TaskAttempt{
		// PR-2: AttemptID/JobID/AttemptNumber match the fixture
		// IngestCommand literals for the canonical (T1, w-1, L1)
		// tuple. seedAttemptWithNumber escalates for tests that
		// drive the wire-vs-canonical strict-compare (T2/T3 cases).
		ID:            "A1",
		TaskID:        taskID,
		WorkerID:      workerID,
		LeaseID:       leaseID,
		AttemptNumber: 1,
		JobID:         "J1",
		Status:        taskattempts.AttemptStatusRunning,
	}
}

// seedAttemptWithNumber lets a test override the canonical row's
// identity fields so the validator's wire-vs-canonical strict-compare
// can be exercised outside the happy-path values.
func (s *stubIngestAttemptRepo) seedAttemptWithNumber(taskID, workerID, leaseID, attemptID, jobID string, attemptNumber int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attempts == nil {
		s.attempts = map[string]*taskattempts.TaskAttempt{}
	}
	key := taskID + "|" + workerID + "|" + leaseID
	s.attempts[key] = &taskattempts.TaskAttempt{
		ID:            attemptID,
		TaskID:        taskID,
		WorkerID:      workerID,
		LeaseID:       leaseID,
		AttemptNumber: attemptNumber,
		JobID:         jobID,
		Status:        taskattempts.AttemptStatusRunning,
	}
}

// GetByTaskIDAndWorkerAndLease returns the seeded attempt for the
// (task_id, worker_id, lease_id) tuple, or (nil, nil) when the tuple
// is unknown to the canonical store (the wire-fallback impersonation
// path).
func (s *stubIngestAttemptRepo) GetByTaskIDAndWorkerAndLease(_ context.Context, taskID, workerID, leaseID string) (*taskattempts.TaskAttempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lookupErr != nil {
		return nil, s.lookupErr
	}
	key := taskID + "|" + workerID + "|" + leaseID
	if att, ok := s.attempts[key]; ok {
		cp := *att
		return &cp, nil
	}
	return nil, nil
}

// Panics for every other method so any accidental call is loud.
func (s *stubIngestAttemptRepo) Get(_ context.Context, _ string) (*taskattempts.TaskAttempt, error) {
	panic("stubIngestAttemptRepo.Get")
}
func (s *stubIngestAttemptRepo) ListByTaskID(_ context.Context, _ string) ([]taskattempts.TaskAttempt, error) {
	panic("stubIngestAttemptRepo.ListByTaskID")
}
func (s *stubIngestAttemptRepo) GetActiveAttempt(_ context.Context, _ string) (*taskattempts.TaskAttempt, error) {
	panic("stubIngestAttemptRepo.GetActiveAttempt")
}
func (s *stubIngestAttemptRepo) Create(_ context.Context, _ *taskattempts.TaskAttempt) error {
	panic("stubIngestAttemptRepo.Create")
}
func (s *stubIngestAttemptRepo) SetStatus(_ context.Context, _ string, _, _ taskattempts.AttemptStatus, _ int) error {
	panic("stubIngestAttemptRepo.SetStatus")
}
func (s *stubIngestAttemptRepo) CompleteFinal(_ context.Context, _, _, _ string, _ taskattempts.AttemptStatus, _, _ string, _ int) error {
	panic("stubIngestAttemptRepo.CompleteFinal")
}
func (s *stubIngestAttemptRepo) Delete(_ context.Context, _ string) error {
	panic("stubIngestAttemptRepo.Delete")
}

// TestIngestionService_ValidateIdentityTuple_WireAttemptIDMismatch:
// PR-2 strict-compare guard for the AttemptID half of the wire tuple.
// Seed canonical row with AttemptID="A-canonical" + call with
// AttemptID="A-attacker" over the same (task_id, worker_id, lease_id).
// Distinct from the impersonation (lookup-miss) test: here the row
// exists and only the attempt_id field is wrong, exercising the
// att.ID != cmd.AttemptID strict-compare path.
func TestIngestionService_ValidateIdentityTuple_WireAttemptIDMismatch(t *testing.T) {
	taskRepo := &stubIngestTaskRepo{}
	jobsRepo := &stubIngestJobsRepo{}
	attempts := &stubIngestAttemptRepo{}
	attempts.seedAttemptWithNumber("T4", "w4", "L4", "A-canonical", "J4", 1)
	svc, err := NewTaskReportIngestionService(taskRepo, jobsRepo, attempts, newStubIngestOutputArtifacts())
	if err != nil {
		t.Fatalf("NewTaskReportIngestionService: %v", err)
	}
	err = svc.ValidateIdentityTuple(context.Background(), IngestCommand{
		TaskID:        "T4",
		AttemptID:     "A-attacker", // disagrees with canonical "A-canonical"
		LeaseID:       "L4",
		WorkerID:      "w4",
		JobID:         "J4",
		AttemptNumber: 1,
	})
	if err == nil {
		t.Fatal("ValidateIdentityTuple returned nil for wire-vs-canonical attempt_id mismatch; want ErrIdentityMismatch")
	}
	if !errors.Is(err, taskattempts.ErrIdentityMismatch) {
		t.Errorf("ValidateIdentityTuple returned %v; want taskattempts.ErrIdentityMismatch wrapped", err)
	}
}

// Compile-time assertion that the stub satisfies the canonical interface.
var _ taskattempts.Repository = (*stubIngestAttemptRepo)(nil)

// TestIngestionService_ValidateIdentityTuple_WireAttemptNumberMismatch:
// PR-2 strict-compare guard for the AttemptNumber half of the wire
// tuple. Seed canonical with AttemptNumber=3 and call the validator
// with AttemptNumber=2. The strict-compare must surface
// taskattempts.ErrIdentityMismatch so reaper + log surface code
// can identify impersonation-class errors canonically.
func TestIngestionService_ValidateIdentityTuple_WireAttemptNumberMismatch(t *testing.T) {
	taskRepo := &stubIngestTaskRepo{}
	jobsRepo := &stubIngestJobsRepo{}
	attempts := &stubIngestAttemptRepo{}
	attempts.seedAttemptWithNumber("T2", "w2", "L2", "A2", "J2", 3)
	svc, err := NewTaskReportIngestionService(taskRepo, jobsRepo, attempts, newStubIngestOutputArtifacts())
	if err != nil {
		t.Fatalf("NewTaskReportIngestionService: %v", err)
	}
	err = svc.ValidateIdentityTuple(context.Background(), IngestCommand{
		TaskID:        "T2",
		AttemptID:     "A2",
		LeaseID:       "L2",
		WorkerID:      "w2",
		JobID:         "J2",
		AttemptNumber: 2, // wire disagrees with canonical 3
	})
	if err == nil {
		t.Fatal("ValidateIdentityTuple returned nil for wire-vs-canonical attempt_number mismatch; want ErrIdentityMismatch")
	}
	if !errors.Is(err, taskattempts.ErrIdentityMismatch) {
		t.Errorf("ValidateIdentityTuple returned %v; want taskattempts.ErrIdentityMismatch wrapped", err)
	}
}

// TestIngestionService_ValidateIdentityTuple_WireJobIDMismatch:
// PR-2 strict-compare guard for the JobID half of the wire tuple.
// Seed canonical with JobID="J-canonical" and call with
// JobID="J-wire" over the same (task_id, worker_id, lease_id)
// tuple -- the JobID must be wrapped to ErrIdentityMismatch.
func TestIngestionService_ValidateIdentityTuple_WireJobIDMismatch(t *testing.T) {
	taskRepo := &stubIngestTaskRepo{}
	jobsRepo := &stubIngestJobsRepo{}
	attempts := &stubIngestAttemptRepo{}
	attempts.seedAttemptWithNumber("T3", "w3", "L3", "A3", "J-canonical", 1)
	svc, err := NewTaskReportIngestionService(taskRepo, jobsRepo, attempts, newStubIngestOutputArtifacts())
	if err != nil {
		t.Fatalf("NewTaskReportIngestionService: %v", err)
	}
	err = svc.ValidateIdentityTuple(context.Background(), IngestCommand{
		TaskID:        "T3",
		AttemptID:     "A3",
		LeaseID:       "L3",
		WorkerID:      "w3",
		JobID:         "J-wire", // disagrees with canonical "J-canonical"
		AttemptNumber: 1,
	})
	if err == nil {
		t.Fatal("ValidateIdentityTuple returned nil for wire-vs-canonical job_id mismatch; want ErrIdentityMismatch")
	}
	if !errors.Is(err, taskattempts.ErrIdentityMismatch) {
		t.Errorf("ValidateIdentityTuple returned %v; want taskattempts.ErrIdentityMismatch wrapped", err)
	}
}

// =====================================================================
// Tests
// =====================================================================

// newWiredSvc builds a TaskReportIngestionService with all four deps
// wired + a seeded canonical attempt for the happy-path tuple
// (T1, w-1, L1) -- matching the LeaseID the canonical IngestCommand
// fixture uses in TestIngestionService_HappyPathSucceeded + Idempotent
// replay so the wire-fallback lookup succeeds. Returns the service for
// tests that don't need to tweak individual stubs.
func newWiredSvc(t *testing.T, taskRepo *stubIngestTaskRepo, jobsRepo *stubIngestJobsRepo, attemptRepo *stubIngestAttemptRepo, out *stubIngestOutputArtifacts) *TaskReportIngestionService {
	t.Helper()
	attemptRepo.seedAttempt("T1", "w-1", "L1")
	svc, err := NewTaskReportIngestionService(taskRepo, jobsRepo, attemptRepo, out)
	if err != nil {
		t.Fatalf("NewTaskReportIngestionService: %v", err)
	}
	return svc
}

// TestIngestionService_HappyPathSucceeded asserts the canonical happy
// path: identity tuple validates, attempt closes via atomic, artifacts
// register, Job rises to AWAITING_ARTIFACT because all siblings
// succeeded.
func TestIngestionService_HappyPathSucceeded(t *testing.T) {
	taskRepo := &stubIngestTaskRepo{
		listTasks: []taskgraph.Task{
			{ID: "T1", JobID: "J1", Status: taskgraph.StatusSucceeded},
		},
	}
	jobsRepo := &stubIngestJobsRepo{
		getJob: &jobs.Job{ID: "J1", Status: jobs.StatusRunning, MaxRetries: 3, Revision: 0},
	}
	svc := newWiredSvc(t, taskRepo, jobsRepo, &stubIngestAttemptRepo{}, newStubIngestOutputArtifacts())

	res, err := svc.IngestTaskResult(context.Background(), IngestCommand{
		TaskID:        "T1",
		AttemptID:     "A1",
		LeaseID:       "L1",
		WorkerID:      "w-1",
		JobID:         "J1",
		AttemptNumber: 1,
		Status:        "succeeded",
		OutputArtifacts: []DeclaredArtifact{
			{ArtifactID: "art-1", ArtifactType: "video"},
			{ArtifactID: "art-2", ArtifactType: "video"},
		},
	})
	if err != nil {
		t.Fatalf("IngestTaskResult: %v", err)
	}
	if !res.AttemptClosed {
		t.Errorf("AttemptClosed=false; want true (transition fired)")
	}
	if res.ArtifactsNew != 2 {
		t.Errorf("ArtifactsNew=%d; want 2", res.ArtifactsNew)
	}
	if !res.JobTransitioned {
		t.Errorf("JobTransitioned=false; want true (all siblings terminal + all succeeded)")
	}
	if res.JobNewStatus != string(jobs.StatusAwaitingArtifact) {
		t.Errorf("JobNewStatus=%q; want AWAITING_ARTIFACT", res.JobNewStatus)
	}
	if taskRepo.transitionCalls != 1 {
		t.Errorf("transitionCalls=%d; want 1", taskRepo.transitionCalls)
	}
	if taskRepo.transitionedState != taskgraph.StatusSucceeded {
		t.Errorf("transitionedState=%s; want SUCCEEDED", taskRepo.transitionedState)
	}
	if jobsRepo.setStatusCalls != 1 {
		t.Errorf("setStatusCalls=%d; want 1 (one Job roll-up write)", jobsRepo.setStatusCalls)
	}
}

// TestIngestionService_ValidateIdentityTuple_HappyPath confirms the
// store-side wire-fallback path returns nil when the (task_id, worker_id,
// lease_id) tuple maps to a non-terminal attempt in the canonical store.
func TestIngestionService_ValidateIdentityTuple_HappyPath(t *testing.T) {
	taskRepo := &stubIngestTaskRepo{}
	jobsRepo := &stubIngestJobsRepo{}
	attempts := &stubIngestAttemptRepo{}
	// PR-2 seed key (T1, w-1, L1) MUST match the validator's wire
	// tuple below -- otherwise the lookup-miss path fires and the
	// validator returns ErrIdentityMismatch (lookup-miss branch)
	// which is not the happy path we want to assert here.
	attempts.seedAttempt("T1", "w-1", "L1")
	svc, err := NewTaskReportIngestionService(taskRepo, jobsRepo, attempts, newStubIngestOutputArtifacts())
	if err != nil {
		t.Fatalf("NewTaskReportIngestionService: %v", err)
	}
	if err := svc.ValidateIdentityTuple(context.Background(), IngestCommand{
		// T1/A1/J1/L1/w-1/1 match the canonical row that
		// seedAttempt inserts into the stub (PR-2: identity
		// tuple strict-compare; no mismatched field).
		TaskID:        "T1",
		AttemptID:     "A1",
		LeaseID:       "L1",
		WorkerID:      "w-1",
		JobID:         "J1",
		AttemptNumber: 1,
	}); err != nil {
		t.Errorf("ValidateIdentityTuple happy path returned %v; want nil", err)
	}
}

// TestIngestionService_ValidateIdentityTuple_MismatchReturnsCanonicalSentinel
// confirms the pseudonym-attack / lease-revoked stale-worker case: the
// canonical attempt store has NO row matching the wire tuple, so the
// validator surfaces the canonical sentinel taskattempts.ErrIdentityMismatch.
func TestIngestionService_ValidateIdentityTuple_MismatchReturnsCanonicalSentinel(t *testing.T) {
	taskRepo := &stubIngestTaskRepo{}
	jobsRepo := &stubIngestJobsRepo{}
	attempts := &stubIngestAttemptRepo{}
	// Do NOT seed -- simulates impersonation / lease-revoked retry.
	svc, err := NewTaskReportIngestionService(taskRepo, jobsRepo, attempts, newStubIngestOutputArtifacts())
	if err != nil {
		t.Fatalf("NewTaskReportIngestionService: %v", err)
	}
	err = svc.ValidateIdentityTuple(context.Background(), IngestCommand{
		TaskID:        "T-x",
		AttemptID:     "A-x",
		LeaseID:       "L-x",
		WorkerID:      "w-x",
		JobID:         "J-x",
		AttemptNumber: 1,
	})
	if err == nil {
		t.Fatal("ValidateIdentityTuple returned nil for impersonation-style mismatch; want ErrIdentityMismatch")
	}
	if !errors.Is(err, taskattempts.ErrIdentityMismatch) {
		t.Errorf("ValidateIdentityTuple returned %v; want taskattempts.ErrIdentityMismatch wrapped", err)
	}
}

// TestIngestionService_ValidateIdentityTuple_EmptyFields ensures the
// cheap field-presence checks fire BEFORE the store-side lookup
// (the lookup is a DB round-trip, the field check is free).
func TestIngestionService_ValidateIdentityTuple_EmptyFields(t *testing.T) {
	taskRepo := &stubIngestTaskRepo{}
	jobsRepo := &stubIngestJobsRepo{}
	attempts := &stubIngestAttemptRepo{}
	svc, _ := NewTaskReportIngestionService(taskRepo, jobsRepo, attempts, newStubIngestOutputArtifacts())

	cases := []struct {
		name string
		cmd  IngestCommand
		want string
	}{
		{"empty TaskID", IngestCommand{AttemptID: "A1", LeaseID: "L1", WorkerID: "w-1", JobID: "J1", AttemptNumber: 1}, "TaskID is required"},
		{"empty AttemptID", IngestCommand{TaskID: "T1", LeaseID: "L1", WorkerID: "w-1", JobID: "J1", AttemptNumber: 1}, "AttemptID is required"},
		{"empty LeaseID", IngestCommand{TaskID: "T1", AttemptID: "A1", WorkerID: "w-1", JobID: "J1", AttemptNumber: 1}, "LeaseID is required"},
		{"empty WorkerID", IngestCommand{TaskID: "T1", AttemptID: "A1", LeaseID: "L1", JobID: "J1", AttemptNumber: 1}, "WorkerID is required"},
		{"empty JobID (PR-2 strict-compare)", IngestCommand{TaskID: "T1", AttemptID: "A1", LeaseID: "L1", WorkerID: "w-1", AttemptNumber: 1}, "JobID is required"},
		{"zero AttemptNumber (PR-2 strict-compare)", IngestCommand{TaskID: "T1", AttemptID: "A1", LeaseID: "L1", WorkerID: "w-1", JobID: "J1"}, "AttemptNumber must be >0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := svc.ValidateIdentityTuple(context.Background(), tc.cmd)
			if err == nil {
				t.Fatalf("ValidateIdentityTuple returned nil; want error containing %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("ValidateIdentityTuple error=%q; want substring %q", err.Error(), tc.want)
			}
		})
	}
}

// TestIngestionService_IdempotentReplay: a second invocation with the
// same OutputArtifacts is a counted skip -- AttemptClosed=false,
// ArtifactsNew=0, ArtifactsSkips=2, no SetStatus write.
func TestIngestionService_IdempotentReplay(t *testing.T) {
	taskRepo := &stubIngestTaskRepo{
		transitionErr: store.ErrTransitionConflict, // already terminal
		listTasks: []taskgraph.Task{
			{ID: "T1", JobID: "J1", Status: taskgraph.StatusSucceeded},
		},
	}
	jobsRepo := &stubIngestJobsRepo{
		getJob: &jobs.Job{ID: "J1", Status: jobs.StatusAwaitingArtifact, Revision: 0},
	}
	out := newStubIngestOutputArtifacts()
	// Pre-seed the duplicate artifacts so Register returns ErrAlreadyRegistered.
	for _, id := range []string{"art-1", "art-2"} {
		_ = out.Register(context.Background(), taskoutput_artifacts.OutputArtifact{
			TaskID: "T1", ArtifactID: id, AttemptID: "A1",
		})
	}
	svc := newWiredSvc(t, taskRepo, jobsRepo, &stubIngestAttemptRepo{}, out)

	res, err := svc.IngestTaskResult(context.Background(), IngestCommand{
		TaskID:        "T1",
		AttemptID:     "A1",
		LeaseID:       "L1",
		WorkerID:      "w-1",
		JobID:         "J1",
		AttemptNumber: 1,
		Status:        "succeeded",
		OutputArtifacts: []DeclaredArtifact{
			{ArtifactID: "art-1"},
			{ArtifactID: "art-2"},
		},
	})
	if err != nil {
		t.Fatalf("IngestTaskResult on replay: %v", err)
	}
	if res.AttemptClosed {
		t.Errorf("AttemptClosed=true; want false (replay: CAS already missed)")
	}
	if res.ArtifactsNew != 0 {
		t.Errorf("ArtifactsNew=%d; want 0 (all duplicates)", res.ArtifactsNew)
	}
	if res.ArtifactsSkips != 2 {
		t.Errorf("ArtifactsSkips=%d; want 2 (counted skips)", res.ArtifactsSkips)
	}
	if jobsRepo.setStatusCalls != 0 {
		t.Errorf("setStatusCalls=%d; want 0 (Job already AWAITING_ARTIFACT -> idempotent no-op)", jobsRepo.setStatusCalls)
	}
}

// TestIngestionService_SiblingsStillRunningNoJobRollUp: a TaskResult
// whose sibling tasks are still PENDING must NOT trigger a Job SetStatus
// write. Two siblings, T1 the task under test closes to FAILED, T2
// stays PENDING -> maybeTransitionJob observes !allTerminal -> no-op.
func TestIngestionService_SiblingsStillRunningNoJobRollUp(t *testing.T) {
	taskRepo := &stubIngestTaskRepo{
		listTasks: []taskgraph.Task{
			{ID: "T1", JobID: "J1", Status: taskgraph.StatusLeased},
			{ID: "T2", JobID: "J1", Status: taskgraph.StatusPending},
		},
	}
	jobsRepo := &stubIngestJobsRepo{
		getJob: &jobs.Job{ID: "J1", Status: jobs.StatusRunning, Revision: 0},
	}
	svc := newWiredSvc(t, taskRepo, jobsRepo, &stubIngestAttemptRepo{}, newStubIngestOutputArtifacts())

	res, err := svc.IngestTaskResult(context.Background(), IngestCommand{
		TaskID:        "T1",
		AttemptID:     "A1",
		LeaseID:       "L1",
		WorkerID:      "w-1",
		JobID:         "J1",
		AttemptNumber: 1,
		Status:        "failed",
		ErrorCode:     "RENDER_ERROR",
	})
	if err != nil {
		t.Fatalf("IngestTaskResult: %v", err)
	}
	if res.JobTransitioned {
		t.Errorf("JobTransitioned=true; want false (sibling T2 PENDING blocks roll-up)")
	}
	if jobsRepo.setStatusCalls != 0 {
		t.Errorf("setStatusCalls=%d; want 0 (sibling still non-terminal)", jobsRepo.setStatusCalls)
	}
}

// TestIngestionService_RequiresAllDeps ensures nil deps for ALL FOUR
// positions are rejected. The new attemptRepo guarantee means a future
// bootstrap mistake that drops taskattempts.Repository silently can no
// longer weaken the wire-fallback identity tuple contract.
func TestIngestionService_RequiresAllDeps(t *testing.T) {
	out := newStubIngestOutputArtifacts()
	attempts := &stubIngestAttemptRepo{}

	if _, err := NewTaskReportIngestionService(nil, nil, attempts, out); err == nil {
		t.Error("nil taskRepo should return error")
	} else if !strings.Contains(err.Error(), "taskRepo") {
		t.Errorf("error should mention taskRepo, got %q", err.Error())
	}
	if _, err := NewTaskReportIngestionService(&stubIngestTaskRepo{}, nil, attempts, out); err == nil {
		t.Error("nil jobsRepo should return error")
	} else if !strings.Contains(err.Error(), "jobsRepo") {
		t.Errorf("error should mention jobsRepo, got %q", err.Error())
	}
	if _, err := NewTaskReportIngestionService(&stubIngestTaskRepo{}, &stubIngestJobsRepo{}, nil, out); err == nil {
		t.Error("nil attemptRepo should return error")
	} else if !strings.Contains(err.Error(), "attemptRepo") {
		t.Errorf("error should mention attemptRepo, got %q", err.Error())
	}
	if _, err := NewTaskReportIngestionService(&stubIngestTaskRepo{}, &stubIngestJobsRepo{}, attempts, nil); err == nil {
		t.Error("nil outputArtRepo should return error")
	} else if !strings.Contains(err.Error(), "outputArtRepo") {
		t.Errorf("error should mention outputArtRepo, got %q", err.Error())
	}
}
