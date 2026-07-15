package ingest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"velox-server/internal/jobs"
	"velox-server/internal/placement"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
	"velox-server/internal/taskoutput_artifacts"
)

// Shared fixtures; test cases live in service_{identity,result,rollup}_test.go.
type stubIngestTaskRepo struct {
	mu                  sync.Mutex
	transitionCalls     int
	transitionErr       error
	transitionedTask    string
	transitionedState   taskgraph.Status
	listTasks           []taskgraph.Task
	nowTask             taskgraph.Task
	nowErr              error
	allCommitsCommitted bool
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
func (s *stubIngestTaskRepo) TransitionTaskToTerminalAtomic(_ context.Context, taskID, _, _ string, status taskgraph.Status, _ taskattempts.AttemptStatus, _, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.transitionCalls++
	s.transitionedTask = taskID
	s.transitionedState = status
	if s.transitionErr != nil {
		return s.transitionErr
	}
	for i := range s.listTasks {
		if s.listTasks[i].ID == taskID {
			s.listTasks[i].Status = status
		}
	}
	return nil
}
func (s *stubIngestTaskRepo) Create(context.Context, *taskgraph.Task) error { panic("Create") }
func (s *stubIngestTaskRepo) ListByJobID(context.Context, string) (*taskgraph.Task, error) {
	panic("ListByJobID")
}
func (s *stubIngestTaskRepo) SetStatus(context.Context, string, taskgraph.Status, taskgraph.Status, int) error {
	panic("SetStatus")
}
func (s *stubIngestTaskRepo) Lease(context.Context, string, string, string) error { panic("Lease") }
func (s *stubIngestTaskRepo) GetByJobID(_ context.Context, jobID string) (*taskgraph.Task, error) {
	for i := range s.listTasks {
		if s.listTasks[i].JobID == jobID {
			cp := s.listTasks[i]
			return &cp, nil
		}
	}
	return nil, nil
}
func (s *stubIngestTaskRepo) ClaimNextReadyTask(context.Context, string, string) (*taskgraph.TaskWithSpec, error) {
	panic("ClaimNextReadyTask")
}
func (s *stubIngestTaskRepo) ReleaseLease(context.Context, string, string, string) error {
	panic("ReleaseLease")
}
func (s *stubIngestTaskRepo) Start(context.Context, string, string, string, int, int) error {
	panic("Start")
}
func (s *stubIngestTaskRepo) Fail(context.Context, string, string, int) error { panic("Fail") }
func (s *stubIngestTaskRepo) IncrementAttempt(context.Context, string) error {
	panic("IncrementAttempt")
}
func (s *stubIngestTaskRepo) AreDependenciesSatisfied(context.Context, []string) (bool, error) {
	panic("AreDependenciesSatisfied")
}
func (s *stubIngestTaskRepo) AcceptTaskAtomic(context.Context, *taskattempts.TaskAttempt, int) error {
	panic("AcceptTaskAtomic")
}
func (s *stubIngestTaskRepo) RenewLease(context.Context, string, string, string, time.Time, int) error {
	panic("RenewLease")
}
func (s *stubIngestTaskRepo) ExpireTaskLeaseAtomic(context.Context, string, string, string, int) (taskgraph.ExpireResult, error) {
	panic("ExpireTaskLeaseAtomic")
}
func (s *stubIngestTaskRepo) RequeueExpiredLeases(context.Context, string, int) ([]taskgraph.RequeueCandidate, error) {
	panic("RequeueExpiredLeases")
}
func (s *stubIngestTaskRepo) Delete(context.Context, string) error { panic("Delete") }
func (s *stubIngestTaskRepo) IsAllAttemptCommitsCommittedForTasks(context.Context, []string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.allCommitsCommitted, nil
}
func (s *stubIngestTaskRepo) ClaimNextWithAttemptAtomic(context.Context, string, string) (*taskgraph.TaskWithSpec, *taskattempts.TaskAttempt, error) {
	panic("ClaimNextWithAttemptAtomic")
}
func (s *stubIngestTaskRepo) ListReadyCandidates(context.Context, int) ([]placement.TaskCandidate, error) {
	panic("ListReadyCandidates")
}
func (s *stubIngestTaskRepo) ClaimTaskForWorkerAtomic(context.Context, taskgraph.ClaimTaskForWorkerCommand) (*taskgraph.TaskWithSpec, *taskattempts.TaskAttempt, error) {
	panic("ClaimTaskForWorkerAtomic")
}
func (s *stubIngestTaskRepo) IngestTaskResultAtomic(_ context.Context, cmd taskgraph.IngestResultCommand) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.transitionCalls++
	s.transitionedTask = cmd.TaskID
	s.transitionedState = cmd.TaskStatus
	if s.transitionErr != nil {
		return s.transitionErr
	}
	for i := range s.listTasks {
		if s.listTasks[i].ID == cmd.TaskID {
			s.listTasks[i].Status = cmd.TaskStatus
		}
	}
	return nil
}

var _ taskgraph.Repository = (*stubIngestTaskRepo)(nil)

type stubIngestJobsRepo struct {
	mu               sync.Mutex
	getJob           *jobs.Job
	setStatusCalls   int
	lastFrom, lastTo jobs.Status
	lastSetErr       error
}

func (s *stubIngestJobsRepo) Get(context.Context, string) (*jobs.Job, error) {
	if s.getJob == nil {
		return nil, errors.New("job not found")
	}
	cp := *s.getJob
	return &cp, nil
}
func (s *stubIngestJobsRepo) Counts(context.Context) (jobs.Counts, error) { return jobs.Counts{}, nil }
func (s *stubIngestJobsRepo) List(context.Context, jobs.Filter) ([]jobs.Job, error) {
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
func (s *stubIngestJobsRepo) Cancel(context.Context, string, string, int) error { panic("Cancel") }
func (s *stubIngestJobsRepo) Fail(context.Context, string, string) error        { panic("Fail") }
func (s *stubIngestJobsRepo) Delete(context.Context, string) error              { panic("Delete") }

var _ jobs.Repository = (*stubIngestJobsRepo)(nil)

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
	if _, ok := s.items[key]; ok {
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

var _ taskoutput_artifacts.Repository = (*stubIngestOutputArtifacts)(nil)

type stubIngestAttemptRepo struct {
	mu                                                       sync.Mutex
	attempts                                                 map[string]*taskattempts.TaskAttempt
	lookupErr                                                error
	persistMetricsCalls, persistCacheCalls, persistCostCalls int
	lastMetrics                                              taskattempts.AttemptMetrics
	lastCacheStats                                           taskattempts.AttemptCacheStats
	lastCostBasis                                            taskattempts.AttemptCostBasis
}

func (s *stubIngestAttemptRepo) seedAttempt(taskID, workerID, leaseID string) {
	s.seedAttemptWithNumber(taskID, workerID, leaseID, "A1", "J1", 1)
}
func (s *stubIngestAttemptRepo) seedAttemptWithNumber(taskID, workerID, leaseID, attemptID, jobID string, n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attempts == nil {
		s.attempts = map[string]*taskattempts.TaskAttempt{}
	}
	s.attempts[taskID+"|"+workerID+"|"+leaseID] = &taskattempts.TaskAttempt{ID: attemptID, TaskID: taskID, WorkerID: workerID, LeaseID: leaseID, AttemptNumber: n, JobID: jobID, Status: taskattempts.AttemptStatusRunning}
}
func (s *stubIngestAttemptRepo) GetByTaskIDAndWorkerAndLease(_ context.Context, taskID, workerID, leaseID string) (*taskattempts.TaskAttempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lookupErr != nil {
		return nil, s.lookupErr
	}
	if a, ok := s.attempts[taskID+"|"+workerID+"|"+leaseID]; ok {
		cp := *a
		return &cp, nil
	}
	return nil, nil
}
func (s *stubIngestAttemptRepo) Get(_ context.Context, id string) (*taskattempts.TaskAttempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, a := range s.attempts {
		if a.ID == id {
			cp := *a
			return &cp, nil
		}
	}
	return nil, nil
}
func (s *stubIngestAttemptRepo) ListByTaskID(context.Context, string) ([]taskattempts.TaskAttempt, error) {
	panic("ListByTaskID")
}
func (s *stubIngestAttemptRepo) GetActiveAttempt(context.Context, string) (*taskattempts.TaskAttempt, error) {
	panic("GetActiveAttempt")
}
func (s *stubIngestAttemptRepo) Create(context.Context, *taskattempts.TaskAttempt) error {
	panic("Create")
}
func (s *stubIngestAttemptRepo) SetStatus(context.Context, string, taskattempts.AttemptStatus, taskattempts.AttemptStatus, int) error {
	panic("SetStatus")
}
func (s *stubIngestAttemptRepo) CompleteFinal(context.Context, string, string, string, taskattempts.AttemptStatus, string, string, int) error {
	panic("CompleteFinal")
}
func (s *stubIngestAttemptRepo) Delete(context.Context, string) error { panic("Delete") }
func (s *stubIngestAttemptRepo) PersistMetrics(_ context.Context, m taskattempts.AttemptMetrics) error {
	s.persistMetricsCalls++
	s.lastMetrics = m
	return nil
}
func (s *stubIngestAttemptRepo) PersistCacheStats(_ context.Context, c taskattempts.AttemptCacheStats) error {
	s.persistCacheCalls++
	s.lastCacheStats = c
	return nil
}
func (s *stubIngestAttemptRepo) PersistCostBasis(_ context.Context, b taskattempts.AttemptCostBasis) error {
	s.persistCostCalls++
	s.lastCostBasis = b
	return nil
}
func (s *stubIngestAttemptRepo) GetMetrics(context.Context, string) (*taskattempts.AttemptMetrics, error) {
	panic("GetMetrics")
}
func (s *stubIngestAttemptRepo) GetCacheStats(context.Context, string) (*taskattempts.AttemptCacheStats, error) {
	panic("GetCacheStats")
}
func (s *stubIngestAttemptRepo) GetCostBasis(context.Context, string) (*taskattempts.AttemptCostBasis, error) {
	panic("GetCostBasis")
}
func (s *stubIngestAttemptRepo) PersistPhaseTimingsDetailed(context.Context, string, []taskattempts.PhaseTimingDetailed) error {
	return nil
}
func (s *stubIngestAttemptRepo) PersistSegmentTimings(context.Context, string, []taskattempts.SegmentTiming) error {
	return nil
}

var _ taskattempts.Repository = (*stubIngestAttemptRepo)(nil)

func newWiredSvc(t *testing.T, taskRepo *stubIngestTaskRepo, jobsRepo *stubIngestJobsRepo, attemptRepo *stubIngestAttemptRepo, out *stubIngestOutputArtifacts) *TaskReportIngestionService {
	t.Helper()
	attemptRepo.seedAttempt("T1", "w-1", "L1")
	taskRepo.allCommitsCommitted = true
	svc, err := NewTaskReportIngestionService(taskRepo, jobsRepo, attemptRepo, out)
	if err != nil {
		t.Fatal(err)
	}
	return svc
}
