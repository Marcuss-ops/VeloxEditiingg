// =====================================================================
// PR-2 / fix/canonical-attempt-identity — shared handler report fixtures.
// Test cases are split by concern into handler_reports_{identity,metrics,ack}_test.go.
// =====================================================================

package grpcserver

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"velox-server/internal/ingest"
	"velox-server/internal/jobs"
	"velox-server/internal/placement"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
	"velox-server/internal/taskoutput_artifacts"
)

type spoofFixture struct {
	taskID         string
	workerID       string
	canonicalLease string
	wireLease      string
	wireAttemptID  string
	wireJobID      string
}

func newSpoofFixture() spoofFixture {
	return spoofFixture{taskID: "T-spoof", workerID: "w-spoof", canonicalLease: "L-canonical", wireLease: "L-attacker", wireAttemptID: "A-canonical", wireJobID: "J-canonical"}
}

type spoofStubTaskRepo struct {
	mu                  sync.Mutex
	transitionCalls     int
	transitionErr       error
	reportConflictErr   error
	nowTask             taskgraph.Task
	listTasks           []taskgraph.Task
	persistMetricsCalls int
	persistCacheCalls   int
	persistCostCalls    int
	registerCalls       int
	lastMetrics         taskattempts.AttemptMetrics
	lastCacheStats      taskattempts.AttemptCacheStats
	lastCostBasis       taskattempts.AttemptCostBasis
	lastArtifacts       []taskoutput_artifacts.OutputArtifact
}

func (s *spoofStubTaskRepo) Get(_ context.Context, id string) (*taskgraph.Task, error) {
	if s.nowTask.ID == id {
		cp := s.nowTask
		return &cp, nil
	}
	return nil, errors.New("not found (spoof stub)")
}
func (s *spoofStubTaskRepo) List(_ context.Context, _ taskgraph.Filter) ([]taskgraph.Task, error) {
	cp := make([]taskgraph.Task, len(s.listTasks))
	copy(cp, s.listTasks)
	return cp, nil
}
func (s *spoofStubTaskRepo) TransitionTaskToTerminalAtomic(_ context.Context, _, _, _ string, _ taskgraph.Status, _ taskattempts.AttemptStatus, _, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.transitionCalls++
	return s.transitionErr
}
func (s *spoofStubTaskRepo) Create(_ context.Context, _ *taskgraph.Task) error {
	panic("spoofStubTaskRepo.Create")
}
func (s *spoofStubTaskRepo) ListByJobID(_ context.Context, _ string) (*taskgraph.Task, error) {
	panic("spoofStubTaskRepo.ListByJobID")
}
func (s *spoofStubTaskRepo) SetStatus(_ context.Context, _ string, _, _ taskgraph.Status, _ int) error {
	panic("spoofStubTaskRepo.SetStatus")
}
func (s *spoofStubTaskRepo) Lease(_ context.Context, _, _, _ string) error {
	panic("spoofStubTaskRepo.Lease")
}
func (s *spoofStubTaskRepo) GetByJobID(_ context.Context, jobID string) (*taskgraph.Task, error) {
	for i := range s.listTasks {
		if s.listTasks[i].JobID == jobID {
			cp := s.listTasks[i]
			return &cp, nil
		}
	}
	return nil, nil
}
func (s *spoofStubTaskRepo) ClaimNextReadyTask(_ context.Context, _, _ string) (*taskgraph.TaskWithSpec, error) {
	panic("spoofStubTaskRepo.ClaimNextReadyTask")
}
func (s *spoofStubTaskRepo) ReleaseLease(_ context.Context, _, _, _ string) error {
	panic("spoofStubTaskRepo.ReleaseLease")
}
func (s *spoofStubTaskRepo) Start(_ context.Context, _, _, _ string, _, _ int) error {
	panic("spoofStubTaskRepo.Start")
}
func (s *spoofStubTaskRepo) Fail(_ context.Context, _, _ string, _ int) error {
	panic("spoofStubTaskRepo.Fail")
}
func (s *spoofStubTaskRepo) IncrementAttempt(_ context.Context, _ string) error {
	panic("spoofStubTaskRepo.IncrementAttempt")
}
func (s *spoofStubTaskRepo) AreDependenciesSatisfied(_ context.Context, _ []string) (bool, error) {
	panic("spoofStubTaskRepo.AreDependenciesSatisfied")
}
func (s *spoofStubTaskRepo) AcceptTaskAtomic(_ context.Context, _ *taskattempts.TaskAttempt, _ int) error {
	panic("spoofStubTaskRepo.AcceptTaskAtomic")
}
func (s *spoofStubTaskRepo) RenewLease(_ context.Context, _, _, _ string, _ time.Time, _ int) error {
	panic("spoofStubTaskRepo.RenewLease")
}
func (s *spoofStubTaskRepo) ExpireTaskLeaseAtomic(_ context.Context, _, _, _ string, _ int) (taskgraph.ExpireResult, error) {
	panic("spoofStubTaskRepo.ExpireTaskLeaseAtomic")
}
func (s *spoofStubTaskRepo) RequeueExpiredLeases(_ context.Context, _ string, _ int) ([]taskgraph.RequeueCandidate, error) {
	panic("spoofStubTaskRepo.RequeueExpiredLeases")
}
func (s *spoofStubTaskRepo) Delete(_ context.Context, _ string) error {
	panic("spoofStubTaskRepo.Delete")
}
func (s *spoofStubTaskRepo) ClaimNextWithAttemptAtomic(_ context.Context, _, _ string) (*taskgraph.TaskWithSpec, *taskattempts.TaskAttempt, error) {
	panic("spoofStubTaskRepo.ClaimNextWithAttemptAtomic")
}
func (s *spoofStubTaskRepo) ListReadyCandidates(_ context.Context, _ int) ([]placement.TaskCandidate, error) {
	panic("spoofStubTaskRepo.ListReadyCandidates")
}
func (s *spoofStubTaskRepo) ClaimTaskForWorkerAtomic(_ context.Context, _ taskgraph.ClaimTaskForWorkerCommand) (*taskgraph.TaskWithSpec, *taskattempts.TaskAttempt, error) {
	panic("spoofStubTaskRepo.ClaimTaskForWorkerAtomic")
}
func (s *spoofStubTaskRepo) IngestTaskResultAtomic(_ context.Context, cmd taskgraph.IngestResultCommand) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.transitionCalls++
	if s.transitionErr != nil {
		return s.transitionErr
	}
	if s.reportConflictErr != nil {
		return s.reportConflictErr
	}
	s.persistMetricsCalls++
	s.lastMetrics = cmd.Metrics
	s.persistCacheCalls++
	s.lastCacheStats = cmd.CacheStats
	s.persistCostCalls++
	s.lastCostBasis = cmd.CostBasis
	s.registerCalls += len(cmd.Artifacts)
	if cmd.Artifacts != nil {
		s.lastArtifacts = append([]taskoutput_artifacts.OutputArtifact(nil), cmd.Artifacts...)
	}
	return nil
}
func (s *spoofStubTaskRepo) IsAllAttemptCommitsCommittedForTasks(_ context.Context, _ []string) (bool, error) {
	return true, nil
}

var _ taskgraph.Repository = (*spoofStubTaskRepo)(nil)

type spoofStubJobsRepo struct {
	mu             sync.Mutex
	getJob         *jobs.Job
	setStatusCalls int
}

func (s *spoofStubJobsRepo) Get(_ context.Context, _ string) (*jobs.Job, error) {
	if s.getJob == nil {
		return nil, errors.New("job not found (spoof stub)")
	}
	cp := *s.getJob
	return &cp, nil
}
func (s *spoofStubJobsRepo) Counts(_ context.Context) (jobs.Counts, error) { return jobs.Counts{}, nil }
func (s *spoofStubJobsRepo) List(_ context.Context, _ jobs.Filter) ([]jobs.Job, error) {
	if s.getJob == nil {
		return nil, nil
	}
	cp := *s.getJob
	return []jobs.Job{cp}, nil
}
func (s *spoofStubJobsRepo) SetStatus(_ context.Context, _ string, _, _ jobs.Status) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setStatusCalls++
	return nil
}
func (s *spoofStubJobsRepo) Cancel(_ context.Context, _ string, _ string, _ int) error {
	panic("spoofStubJobsRepo.Cancel")
}
func (s *spoofStubJobsRepo) Fail(_ context.Context, _ string, _ string) error {
	panic("spoofStubJobsRepo.Fail")
}
func (s *spoofStubJobsRepo) Delete(_ context.Context, _ string) error {
	panic("spoofStubJobsRepo.Delete")
}

var _ jobs.Repository = (*spoofStubJobsRepo)(nil)

type spoofStubAttemptRepo struct {
	mu                  sync.Mutex
	attempts            map[string]*taskattempts.TaskAttempt
	persistMetricsCalls int
	persistCacheCalls   int
	persistCostCalls    int
	lastMetrics         taskattempts.AttemptMetrics
	lastCacheStats      taskattempts.AttemptCacheStats
	lastCostBasis       taskattempts.AttemptCostBasis
}

func (s *spoofStubAttemptRepo) seedCanonical(taskID, workerID, leaseID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attempts == nil {
		s.attempts = map[string]*taskattempts.TaskAttempt{}
	}
	s.attempts[taskID+"|"+workerID+"|"+leaseID] = &taskattempts.TaskAttempt{ID: "A-canonical", TaskID: taskID, WorkerID: workerID, LeaseID: leaseID, AttemptNumber: 1, JobID: "J-canonical", Status: taskattempts.AttemptStatusRunning}
}
func (s *spoofStubAttemptRepo) GetByTaskIDAndWorkerAndLease(_ context.Context, taskID, workerID, leaseID string) (*taskattempts.TaskAttempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if att, ok := s.attempts[taskID+"|"+workerID+"|"+leaseID]; ok {
		cp := *att
		return &cp, nil
	}
	return nil, nil
}
func (s *spoofStubAttemptRepo) Get(_ context.Context, id string) (*taskattempts.TaskAttempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, att := range s.attempts {
		if att.ID == id {
			cp := *att
			return &cp, nil
		}
	}
	return nil, nil
}
func (s *spoofStubAttemptRepo) ListByTaskID(_ context.Context, _ string) ([]taskattempts.TaskAttempt, error) {
	panic("spoofStubAttemptRepo.ListByTaskID")
}
func (s *spoofStubAttemptRepo) GetActiveAttempt(_ context.Context, _ string) (*taskattempts.TaskAttempt, error) {
	panic("spoofStubAttemptRepo.GetActiveAttempt")
}
func (s *spoofStubAttemptRepo) Create(_ context.Context, _ *taskattempts.TaskAttempt) error {
	panic("spoofStubAttemptRepo.Create")
}
func (s *spoofStubAttemptRepo) SetStatus(_ context.Context, _ string, _, _ taskattempts.AttemptStatus, _ int) error {
	panic("spoofStubAttemptRepo.SetStatus")
}
func (s *spoofStubAttemptRepo) CompleteFinal(_ context.Context, _, _, _ string, _ taskattempts.AttemptStatus, _, _ string, _ int) error {
	panic("spoofStubAttemptRepo.CompleteFinal")
}
func (s *spoofStubAttemptRepo) Delete(_ context.Context, _ string) error {
	panic("spoofStubAttemptRepo.Delete")
}
func (s *spoofStubAttemptRepo) PersistMetrics(_ context.Context, m taskattempts.AttemptMetrics) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.persistMetricsCalls++
	s.lastMetrics = m
	return nil
}
func (s *spoofStubAttemptRepo) PersistCacheStats(_ context.Context, c taskattempts.AttemptCacheStats) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.persistCacheCalls++
	s.lastCacheStats = c
	return nil
}
func (s *spoofStubAttemptRepo) PersistCostBasis(_ context.Context, b taskattempts.AttemptCostBasis) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.persistCostCalls++
	s.lastCostBasis = b
	return nil
}
func (s *spoofStubAttemptRepo) GetMetrics(_ context.Context, _ string) (*taskattempts.AttemptMetrics, error) {
	panic("spoofStubAttemptRepo.GetMetrics")
}
func (s *spoofStubAttemptRepo) GetCacheStats(_ context.Context, _ string) (*taskattempts.AttemptCacheStats, error) {
	panic("spoofStubAttemptRepo.GetCacheStats")
}
func (s *spoofStubAttemptRepo) GetCostBasis(_ context.Context, _ string) (*taskattempts.AttemptCostBasis, error) {
	panic("spoofStubAttemptRepo.GetCostBasis")
}
func (s *spoofStubAttemptRepo) PersistPhaseTimingsDetailed(_ context.Context, _ string, _ []taskattempts.PhaseTimingDetailed) error {
	return nil
}
func (s *spoofStubAttemptRepo) PersistSegmentTimings(_ context.Context, _ string, _ []taskattempts.SegmentTiming) error {
	return nil
}

var _ taskattempts.Repository = (*spoofStubAttemptRepo)(nil)

type spoofStubOutputArts struct {
	mu            sync.Mutex
	registerCalls int
	items         map[string]taskoutput_artifacts.OutputArtifact
}

func newSpoofStubOutputArts() *spoofStubOutputArts {
	return &spoofStubOutputArts{items: map[string]taskoutput_artifacts.OutputArtifact{}}
}
func (s *spoofStubOutputArts) Register(_ context.Context, a taskoutput_artifacts.OutputArtifact) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.registerCalls++
	if s.items == nil {
		s.items = map[string]taskoutput_artifacts.OutputArtifact{}
	}
	s.items[a.TaskID+"|"+a.ArtifactID] = a
	return nil
}
func (s *spoofStubOutputArts) ListByTask(_ context.Context, taskID string) ([]taskoutput_artifacts.OutputArtifact, error) {
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

var _ taskoutput_artifacts.Repository = (*spoofStubOutputArts)(nil)

func buildSpoofHandler(t *testing.T) (*Handler, *spoofStubTaskRepo, *spoofStubJobsRepo, *spoofStubOutputArts) {
	t.Helper()
	fx := newSpoofFixture()
	attempts := &spoofStubAttemptRepo{}
	attempts.seedCanonical(fx.taskID, fx.workerID, fx.canonicalLease)
	taskRepo := &spoofStubTaskRepo{listTasks: []taskgraph.Task{{ID: fx.taskID, JobID: fx.wireJobID, Status: taskgraph.StatusSucceeded}}}
	jobsRepo := &spoofStubJobsRepo{getJob: &jobs.Job{ID: fx.wireJobID, Status: jobs.StatusRunning, MaxRetries: 3, Revision: 0}}
	outputArts := newSpoofStubOutputArts()
	svc, err := ingest.NewTaskReportIngestionService(taskRepo, jobsRepo, attempts, outputArts)
	if err != nil {
		t.Fatalf("NewTaskReportIngestionService: %v", err)
	}
	handler := NewHandler(nil, nil, jobsRepo, taskRepo, attempts, nil, nil, &HandlerConfig{PushMode: true})
	handler.SetIngestionSvc(svc)
	return handler, taskRepo, jobsRepo, outputArts
}
