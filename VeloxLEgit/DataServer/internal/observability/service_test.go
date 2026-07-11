package observability

import (
	"context"
	"errors"
	"testing"

	"velox-server/internal/jobs"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

// ── Stub implementations ──────────────────────────────────────────────────

type stubTaskReader struct {
	tasks       map[string]*taskgraph.Task
	listResult  []taskgraph.Task
	listErr     error
}

func (s *stubTaskReader) Get(_ context.Context, id string) (*taskgraph.Task, error) {
	t, ok := s.tasks[id]
	if !ok {
		return nil, nil
	}
	return t, nil
}

func (s *stubTaskReader) GetByJobID(_ context.Context, jobID string) (*taskgraph.Task, error) {
	for _, t := range s.tasks {
		if t.JobID == jobID {
			return t, nil
		}
	}
	return nil, nil
}

func (s *stubTaskReader) List(_ context.Context, _ taskgraph.Filter) ([]taskgraph.Task, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	return s.listResult, nil
}

type stubAttemptReader struct {
	attempts      map[string][]taskattempts.TaskAttempt
	phaseTimings  map[string][]taskattempts.PhaseTiming
	metrics       map[string]*taskattempts.AttemptMetrics
	listErr       error
	phaseErr      error
	metricsErr    error
}

func (s *stubAttemptReader) Get(_ context.Context, id string) (*taskattempts.TaskAttempt, error) {
	return nil, nil
}

func (s *stubAttemptReader) ListByTaskID(_ context.Context, taskID string) ([]taskattempts.TaskAttempt, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	return s.attempts[taskID], nil
}

func (s *stubAttemptReader) GetPhaseTimings(_ context.Context, attemptID string) ([]taskattempts.PhaseTiming, error) {
	if s.phaseErr != nil {
		return nil, s.phaseErr
	}
	return s.phaseTimings[attemptID], nil
}

func (s *stubAttemptReader) GetMetrics(_ context.Context, attemptID string) (*taskattempts.AttemptMetrics, error) {
	if s.metricsErr != nil {
		return nil, s.metricsErr
	}
	return s.metrics[attemptID], nil
}

type stubJobReader struct {
	counts jobs.Counts
	err    error
}

func (s *stubJobReader) Get(_ context.Context, _ string) (*jobs.Job, error) {
	return nil, nil
}

func (s *stubJobReader) List(_ context.Context, _ jobs.Filter) ([]jobs.Job, error) {
	return nil, nil
}

func (s *stubJobReader) Counts(_ context.Context) (jobs.Counts, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.counts, nil
}

type stubWorkerReader struct {
	workers []map[string]any
	err     error
}

func (s *stubWorkerReader) ListWorkers() ([]map[string]any, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.workers, nil
}

func (s *stubWorkerReader) GetWorker(_ string) (map[string]any, error) {
	return nil, nil
}

func newTestService() (*Service, *stubTaskReader, *stubAttemptReader, *stubJobReader, *stubWorkerReader) {
	tasks := &stubTaskReader{
		tasks: map[string]*taskgraph.Task{},
		listResult: []taskgraph.Task{
			{ID: "T-1", JobID: "J-1", Status: taskgraph.StatusSucceeded, ExecutorID: "scene.composite.v1", AttemptCount: 1},
			{ID: "T-2", JobID: "J-2", Status: taskgraph.StatusFailed, ExecutorID: "scene.composite.v1", AttemptCount: 2},
			{ID: "T-3", JobID: "J-3", Status: taskgraph.StatusSucceeded, ExecutorID: "scene.composite.v1", AttemptCount: 1},
		},
	}
	attempts := &stubAttemptReader{
		attempts: map[string][]taskattempts.TaskAttempt{
			"T-1": {
				{ID: "A-1", TaskID: "T-1", WorkerID: "worker-01", Status: taskattempts.AttemptStatusSucceeded, AttemptNumber: 1},
			},
			"T-2": {
				{ID: "A-2", TaskID: "T-2", WorkerID: "worker-02", Status: taskattempts.AttemptStatusFailed, AttemptNumber: 1, ErrorCode: "ASSET_DOWNLOAD_FAILED"},
				{ID: "A-2b", TaskID: "T-2", WorkerID: "worker-02", Status: taskattempts.AttemptStatusFailed, AttemptNumber: 2},
			},
			"T-3": {
				{ID: "A-3", TaskID: "T-3", WorkerID: "worker-01", Status: taskattempts.AttemptStatusSucceeded, AttemptNumber: 1},
			},
		},
		phaseTimings: map[string][]taskattempts.PhaseTiming{
			"A-1": {
				{AttemptID: "A-1", Phase: "render", DurationMS: 120000},
				{AttemptID: "A-1", Phase: "encode", DurationMS: 45000},
				{AttemptID: "A-1", Phase: "upload", DurationMS: 15000},
			},
			"A-2": {
				{AttemptID: "A-2", Phase: "cache_lookup", DurationMS: 2000},
				{AttemptID: "A-2", Phase: "download", DurationMS: 50000},
				{AttemptID: "A-2", Phase: "render", DurationMS: 80000},
			},
			"A-2b": {
				{AttemptID: "A-2b", Phase: "render", DurationMS: 90000},
			},
			"A-3": {
				{AttemptID: "A-3", Phase: "render", DurationMS: 60000},
			},
		},
		metrics: map[string]*taskattempts.AttemptMetrics{
			"A-1": {AttemptID: "A-1", InputBytes: 52428800, OutputBytes: 26214400, CPUTimeMS: 120000},
		},
	}
	jobs := &stubJobReader{
		counts: jobs.Counts{
			jobs.StatusSucceeded:  10,
			jobs.StatusFailed:     2,
			jobs.StatusPending:    5,
			jobs.StatusRunning:    3,
			jobs.StatusCancelled:  1,
		},
	}
	workers := &stubWorkerReader{
		workers: []map[string]any{
			{"worker_id": "worker-01", "worker_name": "Worker One", "status": "idle", "last_heartbeat": "2026-07-06T10:00:00Z"},
			{"worker_id": "worker-02", "worker_name": "Worker Two", "status": "busy", "last_heartbeat": "2026-07-06T09:55:00Z"},
			{"worker_id": "worker-03", "worker_name": "Worker Three", "status": "idle", "last_heartbeat": "2026-07-06T09:50:00Z"},
		},
	}

	svc, _ := NewService(tasks, attempts)
	svc.WithJobs(jobs).WithWorkers(workers)
	return svc, tasks, attempts, jobs, workers
}

// ── Tests ─────────────────────────────────────────────────────────────────

func TestService_Overview(t *testing.T) {
	svc, _, _, _, _ := newTestService()

	result, err := svc.Overview(context.Background())
	if err != nil {
		t.Fatalf("Overview() error: %v", err)
	}

	if result.JobsCompleted24h != 10 {
		t.Errorf("JobsCompleted24h = %d, want 10", result.JobsCompleted24h)
	}
	if result.JobsFailed24h != 3 {
		t.Errorf("JobsFailed24h = %d, want 3", result.JobsFailed24h)
	}
	if result.ActiveWorkers != 3 {
		t.Errorf("ActiveWorkers = %d, want 3", result.ActiveWorkers)
	}
	if result.QueueDepth != 8 {
		t.Errorf("QueueDepth = %d, want 8 (5 pending + 3 running)", result.QueueDepth)
	}
	if len(result.TopSlowPhases) == 0 {
		t.Error("TopSlowPhases should not be empty")
	}
	if len(result.TopSlowWorkers) == 0 {
		t.Error("TopSlowWorkers should not be empty")
	}
	// Verify error stats include the failed task's error code.
	found := false
	for _, e := range result.TopErrors {
		if e.ErrorCode == "ASSET_DOWNLOAD_FAILED" {
			found = true
			break
		}
	}
	if !found {
		t.Error("TopErrors should include ASSET_DOWNLOAD_FAILED")
	}
}

func TestService_Overview_NilWorkers(t *testing.T) {
	svc, _, _, _, _ := newTestService()
	svc.workers = nil

	result, err := svc.Overview(context.Background())
	if err != nil {
		t.Fatalf("Overview() with nil workers error: %v", err)
	}
	if result.ActiveWorkers != 0 {
		t.Errorf("ActiveWorkers with nil workers = %d, want 0", result.ActiveWorkers)
	}
}

func TestService_Overview_NilJobs(t *testing.T) {
	svc, _, _, _, _ := newTestService()
	svc.jobs = nil

	result, err := svc.Overview(context.Background())
	if err != nil {
		t.Fatalf("Overview() with nil jobs error: %v", err)
	}
	if result.JobsCompleted24h != 0 {
		t.Errorf("JobsCompleted24h with nil jobs = %d, want 0", result.JobsCompleted24h)
	}
}

func TestService_Overview_ListTasksError(t *testing.T) {
	svc, tasks, _, _, _ := newTestService()
	tasks.listErr = errors.New("db down")

	result, err := svc.Overview(context.Background())
	if err != nil {
		t.Fatalf("Overview() should tolerate List error: %v", err)
	}
	if result == nil {
		t.Fatal("result should not be nil")
	}
}

func TestService_ListWorkers(t *testing.T) {
	svc, _, _, _, _ := newTestService()

	workers, err := svc.ListWorkers(context.Background())
	if err != nil {
		t.Fatalf("ListWorkers() error: %v", err)
	}

	if len(workers) != 3 {
		t.Fatalf("ListWorkers() returned %d workers, want 3", len(workers))
	}

	// worker-01 has 2 jobs (T-1, T-3), both succeeded.
	w1 := findWorker(workers, "worker-01")
	if w1 == nil {
		t.Fatal("worker-01 not found")
	}
	if w1.JobCount != 2 {
		t.Errorf("worker-01 JobCount = %d, want 2", w1.JobCount)
	}
	if w1.SuccessRate != 100.0 {
		t.Errorf("worker-01 SuccessRate = %.1f, want 100.0", w1.SuccessRate)
	}

	// worker-02 has 2 attempts on T-2 (both failed).
	w2 := findWorker(workers, "worker-02")
	if w2 == nil {
		t.Fatal("worker-02 not found")
	}
	if w2.JobCount != 2 {
		t.Errorf("worker-02 JobCount = %d, want 2 (two attempts)", w2.JobCount)
	}
}

func TestService_ListWorkers_NilWorkerReader(t *testing.T) {
	svc, _, _, _, _ := newTestService()
	svc.workers = nil

	_, err := svc.ListWorkers(context.Background())
	if err == nil {
		t.Error("ListWorkers() with nil worker reader should return error")
	}
}

func TestService_PhaseTrends(t *testing.T) {
	svc, _, _, _, _ := newTestService()

	result, err := svc.PhaseTrends(context.Background(), "render", "")
	if err != nil {
		t.Fatalf("PhaseTrends() error: %v", err)
	}

	if result.Phase != "render" {
		t.Errorf("Phase = %q, want \"render\"", result.Phase)
	}
	if result.Samples == 0 {
		t.Error("Samples should be > 0 for render phase")
	}
	if result.AvgMS <= 0 {
		t.Error("AvgMS should be > 0")
	}
}

func TestService_PhaseTrends_EmptyPhase(t *testing.T) {
	svc, _, _, _, _ := newTestService()

	_, err := svc.PhaseTrends(context.Background(), "", "")
	if err == nil {
		t.Error("PhaseTrends() with empty phase should return error")
	}
}

func TestService_PhaseTrends_WithExecutor(t *testing.T) {
	svc, _, _, _, _ := newTestService()

	result, err := svc.PhaseTrends(context.Background(), "render", "scene.composite.v1")
	if err != nil {
		t.Fatalf("PhaseTrends(filtered) error: %v", err)
	}
	if result.Samples == 0 {
		t.Error("Samples should be > 0 when filtering by executor")
	}
}

func TestService_SummarizeJob(t *testing.T) {
	svc, tasks, _, _, _ := newTestService()
	tasks.tasks["T-1"] = &taskgraph.Task{ID: "T-1", JobID: "J-1", Status: taskgraph.StatusSucceeded, AttemptCount: 1}

	result, err := svc.SummarizeJob(context.Background(), "J-1")
	if err != nil {
		t.Fatalf("SummarizeJob() error: %v", err)
	}
	if result == nil {
		t.Fatal("result should not be nil")
	}
	if result.TaskID != "T-1" {
		t.Errorf("TaskID = %q, want \"T-1\"", result.TaskID)
	}
}

func TestService_SummarizeJob_NotFound(t *testing.T) {
	svc, _, _, _, _ := newTestService()

	_, err := svc.SummarizeJob(context.Background(), "nonexistent")
	if err == nil {
		t.Error("SummarizeJob() should return error for nonexistent job")
	}
}

// ── Helpers ──────────────────────────────────────────────────────────────

func findWorker(workers []WorkerPerformance, id string) *WorkerPerformance {
	for i, w := range workers {
		if w.WorkerID == id {
			return &workers[i]
		}
	}
	return nil
}
