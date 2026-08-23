package observability

import (
	"context"
	"velox-server/internal/jobs"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

// ── Stub implementations ──────────────────────────────────────────────────

type stubTaskReader struct {
	tasks      map[string]*taskgraph.Task
	listResult []taskgraph.Task
	listErr    error
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
	attempts     map[string][]taskattempts.TaskAttempt
	phaseTimings map[string][]taskattempts.PhaseTiming
	metrics      map[string]*taskattempts.AttemptMetrics
	cacheStats   map[string]*taskattempts.AttemptCacheStats
	listErr      error
	phaseErr     error
	cacheErr     error
	metricsErr   error
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

func (s *stubAttemptReader) GetCacheStats(_ context.Context, attemptID string) (*taskattempts.AttemptCacheStats, error) {
	if s.cacheErr != nil {
		return nil, s.cacheErr
	}
	return s.cacheStats[attemptID], nil
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

type stubLiveAttemptReader struct {
	live *LiveAttempt
}

func (r stubLiveAttemptReader) GetWorkerTaskRuntimeByJob(context.Context, string) (*LiveAttempt, error) {
	return r.live, nil
}

func (r stubLiveAttemptReader) GetWorkerTaskRuntimeByTask(context.Context, string, string) (*LiveAttempt, error) {
	return r.live, nil
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
				{ID: "A-2", TaskID: "T-2", WorkerID: "worker-02", Status: taskattempts.AttemptStatusFailed, AttemptNumber: 1, ErrorCode: "ASSET_DOWNLOAD_FAILED", ErrorMessage: "asset download failed"},
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
			jobs.StatusSucceeded: 10,
			jobs.StatusFailed:    2,
			jobs.StatusPending:   5,
			jobs.StatusRunning:   3,
			jobs.StatusCancelled: 1,
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

type failingLiveAttemptReader struct{ err error }

func (r failingLiveAttemptReader) GetWorkerTaskRuntimeByJob(context.Context, string) (*LiveAttempt, error) {
	return nil, r.err
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
