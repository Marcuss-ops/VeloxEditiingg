package observability

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

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

func TestService_SummarizeTaskIncludesAttemptFailureDetails(t *testing.T) {
	svc, tasks, _, _, _ := newTestService()
	tasks.tasks["T-2"] = &taskgraph.Task{ID: "T-2", JobID: "J-2", Status: taskgraph.StatusFailed, ExecutorID: "scene.composite.v1", AttemptCount: 2}

	result, err := svc.SummarizeTask(context.Background(), "T-2")
	if err != nil {
		t.Fatalf("SummarizeTask() error: %v", err)
	}
	if len(result.Attempts) != 2 {
		t.Fatalf("attempts = %d, want 2", len(result.Attempts))
	}
	if got := result.Attempts[0].ErrorCode; got != "ASSET_DOWNLOAD_FAILED" {
		t.Fatalf("error_code = %q, want ASSET_DOWNLOAD_FAILED", got)
	}
	if got := result.Attempts[0].ErrorMessage; got != "asset download failed" {
		t.Fatalf("error_message = %q, want canonical message", got)
	}
}

func TestService_SummarizeTaskReadErrorsFailClosed(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*Service, *stubTaskReader, *stubAttemptReader) error
	}{
		{
			name: "cache stats",
			setup: func(_ *Service, _ *stubTaskReader, attempts *stubAttemptReader) error {
				attempts.cacheErr = errors.New("cache stats unavailable")
				return attempts.cacheErr
			},
		},
		{
			name: "live runtime",
			setup: func(svc *Service, _ *stubTaskReader, _ *stubAttemptReader) error {
				want := errors.New("live runtime unavailable")
				svc.WithLiveAttempts(failingLiveAttemptReader{err: want})
				return want
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, tasks, attempts, _, _ := newTestService()
			tasks.tasks["T-read-error"] = &taskgraph.Task{ID: "T-read-error", JobID: "J-read-error", Status: taskgraph.StatusRunning, AttemptCount: 1}
			attempts.attempts["T-read-error"] = []taskattempts.TaskAttempt{{ID: "A-read-error", TaskID: "T-read-error", JobID: "J-read-error", AttemptNumber: 1, WorkerID: "worker-01", Status: taskattempts.AttemptStatusRunning}}
			want := tt.setup(svc, tasks, attempts)
			_, err := svc.SummarizeTask(context.Background(), "T-read-error")
			if err == nil || !errors.Is(err, want) {
				t.Fatalf("SummarizeTask() error = %v, want wrapped %v", err, want)
			}
		})
	}
}

type failingLiveAttemptReader struct{ err error }

func (r failingLiveAttemptReader) GetWorkerTaskRuntimeByJob(context.Context, string) (*LiveAttempt, error) {
	return nil, r.err
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

	_, err := svc.Overview(context.Background())
	if err == nil || !errors.Is(err, tasks.listErr) {
		t.Fatalf("Overview() error = %v, want wrapped task-list error", err)
	}
}

func TestService_Overview_ReadErrorsFailClosed(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*Service, *stubTaskReader, *stubAttemptReader, *stubJobReader, *stubWorkerReader) error
	}{
		{
			name: "job counts",
			setup: func(_ *Service, _ *stubTaskReader, _ *stubAttemptReader, jobs *stubJobReader, _ *stubWorkerReader) error {
				jobs.err = errors.New("counts unavailable")
				return jobs.err
			},
		},
		{
			name: "workers",
			setup: func(_ *Service, _ *stubTaskReader, _ *stubAttemptReader, _ *stubJobReader, workers *stubWorkerReader) error {
				workers.err = errors.New("workers unavailable")
				return workers.err
			},
		},
		{
			name: "attempts",
			setup: func(_ *Service, _ *stubTaskReader, attempts *stubAttemptReader, _ *stubJobReader, _ *stubWorkerReader) error {
				attempts.listErr = errors.New("attempts unavailable")
				return attempts.listErr
			},
		},
		{
			name: "phase timings",
			setup: func(_ *Service, _ *stubTaskReader, attempts *stubAttemptReader, _ *stubJobReader, _ *stubWorkerReader) error {
				attempts.phaseErr = errors.New("timings unavailable")
				return attempts.phaseErr
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, tasks, attempts, jobs, workers := newTestService()
			want := tt.setup(svc, tasks, attempts, jobs, workers)
			_, err := svc.Overview(context.Background())
			if err == nil || !errors.Is(err, want) {
				t.Fatalf("Overview() error = %v, want wrapped %v", err, want)
			}
		})
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

func TestService_ListWorkers_ReadErrorsFailClosed(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*stubTaskReader, *stubAttemptReader, *stubWorkerReader) error
	}{
		{
			name: "tasks",
			setup: func(tasks *stubTaskReader, _ *stubAttemptReader, _ *stubWorkerReader) error {
				tasks.listErr = errors.New("tasks unavailable")
				return tasks.listErr
			},
		},
		{
			name: "attempts",
			setup: func(_ *stubTaskReader, attempts *stubAttemptReader, _ *stubWorkerReader) error {
				attempts.listErr = errors.New("attempts unavailable")
				return attempts.listErr
			},
		},
		{
			name: "phase timings",
			setup: func(_ *stubTaskReader, attempts *stubAttemptReader, _ *stubWorkerReader) error {
				attempts.phaseErr = errors.New("timings unavailable")
				return attempts.phaseErr
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, tasks, attempts, _, workers := newTestService()
			want := tt.setup(tasks, attempts, workers)
			_, err := svc.ListWorkers(context.Background())
			if err == nil || !errors.Is(err, want) {
				t.Fatalf("ListWorkers() error = %v, want wrapped %v", err, want)
			}
		})
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

func TestApplyLiveAttemptOverlayPreservesDurableFields(t *testing.T) {
	target := AttemptSummary{
		AttemptID: "attempt-durable", AttemptNumber: 2,
		Status: taskattempts.AttemptStatusFailed, WorkerID: "worker-durable",
		ErrorCode: "FINAL_ERROR", ErrorMessage: "durable failure",
		StartedAt: "2026-08-10T10:00:00Z", CompletedAt: "2026-08-10T10:04:00Z",
		Metrics: &taskattempts.AttemptMetrics{AttemptID: "attempt-durable", FramesEncoded: 12},
	}
	applyLiveAttemptOverlay(&target, &LiveAttempt{
		AttemptID: "attempt-live", WorkerID: "worker-live", RuntimeStatus: "RUNNING",
		ProgressPhase: "render", ProgressPercent: 80, FramesEncoded: 999,
		StartedAt: "2026-08-10T09:00:00Z", LastProgressAt: "2026-08-10T10:03:00Z",
	})
	if !target.Live || target.Status != taskattempts.AttemptStatusFailed || target.WorkerID != "worker-durable" || target.ErrorCode != "FINAL_ERROR" || target.ErrorMessage != "durable failure" {
		t.Fatalf("overlay replaced durable identity/status/error: %#v", target)
	}
	if target.StartedAt != "2026-08-10T10:00:00Z" || target.CompletedAt != "2026-08-10T10:04:00Z" || target.Metrics == nil || target.Metrics.FramesEncoded != 12 {
		t.Fatalf("overlay replaced durable timestamps/metrics: %#v", target)
	}
	if target.Phase != "render" || target.ProgressPercent != 80 || target.FramesEncoded != 999 {
		t.Fatalf("overlay did not apply volatile progress: %#v", target)
	}
}

func TestService_SummarizeTaskIncludesLiveAttemptProgress(t *testing.T) {
	svc, tasks, _, _, _ := newTestService()
	tasks.tasks["T-live"] = &taskgraph.Task{ID: "T-live", JobID: "J-live", Status: taskgraph.StatusRunning, AttemptCount: 1}
	svc.WithLiveAttempts(stubLiveAttemptReader{live: &LiveAttempt{
		TaskID: "T-live", JobID: "J-live", AttemptID: "A-live", AttemptNumber: 1,
		WorkerID: "worker-live", RuntimeStatus: "RUNNING", ProgressPercent: 46,
		ProgressPhase: "building_segments", CurrentScene: 7, TotalScenes: 13,
		CurrentSegment: 12, TotalSegments: 26, FramesEncoded: 18432,
		FFmpegSpeedX: 2.37, ElapsedMS: 183421, LastProgressAt: "2026-08-10T10:03:42Z",
	}})

	result, err := svc.SummarizeTask(context.Background(), "T-live")
	if err != nil {
		t.Fatalf("SummarizeTask() error: %v", err)
	}
	if len(result.Attempts) != 1 || !result.Attempts[0].Live || result.Attempts[0].AttemptID != "A-live" {
		t.Fatalf("live attempts = %#v", result.Attempts)
	}
	live := result.Attempts[0]
	if live.Phase != "building_segments" || live.CurrentScene != 7 || live.CurrentSegment != 12 || live.FramesEncoded != 18432 || live.LastProgressAt == "" {
		t.Fatalf("live attempt projection = %#v", live)
	}
	if live.Metrics == nil || live.Metrics.AttemptID != "A-live" || live.Metrics.FramesEncoded != 18432 || live.Metrics.FramesDecoded != 0 {
		t.Fatalf("live metrics projection = %#v; expected the same typed AttemptMetrics shape used by final ingestion", live.Metrics)
	}
	if result.AttemptID != "A-live" || result.WorkerID != "worker-live" || result.Phase != "building_segments" || result.LastProgressAt != "2026-08-10T10:03:42Z" {
		t.Fatalf("top-level live execution identity = attempt=%q worker=%q phase=%q last_progress_at=%q; want the same canonical Attempt projection",
			result.AttemptID, result.WorkerID, result.Phase, result.LastProgressAt)
	}
	if result.Progress == nil || result.Progress.Percent != 46 || result.Progress.Scene != 7 || result.Progress.ScenesTotal != 13 || result.Progress.Segment != 12 || result.Progress.SegmentsTotal != 26 {
		t.Fatalf("top-level live progress = %#v; want canonical scene/segment projection", result.Progress)
	}
	if result.LiveMetrics == nil || result.LiveMetrics.ElapsedMS != 183421 || result.LiveMetrics.FramesEncoded != 18432 || result.LiveMetrics.FFmpegSpeedX != 2.37 {
		t.Fatalf("top-level live metrics = %#v; want canonical cumulative metrics projection", result.LiveMetrics)
	}
}

func TestService_SummarizeTaskOmitsLiveExecutionFieldsForLegacyJob(t *testing.T) {
	svc, tasks, _, _, _ := newTestService()
	tasks.tasks["T-legacy"] = &taskgraph.Task{ID: "T-legacy", JobID: "J-legacy", Status: taskgraph.StatusSucceeded, AttemptCount: 1}

	result, err := svc.SummarizeTask(context.Background(), "T-legacy")
	if err != nil {
		t.Fatalf("SummarizeTask() error: %v", err)
	}
	if result.AttemptID != "" || result.WorkerID != "" || result.Phase != "" || result.Progress != nil || result.LiveMetrics != nil || result.LastProgressAt != "" {
		t.Fatalf("legacy execution unexpectedly contains live fields: %#v", result)
	}
}

func TestService_SummarizeTaskTerminalAttemptWinsOverStaleLiveProjection(t *testing.T) {
	svc, tasks, attempts, _, _ := newTestService()
	now := time.Date(2026, 8, 10, 10, 5, 0, 0, time.UTC)
	tasks.tasks["T-converged"] = &taskgraph.Task{
		ID: "T-converged", JobID: "J-converged", Status: taskgraph.StatusSucceeded, AttemptCount: 1,
	}
	attempts.attempts["T-converged"] = []taskattempts.TaskAttempt{{
		ID: "A-converged", TaskID: "T-converged", JobID: "J-converged", AttemptNumber: 1,
		WorkerID: "worker-final", Status: taskattempts.AttemptStatusSucceeded,
		StartedAt: &now,
	}}
	attempts.metrics["A-converged"] = &taskattempts.AttemptMetrics{
		AttemptID: "A-converged", InputBytes: 100, OutputBytes: 80, FramesEncoded: 10,
	}
	// Simulate a heartbeat/runtime cleanup race: the volatile row still has
	// the same Attempt identity, but its progress is an older RUNNING view.
	svc.WithLiveAttempts(stubLiveAttemptReader{live: &LiveAttempt{
		TaskID: "T-converged", JobID: "J-converged", AttemptID: "A-converged", AttemptNumber: 1,
		WorkerID: "worker-stale", RuntimeStatus: "RUNNING", ProgressPercent: 46,
		ProgressPhase: "building_segments", FramesEncoded: 999, ElapsedMS: 9999,
	}})

	result, err := svc.SummarizeTask(context.Background(), "T-converged")
	if err != nil {
		t.Fatalf("SummarizeTask() error: %v", err)
	}
	if len(result.Attempts) != 1 {
		t.Fatalf("attempts = %#v, want one canonical attempt", result.Attempts)
	}
	got := result.Attempts[0]
	if got.Live {
		t.Fatalf("terminal attempt incorrectly marked live: %#v", got)
	}
	if got.Status != taskattempts.AttemptStatusSucceeded || got.WorkerID != "worker-final" {
		t.Fatalf("terminal durable identity/status overwritten by stale live row: %#v", got)
	}
	if got.Metrics == nil || got.Metrics.FramesEncoded != 10 || result.TotalOutputBytes != 80 {
		t.Fatalf("final durable metrics did not converge: attempt=%#v summary=%#v", got, result)
	}
	if result.AttemptID != "" || result.WorkerID != "" || result.Phase != "" || result.Progress != nil || result.LiveMetrics != nil || result.LastProgressAt != "" {
		t.Fatalf("stale live execution leaked into top-level summary: %#v", result)
	}
}

func TestService_SummarizeTaskUsesAttemptLifecycleForWallClock(t *testing.T) {
	svc, tasks, attempts, _, _ := newTestService()
	started := time.Date(2026, 8, 16, 16, 42, 19, 0, time.UTC)
	completed := started.Add(3*time.Minute + 14*time.Second)
	tasks.tasks["T-telemetry-clock"] = &taskgraph.Task{
		ID: "T-telemetry-clock", JobID: "J-telemetry-clock",
		Status: taskgraph.StatusSucceeded, AttemptCount: 1,
	}
	attempts.attempts["T-telemetry-clock"] = []taskattempts.TaskAttempt{{
		ID: "A-telemetry-clock", TaskID: "T-telemetry-clock", JobID: "J-telemetry-clock",
		AttemptNumber: 1, WorkerID: "worker-clock", Status: taskattempts.AttemptStatusSucceeded,
		StartedAt: &started, CompletedAt: &completed,
	}}
	attempts.phaseTimings["A-telemetry-clock"] = []taskattempts.PhaseTiming{
		{AttemptID: "A-telemetry-clock", Phase: "render", DurationMS: 190000,
			WallStart: started, WallEnd: started.Add(190 * time.Second)},
		// Regression fixture: a 1ms finalize event whose wall end was
		// accidentally stamped five minutes late.
		{AttemptID: "A-telemetry-clock", Phase: "finalize", DurationMS: 1,
			WallStart: completed, WallEnd: completed.Add(5 * time.Minute)},
	}
	attempts.metrics["A-telemetry-clock"] = &taskattempts.AttemptMetrics{
		AttemptID: "A-telemetry-clock", WallClockSeconds: 486.072,
	}

	result, err := svc.SummarizeTask(context.Background(), "T-telemetry-clock")
	if err != nil {
		t.Fatalf("SummarizeTask() error: %v", err)
	}
	if len(result.Attempts) != 1 {
		t.Fatalf("attempts = %#v, want one attempt", result.Attempts)
	}
	got := result.Attempts[0]
	wantMS := int64((3*time.Minute + 14*time.Second) / time.Millisecond)
	if got.DurationMS != wantMS {
		t.Fatalf("attempt duration = %dms, want lifecycle duration %dms", got.DurationMS, wantMS)
	}
	if result.TotalWallTimeMS != wantMS {
		t.Fatalf("total wall time = %dms, want lifecycle duration %dms", result.TotalWallTimeMS, wantMS)
	}
	if got.Metrics == nil || math.Abs(got.Metrics.WallClockSeconds-float64(wantMS)/1000) > 1e-9 {
		t.Fatalf("wall_clock_seconds = %#v, want lifecycle duration %.3f", got.Metrics, float64(wantMS)/1000)
	}
}

func TestService_SummarizeJobLiveAttemptIdentityIsImmediateAndUnique(t *testing.T) {
	svc, tasks, attempts, _, _ := newTestService()
	tasks.tasks["T-live-admin"] = &taskgraph.Task{ID: "T-live-admin", JobID: "J-live-admin", Status: taskgraph.StatusRunning, AttemptCount: 1}
	attempts.attempts["T-live-admin"] = []taskattempts.TaskAttempt{{
		ID: "A-live-admin", TaskID: "T-live-admin", JobID: "J-live-admin", AttemptNumber: 1,
		WorkerID: "worker-admin", Status: taskattempts.AttemptStatusRunning,
	}}
	svc.WithLiveAttempts(stubLiveAttemptReader{live: &LiveAttempt{
		TaskID: "T-live-admin", JobID: "J-live-admin", AttemptID: "A-live-admin", AttemptNumber: 1,
		WorkerID: "worker-admin", RuntimeStatus: "RUNNING", StartedAt: "2026-08-10T10:00:00Z",
	}})

	result, err := svc.SummarizeJob(context.Background(), "J-live-admin")
	if err != nil {
		t.Fatalf("SummarizeJob() error: %v", err)
	}
	if len(result.Attempts) != 1 {
		t.Fatalf("attempts = %#v, want one canonical live Attempt", result.Attempts)
	}
	live := result.Attempts[0]
	if !live.Live || live.WorkerID != "worker-admin" || live.AttemptID != "A-live-admin" || live.StartedAt == "" {
		t.Fatalf("canonical live Attempt = %#v; worker_id, attempt_id and started_at must be immediate and non-empty", live)
	}
}

func TestService_SummarizeTaskDropsOlderLiveAttemptAfterRetry(t *testing.T) {
	svc, tasks, attempts, _, _ := newTestService()
	tasks.tasks["T-retry-live"] = &taskgraph.Task{
		ID: "T-retry-live", JobID: "J-retry-live", Status: taskgraph.StatusRunning, AttemptCount: 2,
	}
	attempts.attempts["T-retry-live"] = []taskattempts.TaskAttempt{{
		ID: "A-retry-new", TaskID: "T-retry-live", JobID: "J-retry-live", AttemptNumber: 2,
		WorkerID: "worker-new", Status: taskattempts.AttemptStatusRunning,
	}}
	svc.WithLiveAttempts(stubLiveAttemptReader{live: &LiveAttempt{
		TaskID: "T-retry-live", JobID: "J-retry-live", AttemptID: "A-retry-old", AttemptNumber: 1,
		WorkerID: "worker-old", RuntimeStatus: "RUNNING", ProgressPercent: 82,
		ProgressPhase: "building_segments", LastProgressAt: "2026-08-10T10:03:42Z",
	}})

	result, err := svc.SummarizeTask(context.Background(), "T-retry-live")
	if err != nil {
		t.Fatalf("SummarizeTask() error: %v", err)
	}
	if len(result.Attempts) != 1 || result.Attempts[0].AttemptID != "A-retry-new" {
		t.Fatalf("older live attempt was appended after retry: %#v", result.Attempts)
	}
	if result.AttemptID != "" || result.WorkerID != "" || result.Progress != nil || result.LastProgressAt != "" {
		t.Fatalf("older live attempt shadowed the retry at top level: %#v", result)
	}
}

func TestService_SummarizeTaskDropsDisconnectedLiveAttempt(t *testing.T) {
	svc, tasks, _, _, _ := newTestService()
	tasks.tasks["T-disconnected-live"] = &taskgraph.Task{
		ID: "T-disconnected-live", JobID: "J-disconnected-live", Status: taskgraph.StatusRunning, AttemptCount: 1,
	}
	svc.WithLiveAttempts(stubLiveAttemptReader{live: &LiveAttempt{
		TaskID: "T-disconnected-live", JobID: "J-disconnected-live", AttemptID: "A-disconnected", AttemptNumber: 1,
		WorkerID: "worker-disconnected", RuntimeStatus: "PARTITIONED_SUSPECTED", ProgressPercent: 74,
		ProgressPhase: "building_segments", LastProgressAt: "2026-08-10T10:03:42Z",
	}})

	result, err := svc.SummarizeTask(context.Background(), "T-disconnected-live")
	if err != nil {
		t.Fatalf("SummarizeTask() error: %v", err)
	}
	if len(result.Attempts) != 0 {
		t.Fatalf("partitioned runtime was exposed as a live attempt: %#v", result.Attempts)
	}
	if result.AttemptID != "" || result.WorkerID != "" || result.Progress != nil || result.LiveMetrics != nil || result.LastProgressAt != "" {
		t.Fatalf("partitioned runtime leaked into top-level live projection: %#v", result)
	}
}

func TestService_SummarizeTaskDropsLiveRuntimeFromAnotherTask(t *testing.T) {
	svc, tasks, _, _, _ := newTestService()
	tasks.tasks["T-current"] = &taskgraph.Task{ID: "T-current", JobID: "J-multi", Status: taskgraph.StatusRunning, AttemptCount: 1}
	svc.WithLiveAttempts(stubLiveAttemptReader{live: &LiveAttempt{
		TaskID: "T-other", JobID: "J-multi", AttemptID: "A-other", AttemptNumber: 1,
		WorkerID: "worker-other", RuntimeStatus: "RUNNING", ProgressPercent: 90,
	}})

	result, err := svc.SummarizeTask(context.Background(), "T-current")
	if err != nil {
		t.Fatalf("SummarizeTask() error: %v", err)
	}
	if len(result.Attempts) != 0 || result.AttemptID != "" || result.WorkerID != "" {
		t.Fatalf("runtime from another task was exposed: %#v", result)
	}
}

func TestService_LiveRuntimeStatusesMapToRunningAttemptStatus(t *testing.T) {
	for _, runtimeStatus := range []string{"ACCEPTED", "STARTING", "RUNNING", "CANCELLING", "UPLOADING", "FINALIZING"} {
		if got := liveAttemptStatus(&LiveAttempt{RuntimeStatus: runtimeStatus}); got != taskattempts.AttemptStatusRunning {
			t.Fatalf("runtime status %q mapped to %q; want RUNNING", runtimeStatus, got)
		}
	}
}

func TestService_SummarizeTaskDropsRuntimeForPartitionedWorker(t *testing.T) {
	svc, tasks, _, _, _ := newTestService()
	tasks.tasks["T-worker-partitioned"] = &taskgraph.Task{
		ID: "T-worker-partitioned", JobID: "J-worker-partitioned", Status: taskgraph.StatusRunning, AttemptCount: 1,
	}
	svc.WithLiveAttempts(stubLiveAttemptReader{live: &LiveAttempt{
		TaskID: "T-worker-partitioned", JobID: "J-worker-partitioned", AttemptID: "A-worker-partitioned", AttemptNumber: 1,
		WorkerID: "worker-partitioned", RuntimeStatus: "RUNNING", WorkerConnectionState: "PARTITIONED",
		ProgressPercent: 50, ProgressPhase: "building_segments",
	}})

	result, err := svc.SummarizeTask(context.Background(), "T-worker-partitioned")
	if err != nil {
		t.Fatalf("SummarizeTask() error: %v", err)
	}
	if len(result.Attempts) != 0 || result.AttemptID != "" || result.WorkerID != "" || result.Progress != nil {
		t.Fatalf("runtime from partitioned worker was exposed as live: %#v", result)
	}
}

func TestService_SummarizeTaskKeepsMissingDurableAttemptAsTemporaryOverlay(t *testing.T) {
	svc, tasks, _, _, _ := newTestService()
	tasks.tasks["T-missing-durable"] = &taskgraph.Task{ID: "T-missing-durable", JobID: "J-missing-durable", Status: taskgraph.StatusRunning, AttemptCount: 1}
	svc.WithLiveAttempts(stubLiveAttemptReader{live: &LiveAttempt{
		TaskID: "T-missing-durable", JobID: "J-missing-durable", AttemptID: "A-missing-durable", AttemptNumber: 1,
		WorkerID: "worker-live", RuntimeStatus: "RUNNING", ProgressPercent: 50,
	}})
	result, err := svc.SummarizeTask(context.Background(), "T-missing-durable")
	if err != nil {
		t.Fatalf("SummarizeTask() error: %v", err)
	}
	if len(result.Attempts) != 1 || !result.Attempts[0].Live || result.Attempts[0].AttemptID != "A-missing-durable" {
		t.Fatalf("missing durable row should remain a temporary live overlay: %#v", result)
	}
	if result.AttemptID != "A-missing-durable" || result.Progress == nil || result.Progress.Percent != 50 {
		t.Fatalf("temporary overlay was not projected consistently: %#v", result)
	}
}

func TestService_SummarizeTaskDropsUnmatchedLiveAttemptForTerminalTask(t *testing.T) {
	svc, tasks, _, _, _ := newTestService()
	tasks.tasks["T-terminal-ghost"] = &taskgraph.Task{
		ID: "T-terminal-ghost", JobID: "J-terminal-ghost", Status: taskgraph.StatusFailed, AttemptCount: 1,
	}
	svc.WithLiveAttempts(stubLiveAttemptReader{live: &LiveAttempt{
		TaskID: "T-terminal-ghost", JobID: "J-terminal-ghost", AttemptID: "A-terminal-ghost", AttemptNumber: 1,
		WorkerID: "worker-ghost", RuntimeStatus: "RUNNING", ProgressPercent: 50,
	}})

	result, err := svc.SummarizeTask(context.Background(), "T-terminal-ghost")
	if err != nil {
		t.Fatalf("SummarizeTask() error: %v", err)
	}
	if len(result.Attempts) != 0 {
		t.Fatalf("unmatched live attempt survived terminal task: %#v", result.Attempts)
	}
	if result.AttemptID != "" || result.WorkerID != "" || result.Progress != nil || result.LiveMetrics != nil {
		t.Fatalf("terminal ghost leaked into top-level projection: %#v", result)
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

func TestService_SummarizeTask_RollsUpDownloadVolumeInCacheSummary(t *testing.T) {
	svc, tasks, attempts, _, _ := newTestService()
	tasks.tasks["T-cache-dl"] = &taskgraph.Task{ID: "T-cache-dl", JobID: "J-cache-dl", Status: taskgraph.StatusSucceeded, AttemptCount: 1}
	attempts.attempts["T-cache-dl"] = []taskattempts.TaskAttempt{{
		ID: "A-cache-dl", TaskID: "T-cache-dl", JobID: "J-cache-dl", AttemptNumber: 1,
		WorkerID: "worker-01", Status: taskattempts.AttemptStatusSucceeded,
	}}
	attempts.cacheStats = map[string]*taskattempts.AttemptCacheStats{
		"A-cache-dl": {
			AttemptID:          "A-cache-dl",
			CacheHits:          10,
			CacheMisses:        2,
			CacheLookups:       12,
			CacheDownloadCount: 2,
			CacheDownloadBytes: 2 * 524288,
		},
	}

	result, err := svc.SummarizeTask(context.Background(), "T-cache-dl")
	if err != nil {
		t.Fatalf("SummarizeTask() error: %v", err)
	}
	if result.Cache.DownloadCount != 2 || result.Cache.DownloadBytes != 2*524288 {
		t.Fatalf("CacheSummary download volume = %d/%d; want 2/%d (migration 147 columns must roll up)", result.Cache.DownloadCount, result.Cache.DownloadBytes, 2*524288)
	}
	if result.Cache.Lookups != 12 || result.Cache.HitRatio != 10.0/12.0 {
		t.Fatalf("CacheSummary lookups/hit_ratio = %d/%.3f; want 12/%.3f", result.Cache.Lookups, result.Cache.HitRatio, 10.0/12.0)
	}
}

func TestService_SummarizeJob_NotFound(t *testing.T) {
	svc, _, _, _, _ := newTestService()

	_, err := svc.SummarizeJob(context.Background(), "nonexistent")
	if err == nil {
		t.Error("SummarizeJob() should return error for nonexistent job")
	}
}

func TestService_RecentScalarMetric_DerivedMetrics(t *testing.T) {
	svc, tasks, attempts, _, _ := newTestService()
	tasks.tasks["T-1"] = &taskgraph.Task{ID: "T-1", JobID: "J-1", Status: taskgraph.StatusSucceeded, AttemptCount: 1}
	attempts.metrics["A-1"] = &taskattempts.AttemptMetrics{
		AttemptID:             "A-1",
		WallClockSeconds:      30,
		MediaDurationSeconds:  60,
		EngineSegmentBuildMs:  3000,
		CPUTimeMS:             60000,
		EngineAssetDownloadMs: 2000,
		BytesFromBlobstore:    4_000_000,
		BytesFromDrive:        2_000_000,
	}

	result, err := svc.RecentScalarMetric(context.Background(), "render_factor")
	if err != nil {
		t.Fatalf("RecentScalarMetric(render_factor) error: %v", err)
	}
	if result.Samples != 1 {
		t.Fatalf("Samples = %d, want 1", result.Samples)
	}
	// 30 / 60 = 0.5
	if math.Abs(result.Avg-0.5) > 1e-9 {
		t.Errorf("Avg = %v, want 0.5", result.Avg)
	}

	// 60s media = 1 output minute; 3000ms encode -> 3000 ms/min.
	result, err = svc.RecentScalarMetric(context.Background(), "encode_ms_per_output_minute")
	if err != nil {
		t.Fatalf("RecentScalarMetric(encode_ms_per_output_minute) error: %v", err)
	}
	if math.Abs(result.Avg-3000.0) > 1e-9 {
		t.Errorf("Avg = %v, want 3000", result.Avg)
	}

	// 60s media = 1 output minute; 60000ms cpu -> 60000 ms/min.
	result, err = svc.RecentScalarMetric(context.Background(), "cpu_ms_per_output_minute")
	if err != nil {
		t.Fatalf("RecentScalarMetric(cpu_ms_per_output_minute) error: %v", err)
	}
	if math.Abs(result.Avg-60000.0) > 1e-9 {
		t.Errorf("Avg = %v, want 60000", result.Avg)
	}

	// 6_000_000 bytes / 2s = 3_000_000 bytes/sec.
	result, err = svc.RecentScalarMetric(context.Background(), "download_throughput")
	if err != nil {
		t.Fatalf("RecentScalarMetric(download_throughput) error: %v", err)
	}
	if math.Abs(result.Avg-3_000_000.0) > 1e-9 {
		t.Errorf("Avg = %v, want 3000000", result.Avg)
	}

	// cache_hit_ratio requires cache stats.
	attempts.cacheStats = map[string]*taskattempts.AttemptCacheStats{
		"A-1": {CacheHits: 75, CacheMisses: 25},
	}
	result, err = svc.RecentScalarMetric(context.Background(), "cache_hit_ratio")
	if err != nil {
		t.Fatalf("RecentScalarMetric(cache_hit_ratio) error: %v", err)
	}
	if result.Samples != 1 {
		t.Fatalf("Samples = %d, want 1", result.Samples)
	}
	if math.Abs(result.Avg-0.75) > 1e-9 {
		t.Errorf("Avg = %v, want 0.75", result.Avg)
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

func TestRollupPhaseTimings(t *testing.T) {
	mk := func(dur int64, phase string, start, end time.Time) taskattempts.PhaseTiming {
		return taskattempts.PhaseTiming{AttemptID: "A", Phase: phase, DurationMS: dur, WallStart: start, WallEnd: end}
	}
	base := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		timings     []taskattempts.PhaseTiming
		wantDur     int64
		wantTotals  map[string]int64
		wantSnaps   int
		wantBreakdn map[string]int64
	}{
		{
			name:        "empty",
			timings:     nil,
			wantDur:     0,
			wantTotals:  map[string]int64{},
			wantSnaps:   0,
			wantBreakdn: map[string]int64{},
		},
		{
			name: "wall bounds derive duration",
			timings: []taskattempts.PhaseTiming{
				mk(100, "render", base, base.Add(10*time.Second)),
				mk(50, "encode", base.Add(2*time.Second), base.Add(14*time.Second)),
			},
			wantDur:     14000,
			wantTotals:  map[string]int64{"render": 100, "encode": 50},
			wantSnaps:   2,
			wantBreakdn: map[string]int64{"render": 100, "encode": 50},
		},
		{
			name: "out-of-order bounds still span min/max",
			timings: []taskattempts.PhaseTiming{
				mk(30, "encode", base.Add(20*time.Second), base.Add(25*time.Second)),
				mk(30, "render", base, base.Add(10*time.Second)),
			},
			wantDur:     25000,
			wantTotals:  map[string]int64{"encode": 30, "render": 30},
			wantSnaps:   2,
			wantBreakdn: map[string]int64{"encode": 30, "render": 30},
		},
		{
			name: "no wall bounds falls back to sum",
			timings: []taskattempts.PhaseTiming{
				mk(100, "render", time.Time{}, time.Time{}),
				mk(50, "encode", time.Time{}, time.Time{}),
			},
			wantDur:     150,
			wantTotals:  map[string]int64{"render": 100, "encode": 50},
			wantSnaps:   2,
			wantBreakdn: map[string]int64{"render": 100, "encode": 50},
		},
		{
			name: "partial bounds ignore zero timestamps",
			timings: []taskattempts.PhaseTiming{
				mk(100, "render", base, base.Add(10*time.Second)),
				mk(50, "quality", time.Time{}, time.Time{}),
			},
			wantDur:     10000,
			wantTotals:  map[string]int64{"render": 100, "quality": 50},
			wantSnaps:   2,
			wantBreakdn: map[string]int64{"render": 100, "quality": 50},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			as := &AttemptSummary{PhaseBreakdown: make(map[string]int64)}
			summary := &ExecutionSummary{PhaseTotals: make(map[string]int64)}
			got := rollupPhaseTimings(tt.timings, as, summary)
			if got != tt.wantDur {
				t.Fatalf("duration = %d, want %d", got, tt.wantDur)
			}
			if len(summary.PhaseTimings) != tt.wantSnaps {
				t.Fatalf("PhaseTimings len = %d, want %d", len(summary.PhaseTimings), tt.wantSnaps)
			}
			for phase, want := range tt.wantTotals {
				if summary.PhaseTotals[phase] != want {
					t.Errorf("PhaseTotals[%q] = %d, want %d", phase, summary.PhaseTotals[phase], want)
				}
				if as.PhaseBreakdown[phase] != want {
					t.Errorf("PhaseBreakdown[%q] = %d, want %d", phase, as.PhaseBreakdown[phase], want)
				}
			}
		})
	}
}

func TestMergeWallBounds(t *testing.T) {
	base := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)

	// Starts nil and gets seeded from the first non-zero row.
	firstStart, lastEnd := mergeWallBounds([]taskattempts.PhaseTiming{
		{AttemptID: "A", WallStart: base.Add(5 * time.Second), WallEnd: base.Add(15 * time.Second)},
		{AttemptID: "A", WallStart: base, WallEnd: base.Add(20 * time.Second)},
	}, nil, nil)
	if firstStart == nil || lastEnd == nil {
		t.Fatal("expected non-nil bounds after seeding")
	}
	if !firstStart.Equal(base) || !lastEnd.Equal(base.Add(20*time.Second)) {
		t.Fatalf("bounds = %v..%v, want %v..%v", firstStart, lastEnd, base, base.Add(20*time.Second))
	}

	// Zero timestamps never replace a valid bound.
	firstStart, lastEnd = mergeWallBounds([]taskattempts.PhaseTiming{
		{AttemptID: "A", WallStart: time.Time{}, WallEnd: time.Time{}},
	}, firstStart, lastEnd)
	if !firstStart.Equal(base) || !lastEnd.Equal(base.Add(20*time.Second)) {
		t.Fatalf("zero timestamps replaced valid bounds: %v..%v", firstStart, lastEnd)
	}
}
