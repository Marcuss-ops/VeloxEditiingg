package observability

import (
	"context"
	"errors"
	"testing"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

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
