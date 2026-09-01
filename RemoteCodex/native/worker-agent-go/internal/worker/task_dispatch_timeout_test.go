package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/taskrunner"
)

type contextDeadlineProbeExecutor struct {
	mu          sync.Mutex
	deadlineSet bool
	deadline    time.Time
}

func (e *contextDeadlineProbeExecutor) Descriptor() executor.Descriptor {
	return executor.Descriptor{
		ID:            "context.deadline.probe",
		Version:       1,
		ResourceClass: executor.ResourceCPU,
		TemporalMode:  executor.TemporalGlobal,
	}
}

func (e *contextDeadlineProbeExecutor) Validate(executor.TaskSpec) error { return nil }

func (e *contextDeadlineProbeExecutor) Execute(ctx context.Context, _ executor.ExecutionContext, _ executor.TaskSpec) (executor.ExecutionResult, error) {
	e.mu.Lock()
	e.deadline, e.deadlineSet = ctx.Deadline()
	e.mu.Unlock()
	return executor.ExecutionResult{Status: "succeeded"}, nil
}

func (e *contextDeadlineProbeExecutor) observedDeadline() (time.Time, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.deadline, e.deadlineSet
}

func TestRunJobTaskDoesNotInstallGlobalDeadline(t *testing.T) {
	w, _ := newDispatchTestWorker(t)
	probe := &contextDeadlineProbeExecutor{}
	registry := executor.NewRegistry()
	registry.MustRegister(probe)
	w.taskRunner = taskrunner.NewTaskRunner(registry, w.logger)

	pte := &PendingTaskExecution{
		JobID:      "job-context-deadline",
		ExecutorID: "context.deadline.probe",
		Spec: executor.TaskSpec{
			Version:    1,
			JobID:      "job-context-deadline",
			ExecutorID: "context.deadline.probe",
		},
	}
	if _, err := w.runJobTask(context.Background(), pte); err != nil {
		t.Fatalf("runJobTask: %v", err)
	}
	if _, ok := probe.observedDeadline(); ok {
		t.Fatal("runJobTask installed an artificial deadline")
	}

	probe = &contextDeadlineProbeExecutor{}
	registry = executor.NewRegistry()
	registry.MustRegister(probe)
	w.taskRunner = taskrunner.NewTaskRunner(registry, w.logger)
	deadline := time.Now().Add(time.Minute)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	pte.Spec.ExecutorID = "context.deadline.probe"
	if _, err := w.runJobTask(ctx, pte); err != nil {
		t.Fatalf("runJobTask with caller deadline: %v", err)
	}
	got, ok := probe.observedDeadline()
	if !ok {
		t.Fatal("runJobTask dropped the caller-provided deadline")
	}
	if got.Before(deadline.Add(-time.Millisecond)) || got.After(deadline.Add(time.Millisecond)) {
		t.Fatalf("observed deadline %v, want caller deadline %v", got, deadline)
	}
}
