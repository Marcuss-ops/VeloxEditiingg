package observability

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

// fakeTaskReader is a test double for TaskReader.
type fakeTaskReader struct {
	tasks map[string]*taskgraph.Task
}

func (f *fakeTaskReader) Get(_ context.Context, id string) (*taskgraph.Task, error) {
	t, ok := f.tasks[id]
	if !ok {
		return nil, nil
	}
	return t, nil
}

func (f *fakeTaskReader) GetByJobID(_ context.Context, jobID string) (*taskgraph.Task, error) {
	for _, t := range f.tasks {
		if t.JobID == jobID {
			return t, nil
		}
	}
	return nil, nil
}

func (f *fakeTaskReader) List(_ context.Context, _ taskgraph.Filter) ([]taskgraph.Task, error) {
	var out []taskgraph.Task
	for _, t := range f.tasks {
		out = append(out, *t)
	}
	return out, nil
}

// fakeAttemptReader is a test double for AttemptReader.
type fakeAttemptReader struct {
	attempts map[string]*taskattempts.TaskAttempt
	timings  map[string][]taskattempts.PhaseTiming
	metrics  map[string]*taskattempts.AttemptMetrics
}

func (f *fakeAttemptReader) Get(_ context.Context, id string) (*taskattempts.TaskAttempt, error) {
	a, ok := f.attempts[id]
	if !ok {
		return nil, nil
	}
	return a, nil
}

func (f *fakeAttemptReader) ListByTaskID(_ context.Context, taskID string) ([]taskattempts.TaskAttempt, error) {
	var out []taskattempts.TaskAttempt
	for _, a := range f.attempts {
		if a.TaskID == taskID {
			out = append(out, *a)
		}
	}
	return out, nil
}

func (f *fakeAttemptReader) GetPhaseTimings(_ context.Context, attemptID string) ([]taskattempts.PhaseTiming, error) {
	return f.timings[attemptID], nil
}

func (f *fakeAttemptReader) GetMetrics(_ context.Context, attemptID string) (*taskattempts.AttemptMetrics, error) {
	return f.metrics[attemptID], nil
}

func TestSummarizeTask(t *testing.T) {
	now := time.Now().UTC()
	task := &taskgraph.Task{
		ID:           "t1",
		JobID:        "j1",
		Status:       taskgraph.StatusSucceeded,
		AttemptCount: 2,
	}
	attempt := &taskattempts.TaskAttempt{
		ID:            "a1",
		TaskID:        "t1",
		AttemptNumber: 1,
		Status:        taskattempts.AttemptStatusSucceeded,
		WorkerID:      "w1",
	}
	timings := []taskattempts.PhaseTiming{
		{AttemptID: "a1", Phase: "render", DurationMS: 5000, WallStart: now, WallEnd: now.Add(5 * time.Second)},
		{AttemptID: "a1", Phase: "encode", DurationMS: 2000, WallStart: now.Add(5 * time.Second), WallEnd: now.Add(7 * time.Second)},
	}
	metrics := &taskattempts.AttemptMetrics{
		AttemptID:  "a1",
		InputBytes: 1024,
		CPUTimeMS:  3000,
	}

	tr := &fakeTaskReader{tasks: map[string]*taskgraph.Task{"t1": task}}
	ar := &fakeAttemptReader{
		attempts: map[string]*taskattempts.TaskAttempt{"a1": attempt},
		timings:  map[string][]taskattempts.PhaseTiming{"a1": timings},
		metrics:  map[string]*taskattempts.AttemptMetrics{"a1": metrics},
	}

	svc, err := NewService(tr, ar)
	if err != nil {
		t.Fatal(err)
	}

	summary, err := svc.SummarizeTask(context.Background(), "t1")
	if err != nil {
		t.Fatal(err)
	}

	if summary.TaskID != "t1" {
		t.Errorf("expected task_id t1, got %s", summary.TaskID)
	}
	if summary.AttemptCount != 2 {
		t.Errorf("expected 2 attempts, got %d", summary.AttemptCount)
	}
	if summary.Retries != 1 {
		t.Errorf("expected 1 retry, got %d", summary.Retries)
	}
	if summary.TotalInputBytes != 1024 {
		t.Errorf("expected 1024 input bytes, got %d", summary.TotalInputBytes)
	}
	if summary.PhaseTotals["render"] != 5000 {
		t.Errorf("expected 5000ms render, got %d", summary.PhaseTotals["render"])
	}
	if summary.PhaseTotals["encode"] != 2000 {
		t.Errorf("expected 2000ms encode, got %d", summary.PhaseTotals["encode"])
	}
}

func TestNewServiceValidation(t *testing.T) {
	tr := &fakeTaskReader{tasks: map[string]*taskgraph.Task{}}
	if _, err := NewService(nil, nil); err == nil {
		t.Error("expected error with nil readers")
	}
	if _, err := NewService(tr, nil); err == nil {
		t.Error("expected error with nil attempt reader")
	}
}
