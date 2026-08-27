package observability

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
	sharedtelemetry "velox-shared/telemetry"
)

// TestService_SummarizeTaskMilestonesSurviveVolatileProjectionCleanup locks
// STEP B on the read side: once the canonical TaskResult transaction closed
// the attempt, DeleteWorkerTaskRuntime wipes worker_task_runtime — so the
// durable raw report in task_attempt_reports becomes the ONLY queryable
// source of the milestone timeline. SummarizeTask must keep surfacing the
// full samples list (not just the waterfall buckets) after that cleanup.
func TestService_SummarizeTaskMilestonesSurviveVolatileProjectionCleanup(t *testing.T) {
	svc, tasks, attempts, _, _ := newTestService()
	started := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	completed := started.Add(328*time.Second + 41*time.Millisecond)
	tasks.tasks["T-durable"] = &taskgraph.Task{
		ID: "T-durable", JobID: "J-durable", Status: taskgraph.StatusSucceeded, AttemptCount: 1,
	}
	attempts.attempts["T-durable"] = []taskattempts.TaskAttempt{{
		ID: "A-durable", TaskID: "T-durable", JobID: "J-durable", AttemptNumber: 1,
		WorkerID: "worker-durable", Status: taskattempts.AttemptStatusSucceeded,
		StartedAt: &started, CompletedAt: &completed,
	}}
	attempts.rawReports = map[string]string{"A-durable": realisticAttemptReportJSON}

	result, err := svc.SummarizeTask(context.Background(), "T-durable")
	if err != nil {
		t.Fatalf("SummarizeTask() error: %v", err)
	}
	if len(result.Attempts) != 1 || result.Attempts[0].Live {
		t.Fatalf("attempts = %#v; want one non-live (post-cleanup) attempt", result.Attempts)
	}
	got := result.Attempts[0]
	if len(got.AttemptMilestones) == 0 {
		t.Fatal("attempt_milestones empty after volatile projection cleanup — timeline not queryable from canonical store")
	}
	if want := len(got.AttemptWaterfall.Buckets); want == 0 {
		t.Fatalf("waterfall missing although milestones exist: %#v", got)
	}
	prev := int64(-1)
	for i, sample := range got.AttemptMilestones {
		if !sharedtelemetry.IsCanonicalAttemptMilestone(sample.Name) {
			t.Fatalf("milestone[%d] = %q is not a canonical milestone", i, sample.Name)
		}
		if sample.ElapsedMS < prev {
			t.Fatalf("milestone[%d]=%s elapsed_ms=%d went backwards (prev=%d)", i, sample.Name, sample.ElapsedMS, prev)
		}
		prev = sample.ElapsedMS
	}
	first, last := got.AttemptMilestones[0], got.AttemptMilestones[len(got.AttemptMilestones)-1]
	if first.Name != sharedtelemetry.MilestoneAttemptAccepted || last.Name != sharedtelemetry.MilestoneAttemptCompleted {
		t.Fatalf("durable timeline spine wrong: first=%s last=%s", first.Name, last.Name)
	}
}

// TestService_SummarizeTaskLiveMilestonesTakePrecedenceOverDurableReport pins
// the precedence rule behind the len==0 guard: while the volatile row exists
// the recorder snapshot can be richer than the report frozen at
// result.sending, so the live overlay must win and the durable decode must
// NOT overwrite it.
func TestService_SummarizeTaskLiveMilestonesTakePrecedenceOverDurableReport(t *testing.T) {
	svc, tasks, attempts, _, _ := newTestService()
	started := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	tasks.tasks["T-mix"] = &taskgraph.Task{
		ID: "T-mix", JobID: "J-mix", Status: taskgraph.StatusRunning, AttemptCount: 1,
	}
	attempts.attempts["T-mix"] = []taskattempts.TaskAttempt{{
		ID: "A-mix", TaskID: "T-mix", JobID: "J-mix", AttemptNumber: 1,
		WorkerID: "worker-mix", Status: taskattempts.AttemptStatusRunning,
		StartedAt: &started,
	}}
	attempts.rawReports = map[string]string{"A-mix": realisticAttemptReportJSON}
	svc.WithLiveAttempts(stubLiveAttemptReader{live: &LiveAttempt{
		TaskID: "T-mix", JobID: "J-mix", AttemptID: "A-mix", AttemptNumber: 1,
		WorkerID: "worker-mix", RuntimeStatus: "RUNNING",
		AttemptMilestones: []sharedtelemetry.AttemptMilestoneSample{
			{Name: sharedtelemetry.MilestoneExecutionStarted, Sequence: 1, ElapsedMS: 0},
			{Name: sharedtelemetry.MilestoneAssetsRequested, Sequence: 2, ElapsedMS: 211},
			{Name: sharedtelemetry.MilestoneAllAssetsReady, Sequence: 3, ElapsedMS: 298421},
		},
	}})

	result, err := svc.SummarizeTask(context.Background(), "T-mix")
	if err != nil {
		t.Fatalf("SummarizeTask() error: %v", err)
	}
	if len(result.Attempts) != 1 || !result.Attempts[0].Live {
		t.Fatalf("attempts = %#v; want one live attempt", result.Attempts)
	}
	got := result.Attempts[0]
	if len(got.AttemptMilestones) != 3 {
		t.Fatalf("live milestones must take precedence over the durable report: got %d samples (%+v), want the 3-sample live snapshot", len(got.AttemptMilestones), got.AttemptMilestones)
	}
}
