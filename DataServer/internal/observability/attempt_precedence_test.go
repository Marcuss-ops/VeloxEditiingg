package observability

import (
	"testing"

	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

func TestLiveAttemptIsEligible_DurableTerminalStateWins(t *testing.T) {
	tests := []struct {
		name          string
		taskStatus    taskgraph.Status
		durableStatus taskattempts.AttemptStatus
		want          bool
	}{
		{
			name:          "durable succeeded suppresses stale running live row",
			taskStatus:    taskgraph.StatusSucceeded,
			durableStatus: taskattempts.AttemptStatusSucceeded,
			want:          false,
		},
		{
			name:          "durable failed suppresses stale running live row",
			taskStatus:    taskgraph.StatusRunning,
			durableStatus: taskattempts.AttemptStatusFailed,
			want:          false,
		},
		{
			name:          "durable cancelled suppresses stale running live row",
			taskStatus:    taskgraph.StatusRunning,
			durableStatus: taskattempts.AttemptStatusCancelled,
			want:          false,
		},
		{
			name:          "nonterminal durable attempt permits live overlay",
			taskStatus:    taskgraph.StatusRunning,
			durableStatus: taskattempts.AttemptStatusRunning,
			want:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			live := &LiveAttempt{
				TaskID:        "task-1",
				JobID:         "job-1",
				AttemptID:     "attempt-1",
				AttemptNumber: 1,
				RuntimeStatus: "RUNNING",
			}
			task := &taskgraph.Task{ID: "task-1", JobID: "job-1", Status: tt.taskStatus, AttemptCount: 1}
			attempts := []taskattempts.TaskAttempt{{
				ID: "attempt-1", TaskID: "task-1", JobID: "job-1", AttemptNumber: 1,
				Status: tt.durableStatus,
			}}
			if got := liveAttemptIsEligible(live, task, attempts); got != tt.want {
				t.Fatalf("liveAttemptIsEligible() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLiveAttemptIsEligible_DurableTerminalCheckIsOrderIndependent(t *testing.T) {
	live := &LiveAttempt{
		TaskID:        "task-1",
		JobID:         "job-1",
		AttemptID:     "attempt-1",
		AttemptNumber: 1,
		RuntimeStatus: "RUNNING",
	}
	task := &taskgraph.Task{ID: "task-1", JobID: "job-1", Status: taskgraph.StatusRunning, AttemptCount: 1}
	attempts := []taskattempts.TaskAttempt{
		{ID: "attempt-2", TaskID: "task-1", JobID: "job-1", AttemptNumber: 2, Status: taskattempts.AttemptStatusRunning},
		{ID: "attempt-1", TaskID: "task-1", JobID: "job-1", AttemptNumber: 1, Status: taskattempts.AttemptStatusSucceeded},
	}

	if liveAttemptIsEligible(live, task, attempts) {
		t.Fatal("stale live attempt was eligible when its durable terminal row was not first")
	}
}

func TestLiveAttemptIsEligible_AllowsClaimAcceptVisibilityWindow(t *testing.T) {
	live := &LiveAttempt{
		TaskID:        "task-1",
		JobID:         "job-1",
		AttemptID:     "attempt-2",
		AttemptNumber: 2,
		RuntimeStatus: "ACCEPTED",
	}
	task := &taskgraph.Task{ID: "task-1", JobID: "job-1", Status: taskgraph.StatusRunning, AttemptCount: 2}

	if !liveAttemptIsEligible(live, task, nil) {
		t.Fatal("new live attempt was rejected during claim-to-accept visibility window")
	}
}
