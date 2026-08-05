// Package ingest / job_transitions.go — job-side roll-up transitions.
// Extracted from service.go: the maybeTransitionJob helper and its
// Phase 2.8 commit-presence gate allTasksCommitted.
package ingest

import (
	"context"
	"fmt"

	"velox-server/internal/jobs"
	"velox-server/internal/statemachine"
	"velox-server/internal/taskgraph"
)

// maybeTransitionJob mirrors the helpers introduced in PR-4 + #5 with
// the Phase 2.8 gating: when all sibling tasks are terminal AND each
// succeeded task has an attempt_commits row with status='COMMITTED',
// flip the Job to AWAITING_ARTIFACT. If any task failed, the Job
// moves to FAILED. PR-02 / Phase 2.5: SUCCEEDED on the Job itself is
// reserved for Coordinator.CommitAttempt and we still do NOT write it
// here.
//
// Returns (transitioned, newStatus, err):
//
//	transitioned=true when a SetStatus write really fired;
//	newStatus is the post-state (also populated on idempotency no-op
//	so the handler can report "already at AWAITING_ARTIFACT" honestly).
func (s *TaskReportIngestionService) maybeTransitionJob(ctx context.Context, jobID string, allSucceeded bool) (bool, string, error) {
	job, err := s.jobsRepo.Get(ctx, jobID)
	if err != nil || job == nil {
		return false, "", err
	}
	if job.Status.IsTerminal() {
		// Already terminal — report the current status so the handler
		// logs "Job is in terminal state, no transition needed".
		return false, string(job.Status), nil
	}

	tasks, err := s.taskRepo.List(ctx, taskgraph.Filter{JobIDs: []string{jobID}})
	if err != nil {
		return false, "", fmt.Errorf("list tasks for job %s: %w", jobID, err)
	}
	if len(tasks) == 0 {
		return false, string(job.Status), nil
	}

	allTerminal := true
	anyFailed := false
	anyHardFailed := false
	anyCancelled := false
	allSucceededAndCommitted := true
	for _, t := range tasks {
		if !t.Status.IsTerminal() {
			allTerminal = false
			break
		}
		if t.Status == taskgraph.StatusFailed || t.Status == taskgraph.StatusCancelled {
			anyFailed = true
			allSucceededAndCommitted = false
			if t.Status == taskgraph.StatusCancelled {
				anyCancelled = true
			}
			if t.Status == taskgraph.StatusFailed {
				anyHardFailed = true
			}
		}
	}
	if !allTerminal {
		return false, string(job.Status), nil
	}

	// Phase 2.8 guard: a Task is "succeeded-and-committed" only when
	// status='SUCCEEDED' AND an attempt_commits row exists for it
	// with status='COMMITTED'. RUNNING+winning_attempt_terminal_pending
	// = FALSE (the Ingest path left it there temporarily); the commit
	// protocol must ratify it before the Job promotes. Until that
	// happens, the Job stays at RUNNING.
	if allSucceeded && !anyFailed {
		allSucceededAndCommitted, err = s.allTasksCommitted(ctx, tasks)
		if err != nil {
			return false, string(job.Status), fmt.Errorf("check task commits for job %s: %w", jobID, err)
		}
	} else {
		allSucceededAndCommitted = false
	}

	var newStatus jobs.Status
	if allSucceededAndCommitted {
		newStatus = jobs.StatusAwaitingArtifact
	} else if anyCancelled && !anyHardFailed {
		newStatus = jobs.StatusCancelled
	} else if anyFailed {
		newStatus = jobs.StatusFailed
	} else {
		// allSucceeded AND !anyFailed but the commit-protocol gate
		// block suceeded-only-by-terminal_pending. Stay RUNNING until
		// CommitAttempt ratifies.
		return false, string(job.Status), nil
	}

	// PR-02 idempotency: skip a spurious re-write. We return
	// (transitioned=false, observed_status) on this branch so the
	// handler does not double-log "transitioned Job X" when goroutine
	// B unblocks AFTER goroutine A already wrote AWAITING_ARTIFACT.
	if job.Status == newStatus {
		return false, string(job.Status), nil
	}

	if s.jobTransitions == nil {
		return false, string(job.Status), fmt.Errorf("job transition service is not configured")
	}
	actor := statemachine.ActorSystem
	if newStatus == jobs.StatusCancelled {
		actor = statemachine.ActorOperator
	}
	if setErr := s.jobTransitions.Transition(ctx, jobID, job.Status, newStatus, actor); setErr != nil {
		return false, string(job.Status), fmt.Errorf("Transition %s→%s: %w", job.Status, newStatus, setErr)
	}
	s.logger.Printf("[INGEST] job %s transitioned %s → %s (all sibling tasks terminal)", jobID, job.Status, newStatus)
	return true, string(newStatus), nil
}

// allTasksCommitted returns true iff every Task in `tasks` has an
// attempt_commits row with status='COMMITTED'. Phase 2.8: this is the
// gating condition for AWAITING_ARTIFACT roll-up — pre-Phase-2 the
// roll-up fired as soon as TaskStatus='SUCCEEDED', which produced the
// "Task SUCCEEDED, Job AWAITING_ARTIFACT, no artifact READY"
// impossible state the closure-gate preserves.
func (s *TaskReportIngestionService) allTasksCommitted(ctx context.Context, tasks []taskgraph.Task) (bool, error) {
	if len(tasks) == 0 {
		return false, nil
	}
	taskRepo, ok := s.taskRepo.(interface {
		IsAllAttemptCommitsCommittedForTasks(ctx context.Context, taskIDs []string) (bool, error)
	})
	if !ok {
		return false, fmt.Errorf("ingest.allTasksCommitted: taskRepo %T does not expose commit-presence check (Phase 2.8 wiring)", s.taskRepo)
	}
	ids := make([]string, 0, len(tasks))
	for _, t := range tasks {
		if t.Status == taskgraph.StatusSucceeded {
			ids = append(ids, t.ID)
		}
	}
	if len(ids) == 0 {
		return false, nil
	}
	return taskRepo.IsAllAttemptCommitsCommittedForTasks(ctx, ids)
}
