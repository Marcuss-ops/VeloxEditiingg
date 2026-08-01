// Package ingest / timing.go — phase-timing identity canonicalization.
// Extracted from service.go: the canonicalizePhaseTimingIdentity helper.
package ingest

import (
	"context"
	"fmt"

	"velox-server/internal/taskattempts"
)

// canonicalizePhaseTimingIdentity stamps every detailed event with the
// canonical tuple resolved by the master. The worker's job/task/attempt/
// worker/executor echoes are deliberately discarded.
func (s *TaskReportIngestionService) canonicalizePhaseTimingIdentity(ctx context.Context, cmd *IngestCommand) error {
	att, err := s.attemptRepo.Get(ctx, cmd.AttemptID)
	if err != nil {
		return fmt.Errorf("ingest.canonicalizePhaseTimingIdentity: attempt lookup: %w", err)
	}
	if att == nil || att.ID != cmd.AttemptID || att.TaskID != cmd.TaskID || att.WorkerID != cmd.WorkerID || att.LeaseID != cmd.LeaseID || att.JobID != cmd.JobID {
		return fmt.Errorf("ingest.canonicalizePhaseTimingIdentity: canonical attempt mismatch for %s: %w", cmd.AttemptID, taskattempts.ErrIdentityMismatch)
	}
	task, err := s.taskRepo.Get(ctx, cmd.TaskID)
	if err != nil {
		return fmt.Errorf("ingest.canonicalizePhaseTimingIdentity: task lookup: %w", err)
	}
	if task == nil || task.ID != cmd.TaskID || task.JobID != att.JobID {
		return fmt.Errorf("ingest.canonicalizePhaseTimingIdentity: canonical task mismatch for %s: %w", cmd.TaskID, taskattempts.ErrIdentityMismatch)
	}
	cmd.ExecutorID = task.ExecutorID
	cmd.ExecutorVersion = task.ExecutorVersion
	for i := range cmd.PhaseTimings {
		cmd.PhaseTimings[i].AttemptID = att.ID
		cmd.PhaseTimings[i].TaskID = task.ID
		cmd.PhaseTimings[i].JobID = task.JobID
		cmd.PhaseTimings[i].WorkerID = att.WorkerID
		cmd.PhaseTimings[i].WorkerSnapshotID = att.WorkerSnapshotID
		cmd.PhaseTimings[i].ExecutorID = task.ExecutorID
		cmd.PhaseTimings[i].ExecutorVersion = task.ExecutorVersion
	}
	for i := range cmd.PartialPhaseMetrics {
		cmd.PartialPhaseMetrics[i].AttemptID = att.ID
		cmd.PartialPhaseMetrics[i].TaskID = task.ID
		cmd.PartialPhaseMetrics[i].JobID = task.JobID
		cmd.PartialPhaseMetrics[i].WorkerID = att.WorkerID
		cmd.PartialPhaseMetrics[i].WorkerSnapshotID = att.WorkerSnapshotID
		cmd.PartialPhaseMetrics[i].ExecutorID = task.ExecutorID
		cmd.PartialPhaseMetrics[i].ExecutorVersion = task.ExecutorVersion
	}
	return nil
}
