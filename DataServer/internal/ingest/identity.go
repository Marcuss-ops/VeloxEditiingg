// Package ingest / identity.go — audit-mandated wire identity validation.
// Extracted from service.go: the ValidateIdentityTuple gate.
package ingest

import (
	"context"
	"fmt"

	"velox-server/internal/taskattempts"
)

// validateIdentityTuple runs the audit-mandated wire-tuple gate that
// precedes any state-changing write in IngestTaskResult. Returns
// taskattempts.ErrIdentityMismatch (wrapped) when ANY of the canonical
// identity values on the wire mismatches the authoritatively-derived
// row from task_attempts.
//
// Three layers of defense:
//  1. Cheap field presence checks
//     (TaskID/AttemptID/LeaseID/WorkerID + JobID + AttemptNumber>0).
//  2. attemptRepo.GetByTaskIDAndWorkerAndLease lookup — the canonical
//     PR-02 wire-fallback path. If the lookup returns nil AND the task
//     has zero attempts at all, the message is rejected as an
//     impersonation attempt. If it returns nil AND non-terminal attempts
//     exist for the task with a different worker/lease, the message is
//     rejected as lease-revoked stale-worker retry.
//  3. STRICT-COMPARE the wire (attempt_id, attempt_number, job_id)
//     against the canonical row derived from GetByTaskIDAndWorkerAndLease.
//     Any mismatch surfaces as ErrIdentityMismatch — the message cannot
//     be trusted and is DROPPED upstream by handleTaskResult.
//
// Idempotent retry: a worker that already received a successful close
// (attempt is terminal) may retry the same report after an ACK loss.
// GetByTaskIDAndWorkerAndLease intentionally excludes terminal rows,
// so when it returns nil we fall back to Reader.Get(attempt_id). If the
// canonical row exists and the full tuple still matches, the retry is
// allowed to proceed — IngestTaskResultAtomic's CAS + report_hash check
// will safely no-op or conflict-detect.
//
// The function is exported so tests + non-gRPC callers can drive the
// gate without the close-write + artifact-register side-effects.
func (s *TaskReportIngestionService) ValidateIdentityTuple(ctx context.Context, cmd IngestCommand) error {
	if cmd.TaskID == "" {
		return fmt.Errorf("ingest.ValidateIdentityTuple: TaskID is required")
	}
	if cmd.AttemptID == "" {
		return fmt.Errorf("ingest.ValidateIdentityTuple: AttemptID is required")
	}
	if cmd.LeaseID == "" {
		return fmt.Errorf("ingest.ValidateIdentityTuple: LeaseID is required")
	}
	if cmd.WorkerID == "" {
		return fmt.Errorf("ingest.ValidateIdentityTuple: WorkerID is required")
	}
	if cmd.JobID == "" {
		return fmt.Errorf("ingest.ValidateIdentityTuple: JobID is required (full-tuple strict-compare, PR-2)")
	}
	if cmd.AttemptNumber <= 0 {
		return fmt.Errorf("ingest.ValidateIdentityTuple: AttemptNumber must be >0 (got %d)", cmd.AttemptNumber)
	}

	att, err := s.attemptRepo.GetByTaskIDAndWorkerAndLease(ctx, cmd.TaskID, cmd.WorkerID, cmd.LeaseID)
	if err != nil {
		return fmt.Errorf("ingest.ValidateIdentityTuple: lookup attempt (%s, %s, %s): %w",
			cmd.TaskID, cmd.WorkerID, cmd.LeaseID, err)
	}

	// If no active attempt matches, the attempt may already be terminal
	// (idempotent retry after a prior successful close). Fall back to a
	// direct attempt_id lookup and validate the full tuple there.
	if att == nil {
		att, err = s.attemptRepo.Get(ctx, cmd.AttemptID)
		if err != nil {
			return fmt.Errorf("ingest.ValidateIdentityTuple: fallback lookup attempt %s: %w",
				cmd.AttemptID, err)
		}
		if att == nil {
			return fmt.Errorf("ingest.ValidateIdentityTuple: tuple (%s, %s, %s) not found: %w",
				cmd.TaskID, cmd.WorkerID, cmd.LeaseID, taskattempts.ErrIdentityMismatch)
		}
	}

	// PR-2 strict-compare the FULL wire tuple against the canonical row.
	// Any mismatch is an impersonation / wire-drift attempt and is dropped.
	if att.ID != cmd.AttemptID {
		return fmt.Errorf("ingest.ValidateIdentityTuple: attempt_id mismatch (wire=%s db=%s task=%s): %w",
			cmd.AttemptID, att.ID, cmd.TaskID, taskattempts.ErrIdentityMismatch)
	}
	if att.TaskID != cmd.TaskID || att.WorkerID != cmd.WorkerID || att.LeaseID != cmd.LeaseID {
		return fmt.Errorf("ingest.ValidateIdentityTuple: identity tuple mismatch (wire=%s/%s/%s db=%s/%s/%s attempt=%s): %w",
			cmd.TaskID, cmd.WorkerID, cmd.LeaseID,
			att.TaskID, att.WorkerID, att.LeaseID,
			att.ID, taskattempts.ErrIdentityMismatch)
	}
	if att.AttemptNumber != int(cmd.AttemptNumber) {
		return fmt.Errorf("ingest.ValidateIdentityTuple: attempt_number mismatch (wire=%d db=%d task=%s): %w",
			cmd.AttemptNumber, att.AttemptNumber, cmd.TaskID, taskattempts.ErrIdentityMismatch)
	}
	if att.JobID != cmd.JobID {
		return fmt.Errorf("ingest.ValidateIdentityTuple: job_id mismatch (wire=%s db=%s task=%s): %w",
			cmd.JobID, att.JobID, cmd.TaskID, taskattempts.ErrIdentityMismatch)
	}
	return nil
}
