package worker

import (
	"context"
	"fmt"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/spool"
)

func (w *Worker) awaitArtifactCommit(ctx context.Context, pte *PendingTaskExecution, plan *pb.ArtifactUploadPlan, ackCh <-chan controltransport.ControlMessage, started time.Time, deadline time.Time) error {
	msg, err := w.waitForRegisteredArtifactAck(ctx, ackCh, deadline)
	if err != nil {
		w.logArtifactProtocol("TASK_COMMIT_ACK_WAIT_FAILED", pte, started, plan.GetCommitId(), "", "", map[string]interface{}{"error": err.Error()})
		return fmt.Errorf("worker artifact upload: wait for commit ack: %w", err)
	}
	commitAck, ok := msg.TypedPayload.(*pb.TaskCommitAck)
	if msg.Type != controltransport.MsgTaskCommitAck || !ok || commitAck == nil {
		return fmt.Errorf("worker artifact upload: expected task commit ack, got %s/%T", msg.Type, msg.TypedPayload)
	}
	if commitAck.GetTaskId() != pte.TaskID || commitAck.GetAttemptId() != pte.AttemptID || commitAck.GetJobId() != pte.JobID || commitAck.GetLeaseId() != pte.LeaseID || commitAck.GetRevision() != int32(pte.Revision) || commitAck.GetCommitId() != plan.GetCommitId() {
		w.logArtifactProtocol("TASK_COMMIT_ACK_FENCE_MISMATCH", pte, started, commitAck.GetCommitId(), "", "", map[string]interface{}{"ack_job_id": commitAck.GetJobId(), "ack_task_id": commitAck.GetTaskId(), "ack_attempt_id": commitAck.GetAttemptId(), "ack_lease_id": commitAck.GetLeaseId(), "ack_revision": commitAck.GetRevision(), "error": "commit ack fence mismatch"})
		return fmt.Errorf("worker artifact upload: commit ack fence mismatch")
	}
	w.logArtifactProtocol("TASK_COMMIT_ACK_RECEIVED", pte, started, commitAck.GetCommitId(), "", "", nil)
	return nil
}

func (w *Worker) commitArtifactSpool(ctx context.Context, pte *PendingTaskExecution, entries []spool.SpoolEntry) error {
	for i := range entries {
		if err := w.outputSpool.MarkCommitted(ctx, entries[i].SpoolID); err != nil {
			return fmt.Errorf("worker artifact upload: mark target %d committed: %w", i, err)
		}
		w.releaseCommittedArtifact(entries[i])
	}
	return nil
}

type artifactCommitFence struct {
	TaskID    string
	AttemptID string
	JobID     string
	LeaseID   string
	Revision  int32
	CommitID  string
	At        time.Time
}

var _ = artifactCommitFence{}
var _ = spool.StatusCommitted
