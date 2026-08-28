package worker

// active_task_upload_completion.go sends artifact upload completion messages
// to the master after all uploads finish.

import (
	"context"
	"fmt"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
)

// sendArtifactCompletions transmits each ArtifactUploadCompleted message
// to the master via the transport stream.
func (w *Worker) sendArtifactCompletions(ctx context.Context, pte *PendingTaskExecution, plan *pb.ArtifactUploadPlan, completed []*pb.ArtifactUploadCompleted, started time.Time) error {
	for i, completion := range completed {
		if err := w.transportSend(ctx, w.typedArtifactMessage(controltransport.MsgArtifactUploadCompleted, completion)); err != nil {
			w.logArtifactProtocol("ARTIFACT_COMPLETION_SEND_FAILED", pte, started, completion.GetCommitId(), "", completion.GetUploadId(), map[string]interface{}{"error": err.Error()})
			return fmt.Errorf("worker artifact upload: report completion: %w", err)
		}
		artifactID := ""
		if i < len(plan.GetTargets()) && plan.GetTargets()[i] != nil {
			artifactID = plan.GetTargets()[i].GetArtifactId()
		}
		w.logArtifactProtocol("ARTIFACT_COMPLETION_SENT", pte, started, completion.GetCommitId(), artifactID, completion.GetUploadId(), map[string]interface{}{"uploaded_bytes": completion.GetUploadedBytes()})
	}
	return nil
}
