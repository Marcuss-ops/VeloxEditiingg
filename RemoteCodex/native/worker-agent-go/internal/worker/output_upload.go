package worker

import (
	"context"
	"fmt"
	"os"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/pkg/api"
)

func (w *Worker) uploadTaskOutputs(ctx context.Context, pte *PendingTaskExecution, report *taskrunner.TaskExecutionReport) error {
	if report == nil || len(report.Outputs) == 0 {
		return nil
	}
	if w.apiClient == nil {
		return fmt.Errorf("worker output upload: api client is not configured")
	}

	ref, ok := selectUploadableOutput(report.Outputs)
	if !ok {
		return fmt.Errorf("worker output upload: no uploadable output with a local file path")
	}
	if _, err := os.Stat(ref.URI); err != nil {
		return fmt.Errorf("worker output upload: output file %q is not readable: %w", ref.URI, err)
	}

	resp, err := w.apiClient.UploadCompletedVideo(ctx, api.UploadCompletedVideoRequest{
		JobID:         pte.JobID,
		WorkerID:      w.config.WorkerID,
		LeaseID:       pte.LeaseID,
		AttemptNumber: pte.AttemptNumber,
		Revision:      pte.JobRevision,
		FilePath:      ref.URI,
	})
	if err != nil {
		return err
	}
	if !resp.OK {
		return fmt.Errorf("worker output upload: master rejected upload for job %s: %s", pte.JobID, resp.Error)
	}

	w.logger.Info("[TASK] Output uploaded for task %s (job=%s artifact=%s upload=%s)",
		pte.TaskID, pte.JobID, resp.ArtifactID, resp.UploadID)
	return nil
}

func selectUploadableOutput(outputs []executor.ArtifactRef) (executor.ArtifactRef, bool) {
	for _, ref := range outputs {
		if ref.Type == "render.output" && ref.URI != "" {
			return ref, true
		}
	}
	for _, ref := range outputs {
		if ref.URI != "" {
			return ref, true
		}
	}
	return executor.ArtifactRef{}, false
}
