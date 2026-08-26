package worker

import (
	"context"
	"fmt"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	sharedtelemetry "velox-shared/telemetry"
	"velox-worker-agent/internal/spool"
	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/internal/telemetry"
)

// active_task_publish.go is the orchestration root for Artifact Commit
// Protocol v1. Stage implementations live in active_task_validation.go,
// active_task_declare.go, active_task_upload.go, active_task_ack.go and
// active_task_checkpoint.go.

// publishArtifactsV1 preserves the protocol order:
// validate/register -> declare -> upload -> completion -> commit ack.
func (w *Worker) publishArtifactsV1(ctx context.Context, pte *PendingTaskExecution, report *taskrunner.TaskExecutionReport) error {
	publicationStartedAt := time.Now()
	if report == nil || w.transport == nil || w.publisherRegistry == nil {
		return nil
	}
	if w.publisherPool != nil {
		if err := w.publisherPool.Acquire(ctx); err != nil {
			return err
		}
		defer w.publisherPool.Release()
	}
	// The pool limits publisher concurrency, but it does not serialize the
	// foreground publisher with the durable resume loop. Both paths can touch
	// the same spool row, so the lifecycle mutex is mandatory in either mode.
	w.artifactUploadMu.Lock()
	defer w.artifactUploadMu.Unlock()

	if err := validateArtifactOutputs(report); err != nil {
		return err
	}
	spoolEntries, err := w.registerOutputSpool(ctx, pte, report)
	if err != nil {
		return fmt.Errorf("worker artifact upload: register durable spool: %w", err)
	}
	// The local spool now holds bytes + SHA + size durably: mark output.durable
	// here so the waterfall publish_queue_wait bucket (output.durable ->
	// publish.started) reflects real time spent queued before the upload.
	if m := telemetry.MilestoneRecorderFromContext(ctx); m != nil {
		m.Mark(sharedtelemetry.MilestoneOutputDurable)
	}
	committed := false
	resumable := make(map[string]bool)
	defer func() {
		if !committed {
			w.rejectOutputSpool(spoolEntries, resumable, "publication_failed", "typed artifact publication did not reach commit acknowledgement")
		}
	}()

	manifests := buildOutputManifests(pte, report)
	ackCh := w.registerPendingArtifactAck(pte.TaskID)
	defer w.unregisterPendingArtifactAck(pte.TaskID)

	deadline := artifactProtocolDeadline(ctx)
	plan, err := w.declareArtifactOutputs(ctx, pte, manifests, ackCh, spoolEntries, resumable, publicationStartedAt, deadline)
	if err != nil {
		return err
	}
	// The artifacts are declared/queued for publication. Mark publish.queued
	// now; the actual transfer boundary is publish.started just before the
	// first upload call.
	if m := telemetry.MilestoneRecorderFromContext(ctx); m != nil {
		m.Mark(sharedtelemetry.MilestonePublishQueued)
		m.Mark(sharedtelemetry.MilestonePublishStarted)
	}

	completed, err := w.uploadDeclaredArtifacts(ctx, pte, report, plan, spoolEntries, resumable, publicationStartedAt)
	if err != nil {
		return err
	}
	if err := w.sendArtifactCompletions(ctx, pte, plan, completed, publicationStartedAt); err != nil {
		return err
	}
	if err := w.awaitArtifactCommit(ctx, pte, plan, ackCh, publicationStartedAt, deadline); err != nil {
		return err
	}
	if err := w.commitArtifactSpool(ctx, pte, spoolEntries); err != nil {
		return err
	}
	// commitArtifactSpool is the terminal publish boundary (all outputs
	// uploaded and the commit acknowledged). Mark publish.completed here so
	// the publish bucket spans the actual transfer, not the whole lifecycle.
	if m := telemetry.MilestoneRecorderFromContext(ctx); m != nil {
		m.Mark(sharedtelemetry.MilestonePublishCompleted)
	}
	committed = true
	w.logger.Info("[TASK] %d output artifacts committed for task %s (job=%s attempt=%s)", len(completed), pte.TaskID, pte.JobID, pte.AttemptID)
	return nil
}

// artifactProtocolDeadline bounds both plan and commit acknowledgement waits.
func artifactProtocolDeadline(ctx context.Context) time.Time {
	deadline := time.Now().Add(5 * time.Minute)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	return deadline
}

// typedArtifactMessage constructs protocol messages with the worker's stable
// identity and negotiated protocol version.
func (w *Worker) typedArtifactMessage(kind controltransport.ControlMessageType, payload interface{}) controltransport.ControlMessage {
	return controltransport.NewTypedMessage(kind, w.config.WorkerID, w.config.ProtocolVersion, payload)
}

// keep the protocol package symbols visible in this orchestration file for
// source-level contract checks and documentation tooling.
var _ = pb.TaskOutputDeclared{}
var _ = spool.StatusOutputReady
