package worker

import (
	"context"
	"fmt"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	sharedtelemetry "velox-shared/telemetry"
	"velox-worker-agent/internal/publisher"
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
	m := telemetry.MilestoneRecorderFromContext(ctx)
	if m != nil {
		m.Mark(sharedtelemetry.MilestonePublishSlotWaitStarted)
	}
	// The pool bounds total concurrent publications. Artifact-level locking is
	// acquired after spool identities are known, so unrelated artifacts do not
	// inherit task-wide serialization.
	if w.publisherPool != nil {
		poolWaitStarted := time.Now()
		if err := w.publisherPool.Acquire(ctx); err != nil {
			return err
		}
		telemetry.GetPrometheusMetrics().RecordArtifactLockWait(time.Since(poolWaitStarted))
		defer w.publisherPool.Release()
	}
	if m != nil {
		m.Mark(sharedtelemetry.MilestonePublishSlotWaitCompleted)
	}

	if err := validateArtifactOutputs(report); err != nil {
		return err
	}
	spoolEntries, err := w.registerOutputSpoolRendering(ctx, pte, report)
	if err != nil {
		return fmt.Errorf("worker artifact upload: register durable spool: %w", err)
	}
	keys := make([]string, 0, len(spoolEntries))
	for _, entry := range spoolEntries {
		keys = append(keys, entry.SpoolID)
	}
	if w.artifactLocks == nil {
		return fmt.Errorf("worker artifact upload: artifact lock registry is not configured")
	}
	lockWaitStarted := time.Now()
	releaseArtifacts, err := w.artifactLocks.AcquireMany(ctx, keys)
	telemetry.GetPrometheusMetrics().RecordArtifactLockWait(time.Since(lockWaitStarted))
	if err != nil {
		return fmt.Errorf("worker artifact upload: acquire artifact locks: %w", err)
	}
	defer releaseArtifacts()
	if err := w.markOutputSpoolReady(ctx, spoolEntries, report); err != nil {
		return fmt.Errorf("worker artifact upload: mark spool ready: %w", err)
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
	// The native mux may return an opportunistic digest, but it is trusted
	// only when the sink proved append-only output. Otherwise the canonical
	// manifest path computes the final bytes itself.
	for i, entry := range spoolEntries {
		if i >= len(report.Outputs) || entry.LocalPath == "" {
			continue
		}
		if report.Outputs[i].Hash == "" {
			manifest, manifestErr := publisher.ComputeLocalManifest(ctx, entry.LocalPath)
			if manifestErr != nil {
				return fmt.Errorf("worker artifact upload: fallback manifest: %w", manifestErr)
			}
			report.Outputs[i].Hash = manifest.SHA256Hex
			report.Outputs[i].SizeBytes = manifest.SizeBytes
		}
	}
	if m != nil {
		m.Mark(sharedtelemetry.MilestonePublishDeclareStarted)
	}
	ackCh := w.registerPendingArtifactAck(pte.TaskID)
	defer w.unregisterPendingArtifactAck(pte.TaskID)

	deadline := artifactProtocolDeadline(ctx)
	plan, err := w.declareArtifactOutputs(ctx, pte, manifests, ackCh, spoolEntries, resumable, publicationStartedAt, deadline)
	if m != nil {
		m.Mark(sharedtelemetry.MilestonePublishDeclareCompleted)
	}
	if err != nil {
		return err
	}
	// The artifacts are declared/queued for publication. Mark publish.queued
	// now.
	if m := telemetry.MilestoneRecorderFromContext(ctx); m != nil {
		m.Mark(sharedtelemetry.MilestonePublishQueued)
	}

	// publish.started deliberately does NOT fire beside publish.queued: the
	// two stamps bracket the declaration→transfer handoff, which is exactly
	// the waterfall publish_queue_wait window. Firing them together would
	// collapse that bucket to zero forever and hide publisher-pool contention
	// once render slots and the publisher pool are separated.
	if m := telemetry.MilestoneRecorderFromContext(ctx); m != nil {
		m.Mark(sharedtelemetry.MilestonePublishStarted)
	}

	if m != nil {
		m.Mark(sharedtelemetry.MilestonePublishUploadStarted)
	}
	completed, err := w.uploadDeclaredArtifacts(ctx, pte, report, plan, spoolEntries, resumable, publicationStartedAt)
	if m != nil {
		m.Mark(sharedtelemetry.MilestonePublishUploadCompleted)
	}
	if err != nil {
		return err
	}
	if err := w.sendArtifactCompletions(ctx, pte, plan, completed, publicationStartedAt); err != nil {
		return err
	}
	if m != nil {
		m.Mark(sharedtelemetry.MilestonePublishRemoteFinalizeStarted)
		m.Mark(sharedtelemetry.MilestonePublishCommitWaitStarted)
	}
	if err := w.awaitArtifactCommit(ctx, pte, plan, ackCh, publicationStartedAt, deadline); err != nil {
		return err
	}
	if m != nil {
		m.Mark(sharedtelemetry.MilestonePublishRemoteFinalizeCompleted)
		m.Mark(sharedtelemetry.MilestonePublishCommitWaitCompleted)
	}
	if m != nil {
		m.Mark(sharedtelemetry.MilestonePublishSpoolCommitStarted)
	}
	if err := w.commitArtifactSpool(ctx, pte, spoolEntries); err != nil {
		return err
	}
	if m != nil {
		m.Mark(sharedtelemetry.MilestonePublishSpoolCommitCompleted)
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
