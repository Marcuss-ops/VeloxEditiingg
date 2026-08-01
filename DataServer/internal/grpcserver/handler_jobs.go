package grpcserver

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"velox-server/internal/ingest"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
	"velox-server/internal/telemetry"
	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"

	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// handleTaskResult processes typed TaskResult — feat/task-report-ingestion.
//
// PR-29 (`feat/task-report-ingestion`): the handler is now a thin relay
// to TaskReportIngestionService.Ingest, which centralizes the audit-cloused
// sequence:
//
//  1. atomic Task + Attempt close (TransitionTaskToTerminalAtomic)
//  2. worker-declared output_artifacts registration (with idempotent
//     skip on duplicate (task_id, artifact_id))
//  3. Job roll-up to AWAITING_ARTIFACT (all sibling tasks SUCCEEDED) or
//     FAILED (any sibling task FAILED) when the roll-up condition holds
//
// The handler pre-validates the identity tuple (presence of task_id,
// attempt_id, lease_id) before delegating. A nil ingestionSvc is treated
// as a misconfiguration and surfaces as a structured error log rather
// than a silent no-op — better to fail loud than to leak TaskResults
// without ever closing the Attempt.
func (h *Handler) handleTaskResult(workerID string, tr *pb.TaskResult, sess *workerSession) {
	taskID := tr.GetTaskId()
	jobID := tr.GetJobId()
	attemptID := tr.GetAttemptId()
	leaseID := tr.GetLeaseId()
	attemptNumber := tr.GetAttemptNumber()
	revision := tr.GetRevision()

	if taskID == "" || jobID == "" || attemptID == "" || leaseID == "" || attemptNumber <= 0 {
		log.Printf("[GRPC] TaskResult from worker %s refused — incomplete identity (task=%q job=%q attempt=%q lease=%q attempt_num=%d rev=%d)",
			workerID, taskID, jobID, attemptID, leaseID, attemptNumber, revision)
		return
	}

	log.Printf("[GRPC] Worker %s reported task %s (attempt %s): status=%s code=%q detail=%q, %d output artifacts",
		workerID, taskID, attemptID, tr.GetStatus(), tr.GetErrorCode(), tr.GetErrorDetail(), len(tr.GetOutputArtifacts()))

	if h.ingestionSvc == nil {
		log.Printf("[GRPC] TaskResult from worker %s REJECTED — ingestionSvc not wired (boot misconfig)", workerID)
		return
	}

	// Resolve executor fields from the master-owned task row when it is
	// available. The ingestion service remains the authoritative identity
	// gate and repeats the task/attempt lookup before persistence; keeping
	// this lookup best-effort preserves compatibility with lightweight
	// handler callers while ensuring worker echoes are never trusted.
	var canonicalExecutorID string
	var canonicalExecutorVersion int
	if h.taskRepo != nil {
		if canonicalTask, taskErr := h.taskRepo.Get(context.Background(), taskID); taskErr == nil && canonicalTask != nil && canonicalTask.ID == taskID {
			canonicalExecutorID = canonicalTask.ExecutorID
			canonicalExecutorVersion = canonicalTask.ExecutorVersion
		}
	}

	// Translate protobuf output_artifacts (Struct items) into the typed
	// DeclaredArtifact slice. Metadata is best-effort JSON.
	declared := make([]ingest.DeclaredArtifact, 0, len(tr.GetOutputArtifacts()))
	for _, item := range tr.GetOutputArtifacts() {
		m := item.AsMap()
		artID, _ := m["artifact_id"].(string)
		if artID == "" {
			continue
		}
		artType, _ := m["artifact_type"].(string)
		path, _ := m["artifact_path"].(string)
		var size int64
		if v, ok := m["size_bytes"].(float64); ok {
			size = int64(v)
		} else if v, ok := m["artifact_size"].(float64); ok {
			size = int64(v)
		}
		sha, _ := m["sha256"].(string)
		d := ingest.DeclaredArtifact{
			ArtifactID:   artID,
			ArtifactType: artType,
			Path:         path,
			Size:         size,
			SHA256:       sha,
			Metadata:     m,
		}
		declared = append(declared, d)
	}

	// Scorecard v1 / F1 — typed execution-metrics hoisting. Build the
	// 3 typed Go structs from the wire payload (see handler_jobs_metrics.go
	// for derivation rules + logs). They flow through IngestCommand to
	// IngestTaskResult, which persists them under the per-task mutex
	// immediately after the atomic close-write.
	typedMetrics := executionMetricsToAttemptMetrics(attemptID, tr.GetExecutionMetrics())
	typedCache := deriveCacheStats(attemptID, typedMetrics)
	typedCost := executionMetricsToCostBasis(attemptID, tr.GetExecutionMetrics())
	segmentTimings := segmentTimingsFromProto(attemptID, taskID, jobID, workerID, tr.GetSegmentTimings())
	phaseTimings := phaseTimingsFromProto(attemptID, taskID, jobID, workerID, canonicalExecutorID, canonicalExecutorVersion, tr.GetPhaseTimings())
	partialPhaseTimings := partialPhaseTimingsFromProto(attemptID, taskID, jobID, workerID, canonicalExecutorID, canonicalExecutorVersion, tr.GetPartialPhaseMetrics())

	// Scorecard v2 / Step 15: start an "ingest_result" span.
	// Scorecard v2 / Step 15c: use session.ctx (derived from stream.Context())
	// so the span inherits the parent trace context from the worker.
	gRPCctx := context.Background()
	if sess != nil && sess.ctx != nil {
		gRPCctx = sess.ctx
	}
	ctx, span := telemetry.StartSpan(gRPCctx, "ingest_result",
		attribute.String("velox.task_id", taskID),
		attribute.String("velox.worker_id", workerID),
		attribute.String("velox.attempt_id", attemptID),
	)
	defer span.End()

	// Populate trace context from the current span so downstream
	// persistence paths (IngestTaskResultAtomic) can stamp it on
	// the attempt row.
	traceID := telemetry.TraceIDFromContext(ctx)
	spanID := telemetry.SpanIDFromContext(ctx)

	// PR-2 / attempt_number wire-strict-compare — now sourced directly
	// from the proto (no longer resolved via a canonical lookup because
	// the worker sends the canonical attempt_number on the wire). The
	// revision is also consumed from the proto for CAS validation.
	//
	// A zero attempt_number (legacy worker) is rejected at the
	// field-presence check above; a non-zero value that mismatches the
	// canonical row triggers ErrIdentityMismatch in the ingestion
	// service's ValidateIdentityTuple.

	// Version correlation (Step 4 / Velox Metrics Center): read the
	// worker's software versions from the session (stored on last
	// heartbeat) and pass them through so IngestTaskResultAtomic can
	// stamp them on the task_attempts row.
	var gitSHA, workerVer, engineVer, ffmpegVer string
	if sess != nil {
		if v, ok := sess.gitSHA.Load().(string); ok {
			gitSHA = v
		}
		if v, ok := sess.workerVersion.Load().(string); ok {
			workerVer = v
		}
		if v, ok := sess.engineVersion.Load().(string); ok {
			engineVer = v
		}
		if v, ok := sess.ffmpegVersion.Load().(string); ok {
			ffmpegVer = v
		}
	}

	// Serialize the complete TaskResult protobuf to JSON so the master can
	// store the exact worker report for audit, replay, and forward-compatible
	// metric extraction. If the proto cannot be serialized, reject the
	// report: the raw payload is a required part of the audit trail.
	rawJSON, mErr := protojson.Marshal(tr)
	if mErr != nil {
		log.Printf("[GRPC] Failed to marshal TaskResult to JSON for task=%s attempt=%s: %v", taskID, attemptID, mErr)
		return
	}
	rawReportJSON := string(rawJSON)
	receivedAt := time.Now().UTC()

	res, err := h.ingestionSvc.IngestTaskResult(ctx, ingest.IngestCommand{
		TaskID:              taskID,
		AttemptID:           attemptID,
		AttemptNumber:       attemptNumber,
		LeaseID:             leaseID,
		WorkerID:            workerID,
		JobID:               jobID,
		ExecutorID:          canonicalExecutorID,
		ExecutorVersion:     canonicalExecutorVersion,
		Status:              tr.GetStatus(),
		ErrorCode:           tr.GetErrorCode(),
		ErrorDetail:         tr.GetErrorDetail(),
		OutputArtifacts:     declared,
		TypedMetrics:        typedMetrics,
		CacheStats:          typedCache,
		CostBasis:           typedCost,
		SegmentTimings:      segmentTimings,
		PartialPhaseMetrics: partialPhaseTimings,
		PhaseTimings:        phaseTimings,
		GitSHA:              gitSHA,
		WorkerVersion:       workerVer,
		EngineVersion:       engineVer,
		FFmpegVersion:       ffmpegVer,
		TraceID:             traceID,
		SpanID:              spanID,
		RawReportJSON:       rawReportJSON,
		RawReportReceivedAt: receivedAt,
		ReportSchemaVersion: tr.GetReportSchemaVersion(),
		ReportVersion:       tr.GetReportVersion(),
		ReportHash:          tr.GetReportHash(),
	})

	// Build and send the TaskResultAck. We ACK both successful ingests
	// and immutable-report conflicts so the worker can stop retrying.
	// Internal errors (DB failures, identity mismatch, etc.) do NOT get
	// an ACK, letting the worker retry.
	ackError := ""
	if err != nil {
		if errors.Is(err, taskattempts.ErrReportConflict) {
			ackError = "report_conflict"
			log.Printf("[GRPC] TaskResult conflict for task=%s attempt=%s: %v", taskID, attemptID, err)
		} else {
			log.Printf("[GRPC] TaskResult ingest for task=%s attempt=%s FAILED: %v", taskID, attemptID, err)
			return
		}
	} else {
		log.Printf("[GRPC] TaskResult ingest for task=%s done: closed=%v artNew=%d artSkip=%d jobXn=%v jobStatus=%q",
			taskID, res.AttemptClosed, res.ArtifactsNew, res.ArtifactsSkips, res.JobTransitioned, res.JobNewStatus)
		// worker_task_runtime is a volatile projection. The canonical
		// TaskResult ingest has now closed this attempt, so remove the row
		// immediately instead of waiting for a subsequent heartbeat. This
		// also prevents a final heartbeat race from leaving a stale runtime.
		if h.dbStore != nil {
			if err := h.dbStore.DeleteWorkerTaskRuntime(taskID, attemptID); err != nil {
				log.Printf("[GRPC] failed to remove worker runtime task=%s attempt=%s: %v", taskID, attemptID, err)
			}
		}
	}

	if sess != nil && sess.sendCh != nil {
		ackEnv := &pb.MasterToWorkerEnvelope{
			MessageId:       fmt.Sprintf("taskresultack-%s-%s-%d", workerID, taskID, time.Now().UnixNano()),
			WorkerId:        workerID,
			SentAt:          timestamppb.Now(),
			ProtocolVersion: controltransport.ProtocolVersionCurrent,
			Msg: &pb.MasterToWorkerEnvelope_TaskResultAck{
				TaskResultAck: &pb.TaskResultAck{
					TaskId:    taskID,
					JobId:     jobID,
					AttemptId: attemptID,
					Error:     ackError,
				},
			},
		}
		if !safeSend(sess.sendCh, &outboundMessage{Envelope: ackEnv}) {
			log.Printf("[GRPC] sendCh full/closed for TaskResultAck to worker %s", workerID)
		}
	}
}

// handleTaskRenewal processes a typed TaskLeaseRenewal via gRPC stream.
// fix/identity-tuple-mandatory: the worker sends the full 6-field
// identity tuple on every renewal. We validate all fields are present
// then issue the CAS-backed RenewLease against the live DB revision.
func (h *Handler) handleTaskRenewal(workerID string, tr *pb.TaskLeaseRenewal, sess *workerSession) {
	if tr == nil || h.taskRepo == nil || sess == nil || sess.workerID != workerID {
		return
	}
	ctx := context.Background()
	taskID := tr.GetTaskId()
	jobID := tr.GetJobId()
	attemptID := tr.GetAttemptId()
	leaseID := tr.GetLeaseId()
	attemptNumber := tr.GetAttemptNumber()
	renewalRevision := tr.GetRevision()

	t, err := h.taskRepo.Get(ctx, taskID)
	if err != nil || t == nil {
		log.Printf("[GRPC] TaskLeaseRenewal task %s not found: %v", taskID, err)
		return
	}
	if t.Status != taskgraph.StatusLeased && t.Status != taskgraph.StatusRunning {
		log.Printf("[GRPC] TaskLeaseRenewal from worker %s refused — task %s is not leasable (status=%s)", workerID, taskID, t.Status)
		return
	}
	wireIdentity := taskIdentityFromWire(taskID, jobID, attemptID, leaseID, int(attemptNumber), int(renewalRevision), workerID)
	if err := validateTaskIdentity(wireIdentity, taskIdentityFromTask(t)); err != nil {
		log.Printf("[GRPC] TaskLeaseRenewal from worker %s refused — identity validation failed for task %s: %v", workerID, taskID, err)
		return
	}

	// Fence renewal to the authenticated stream session and hold the
	// registry read lock through the repository CAS. This prevents an old
	// stream from renewing a task after a same-worker reconnect.
	h.mu.RLock()
	defer h.mu.RUnlock()
	if !h.isCurrentSessionLocked(workerID, sess) {
		log.Printf("[GRPC] TaskLeaseRenewal from worker %s refused — session was replaced before CAS for task %s", workerID, taskID)
		return
	}

	expiry := time.Now().UTC().Add(30 * time.Minute)
	if tr.GetRequestedExpiry() != nil {
		expiry = tr.GetRequestedExpiry().AsTime()
	}

	if err := h.taskRepo.RenewLease(ctx, taskID, workerID, leaseID, expiry, int(renewalRevision)); err != nil {
		log.Printf("[GRPC] TaskLeaseRenewal failed for %s (worker %s lease %s): %v",
			taskID, workerID, leaseID, err)
		return
	}
	log.Printf("[GRPC] TaskLeaseRenewal extended task %s for worker %s lease=%s", taskID, workerID, leaseID)
}
