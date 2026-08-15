package grpcserver

import (
	"fmt"
	"net/url"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"velox-server/internal/artifacts"
	"velox-server/internal/completion"
	"velox-server/internal/logging"
	"velox-server/internal/telemetry"
	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-shared/identity"

	"google.golang.org/protobuf/types/known/timestamppb"
)

const masterStreamTransportID = "master-stream.v1"

func (h *Handler) handleTaskOutputDeclared(workerID string, msg *pb.TaskOutputDeclared, sess *workerSession) {
	protocolStartedAt := time.Now()
	ctx := ctxForTaskSession(sess)
	if h.completionCoord == nil || h.completionStore == nil || h.chunkedUploadSvc == nil || h.masterURL == "" {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCCompletionRejected, "[GRPC] TaskOutputDeclared from worker %s rejected: completion protocol is not wired", workerID)
		return
	}
	if msg == nil || msg.GetTaskId() == "" || msg.GetJobId() == "" || msg.GetAttemptId() == "" || msg.GetLeaseId() == "" || msg.GetAttemptNumber() <= 0 || len(msg.GetManifests()) == 0 {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCCompletionRejected, "[GRPC] TaskOutputDeclared from worker %s rejected: incomplete identity or manifests", workerID)
		return
	}
	ctx, span := telemetry.StartSpan(ctx, "task_output_declared",
		attribute.String("velox.task_id", msg.GetTaskId()),
		attribute.String("velox.worker_id", workerID),
		attribute.String("velox.attempt_id", msg.GetAttemptId()),
	)
	defer span.End()
	manifests := make([]completion.OutputManifest, 0, len(msg.GetManifests()))
	for _, m := range msg.GetManifests() {
		if m == nil {
			logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCCompletionRejected, "[GRPC] TaskOutputDeclared task=%s rejected: nil manifest", msg.GetTaskId())
			return
		}
		manifests = append(manifests, completion.OutputManifest{
			OutputKind: m.GetOutputKind(), LogicalName: m.GetLogicalName(), MimeType: m.GetMimeType(),
			SizeBytes: m.GetSizeBytes(), SHA256: m.GetSha256(), WorkerSpoolKey: m.GetWorkerSpoolKey(),
		})
	}
	fence := completion.FenceTuple{
		TaskID: msg.GetTaskId(), AttemptID: msg.GetAttemptId(), WorkerID: identity.ParseWorkerID(workerID),
		LeaseID: msg.GetLeaseId(), Revision: int(msg.GetRevision()),
	}
	plan, err := h.completionCoord.DeclareOutputs(ctx, completion.DeclareOutputsCommand{
		Fence: fence, JobID: msg.GetJobId(), OutputManifests: manifests,
	})
	if err != nil {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCCompletionRejected, "[GRPC] TaskOutputDeclared task=%s attempt=%s rejected: %v", msg.GetTaskId(), msg.GetAttemptId(), err)
		return
	}
	bindings, err := h.completionStore.ListUploadBindings(ctx, plan.CommitID)
	if err != nil {
		logGRPCf(ctx, logging.LevelError, logging.CodeGRPCCompletionFailed, "[GRPC] TaskOutputDeclared task=%s list bindings failed: %v", msg.GetTaskId(), err)
		return
	}
	byKey := make(map[string]completion.UploadBinding, len(bindings))
	for _, b := range bindings {
		byKey[b.OutputKind+"\x00"+b.LogicalName] = b
	}
	targets := make([]*pb.UploadTarget, 0, len(manifests))
	for i, m := range manifests {
		key := m.OutputKind + "\x00" + m.LogicalName
		b, exists := byKey[key]
		// DeclareOutputs creates the durable declaration before the worker
		// upload session exists.  A declaration with empty IDs is therefore
		// not an established binding and must be completed below.  Treating
		// declaration presence alone as a binding leaves the worker with an
		// empty upload target and stalls the task after rendering.
		if !exists || b.UploadID == "" || b.ArtifactID == "" {
			if i >= len(plan.Targets) || plan.Targets[i].DeclarationID == "" {
				logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCCompletionRejected, "[GRPC] TaskOutputDeclared task=%s missing declaration for output=%s", msg.GetTaskId(), m.LogicalName)
				return
			}
			session, beginErr := h.chunkedUploadSvc.InitChunkedSession(ctx, artifacts.BeginUploadCommand{
				JobID: msg.GetJobId(), WorkerID: workerID, LeaseID: msg.GetLeaseId(),
				AttemptNumber: int(msg.GetAttemptNumber()),
				Kind:          m.OutputKind, MimeType: m.MimeType, ExpectedSizeBytes: m.SizeBytes, ExpectedSHA256: m.SHA256,
			})
			if beginErr != nil {
				logGRPCf(ctx, logging.LevelError, logging.CodeGRPCCompletionFailed, "[GRPC] TaskOutputDeclared task=%s begin upload output=%s failed: %v", msg.GetTaskId(), m.LogicalName, beginErr)
				return
			}
			if bindErr := h.completionStore.BindUpload(ctx, plan.Targets[i].DeclarationID, session.UploadID, session.ArtifactID); bindErr != nil {
				logGRPCf(ctx, logging.LevelError, logging.CodeGRPCCompletionFailed, "[GRPC] TaskOutputDeclared task=%s bind upload=%s failed: %v", msg.GetTaskId(), session.UploadID, bindErr)
				return
			}
			bound, getBindingErr := h.completionStore.GetUploadBinding(ctx, session.UploadID)
			err = getBindingErr
			if err != nil {
				logGRPCf(ctx, logging.LevelError, logging.CodeGRPCCompletionFailed, "[GRPC] TaskOutputDeclared task=%s reload binding failed: %v", msg.GetTaskId(), err)
				return
			}
			b = *bound
		}
		uploadSession, getErr := h.chunkedUploadSvc.GetUpload(ctx, b.UploadID)
		if getErr != nil {
			logGRPCf(ctx, logging.LevelError, logging.CodeGRPCCompletionFailed, "[GRPC] TaskOutputDeclared task=%s upload=%s lookup failed: %v", msg.GetTaskId(), b.UploadID, getErr)
			return
		}
		targets = append(targets, &pb.UploadTarget{
			DeclarationId: b.DeclarationID, ArtifactId: b.ArtifactID, UploadId: b.UploadID,
			TransportId: masterStreamTransportID,
			UploadUrl:   h.masterURL + "/api/v1/video/master-stream/" + url.PathEscape(b.UploadID),
			ChunkSize:   8 * 1024 * 1024, ExpiresAtUnix: uploadSession.ExpiresAt.Unix(),
		})
	}
	env := &pb.MasterToWorkerEnvelope{
		MessageId: fmt.Sprintf("artifact-plan-%s-%d", msg.GetTaskId(), time.Now().UnixNano()),
		WorkerId:  workerID, SessionId: sess.sessionID, SequenceNumber: time.Now().UnixNano(),
		SentAt: timestamppb.Now(), ProtocolVersion: controltransport.ProtocolVersionCurrent,
		Msg: &pb.MasterToWorkerEnvelope_ArtifactUploadPlan{ArtifactUploadPlan: &pb.ArtifactUploadPlan{
			TaskId: msg.GetTaskId(), AttemptId: msg.GetAttemptId(), CommitId: plan.CommitID,
			CommitToken: plan.CommitToken, LeaseId: msg.GetLeaseId(), Targets: targets,
		}},
	}
	if !safeSend(sess.sendCh, &outboundMessage{Envelope: env}) {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCCompletionFailed, "[GRPC] TaskOutputDeclared task=%s plan send failed", msg.GetTaskId())
		logArtifactProtocol("ARTIFACT_UPLOAD_PLAN_SEND_FAILED", protocolStartedAt, map[string]interface{}{
			"worker_id": workerID, "job_id": msg.GetJobId(), "task_id": msg.GetTaskId(),
			"attempt_id": msg.GetAttemptId(), "lease_id": msg.GetLeaseId(), "commit_id": plan.CommitID,
			"error": "send channel unavailable",
		})
		return
	}
	logArtifactProtocol("ARTIFACT_UPLOAD_PLAN_SENT", protocolStartedAt, map[string]interface{}{
		"worker_id": workerID, "job_id": msg.GetJobId(), "task_id": msg.GetTaskId(),
		"attempt_id": msg.GetAttemptId(), "lease_id": msg.GetLeaseId(), "commit_id": plan.CommitID,
		"target_count": len(targets),
	})
}

func (h *Handler) handleArtifactUploadCompleted(workerID string, msg *pb.ArtifactUploadCompleted, sess *workerSession) {
	protocolStartedAt := time.Now()
	ctx := ctxForTaskSession(sess)
	if h.completionCoord == nil || h.completionStore == nil || h.chunkedUploadSvc == nil || msg == nil {
		return
	}
	ctx, span := telemetry.StartSpan(ctx, "artifact_upload_completed",
		attribute.String("velox.worker_id", workerID),
		attribute.String("velox.upload_id", msg.GetUploadId()),
	)
	defer span.End()
	b, err := h.completionStore.GetUploadBinding(ctx, msg.GetUploadId())
	if err != nil || b.WorkerID != workerID || b.TaskID != msg.GetTaskId() || b.AttemptID != msg.GetAttemptId() || b.LeaseID != msg.GetLeaseId() || b.CommitID != msg.GetCommitId() {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCCompletionRejected, "[GRPC] ArtifactUploadCompleted rejected worker=%s upload=%s", workerID, msg.GetUploadId())
		return
	}
	session, err := h.chunkedUploadSvc.GetUpload(ctx, msg.GetUploadId())
	if err != nil {
		logGRPCf(ctx, logging.LevelError, logging.CodeGRPCCompletionFailed, "[GRPC] ArtifactUploadCompleted upload=%s lookup failed: %v", msg.GetUploadId(), err)
		return
	}
	logArtifactProtocol("ARTIFACT_COMPLETION_RECEIVED", protocolStartedAt, map[string]interface{}{
		"worker_id": workerID, "task_id": b.TaskID, "attempt_id": b.AttemptID, "lease_id": b.LeaseID,
		"commit_id": b.CommitID, "artifact_id": b.ArtifactID, "upload_id": b.UploadID,
		"uploaded_bytes": msg.GetUploadedBytes(),
	})
	if err := h.completionCoord.CompleteUpload(ctx, completion.CompleteUploadCommand{
		Fence:    completion.FenceTuple{TaskID: b.TaskID, AttemptID: b.AttemptID, WorkerID: identity.ParseWorkerID(workerID), LeaseID: b.LeaseID, Revision: b.Revision},
		UploadID: b.UploadID, UploadedSizeBytes: msg.GetUploadedBytes(), WorkerSHA256: msg.GetWorkerSha256(), ServerSHA256: session.ReceivedSHA256,
	}); err != nil {
		logGRPCf(ctx, logging.LevelError, logging.CodeGRPCCompletionFailed, "[GRPC] ArtifactUploadCompleted upload=%s verification failed: %v", b.UploadID, err)
		return
	}
	result, err := h.completionCoord.CommitAttempt(ctx, b.CommitID)
	if err != nil {
		// Not all outputs may have arrived yet; the last completion retries the
		// same idempotent commit path and emits the ack.
		logGRPCf(ctx, logging.LevelInfo, logging.CodeGRPCCompletion, "[GRPC] ArtifactUploadCompleted upload=%s awaiting commit: %v", b.UploadID, err)
		logArtifactProtocol("TASK_COMMIT_WAITING", protocolStartedAt, map[string]interface{}{
			"worker_id": workerID, "job_id": "", "task_id": b.TaskID, "attempt_id": b.AttemptID,
			"lease_id": b.LeaseID, "commit_id": b.CommitID, "upload_id": b.UploadID, "error": err.Error(),
		})
		return
	}
	ack := &pb.MasterToWorkerEnvelope{
		MessageId: fmt.Sprintf("task-commit-ack-%s-%d", b.TaskID, time.Now().UnixNano()),
		WorkerId:  workerID, SessionId: sess.sessionID, SequenceNumber: time.Now().UnixNano(),
		SentAt: timestamppb.Now(), ProtocolVersion: controltransport.ProtocolVersionCurrent,
		Msg: &pb.MasterToWorkerEnvelope_TaskCommitAck{TaskCommitAck: &pb.TaskCommitAck{
			TaskId: b.TaskID, AttemptId: b.AttemptID, CommitId: b.CommitID, JobId: result.JobID,
			LeaseId: b.LeaseID, Revision: int32(b.Revision), CommittedAt: timestamppb.Now(),
		}},
	}
	if !safeSend(sess.sendCh, &outboundMessage{Envelope: ack}) {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCCompletionFailed, "[GRPC] ArtifactUploadCompleted task=%s ack send failed", b.TaskID)
		logArtifactProtocol("TASK_COMMIT_ACK_SEND_FAILED", protocolStartedAt, map[string]interface{}{
			"worker_id": workerID, "job_id": result.JobID, "task_id": b.TaskID, "attempt_id": b.AttemptID,
			"lease_id": b.LeaseID, "commit_id": b.CommitID, "error": "send channel unavailable",
		})
		return
	}
	logArtifactProtocol("TASK_COMMIT_ACK_SENT", protocolStartedAt, map[string]interface{}{
		"worker_id": workerID, "job_id": result.JobID, "task_id": b.TaskID, "attempt_id": b.AttemptID,
		"lease_id": b.LeaseID, "commit_id": b.CommitID,
	})
}
