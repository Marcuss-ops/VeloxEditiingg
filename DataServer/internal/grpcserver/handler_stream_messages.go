// Package grpcserver / handler_stream_messages.go
//
// Per-message dispatch for the WorkerControl bidi stream. Extracted from
// handler_stream.go so the Stream() lifecycle (handshake, admission,
// teardown) stays separate from message routing.
package grpcserver

import (
	"errors"

	"velox-server/internal/logging"
	pb "velox-shared/controltransport/pb"
)

// errStreamGoodbye is the sentinel returned by dispatchMessage when the
// worker sends Goodbye. Stream() maps it to a clean nil return instead of
// surfacing an error to the client.
var errStreamGoodbye = errors.New("stream: worker goodbye")

// dispatchMessage routes a single WorkerToMasterEnvelope to the matching
// handler. It returns errStreamGoodbye when the worker sent Goodbye (the
// caller should end the stream cleanly), a gate error when an artifact
// commit must be refused, or nil when the message was handled normally.
func (h *Handler) dispatchMessage(workerID, sessionID string, env *pb.WorkerToMasterEnvelope, sess *workerSession, notifyCh chan struct{}) error {
	switch m := env.Msg.(type) {
	case *pb.WorkerToMasterEnvelope_Heartbeat:
		h.handleHeartbeat(workerID, sessionID, m.Heartbeat)
		h.dispatchCommands(workerID, sess)
		signalTaskOffers(notifyCh)

	case *pb.WorkerToMasterEnvelope_TaskLeaseRenewal:
		h.handleTaskRenewal(workerID, m.TaskLeaseRenewal, sess)

	case *pb.WorkerToMasterEnvelope_TaskAccepted:
		h.handleTaskAccepted(workerID, m.TaskAccepted, sess)
		// Accept completes the offer handshake; re-evaluate placement in case
		// another READY task can now use a different phase slot.
		sess.signalPlacement()

	case *pb.WorkerToMasterEnvelope_TaskRejected:
		h.handleTaskRejected(workerID, m.TaskRejected, sess)
		// Rejection releases the lease and returns the task to READY. A
		// capacity_full response is deliberately not an immediate wake-up:
		// the worker has just stated that its phase pool is full, so waking
		// placement synchronously would create a reject/reoffer storm. The
		// heartbeat and the 10s placement safety ticker retry after capacity
		// can have changed; all other rejection reasons remain event-driven.
		if m.TaskRejected.GetReason() != "capacity_full" {
			sess.signalPlacement()
		}

	case *pb.WorkerToMasterEnvelope_TaskResult:
		h.handleTaskResult(workerID, m.TaskResult, sess, env.GetSentAt().AsTime())
		// A terminal result releases the authoritative lease. Do not wait for
		// the next heartbeat/ticker before offering the next READY task.
		sess.signalPlacement()

	case *pb.WorkerToMasterEnvelope_TaskOutputDeclared:
		h.handleTaskOutputDeclared(workerID, m.TaskOutputDeclared, sess)

	case *pb.WorkerToMasterEnvelope_ArtifactUploadIntent:
		if gateErr := h.checkArtifactProgressiveCapability(workerID); gateErr != nil {
			return gateErr
		}
		h.handleArtifactUploadIntent(workerID, m.ArtifactUploadIntent, sess)

	case *pb.WorkerToMasterEnvelope_ArtifactUploadCompleted:
		if gateErr := h.checkArtifactProgressiveCapability(workerID); gateErr != nil {
			return gateErr
		}
		h.handleArtifactUploadCompleted(workerID, m.ArtifactUploadCompleted, sess)

	case *pb.WorkerToMasterEnvelope_AssetDownloadProgress:
		h.handleAssetDownloadProgress(workerID, m.AssetDownloadProgress)

	case *pb.WorkerToMasterEnvelope_CommandAck:
		h.handleCommandAck(workerID, m.CommandAck)

	case *pb.WorkerToMasterEnvelope_ArtifactUploaded:
		// Blocco 1 final-wire (P0 #2, #3, #4): invoke the
		// capability gate before delegating. ArtifactUploaded
		// is the on-the-wire "artifact.commit.v1" message and
		// the canonical write path through artifacts.Service.
		// A misconfigured/spool-broken/transport-empty master
		// MUST NOT accept a commit because that would yield a
		// SUCCEEDED job with no on-disk blob. See
		// handler_artifacts.go::checkArtifactCommitGate for
		// the fail-closed semantic (PermissionDenied).
		if gateErr := h.checkArtifactCommitGate(workerID); gateErr != nil {
			return gateErr
		}
		h.handleArtifactUploaded(workerID, m.ArtifactUploaded)

	case *pb.WorkerToMasterEnvelope_Goodbye:
		return errStreamGoodbye

	case *pb.WorkerToMasterEnvelope_PrefetchLifecycleEvent:
		h.handlePrefetchLifecycleEvent(workerID, m.PrefetchLifecycleEvent)
		// Preparation evidence changes placement eligibility. Wake the
		// push placer immediately instead of waiting for the next heartbeat
		// or its 10-second ticker; otherwise a prepared READY task can sit
		// indefinitely behind an execution slot that has just been released.
		signalTaskOffers(notifyCh)

	default:
		logGRPCf(ctxForTaskSession(sess), logging.LevelWarn, logging.CodeGRPCStreamUnknownMessage, "[GRPC] Unknown message type from worker %s: %T", workerID, env.Msg)
	}
	return nil
}

// signalTaskOffers coalesces placement wake-ups. The channel is deliberately
// non-blocking: a pending wake-up already guarantees another placement pass.
func signalTaskOffers(notifyCh chan struct{}) {
	if notifyCh == nil {
		return
	}
	select {
	case notifyCh <- struct{}{}:
	default:
	}
}
