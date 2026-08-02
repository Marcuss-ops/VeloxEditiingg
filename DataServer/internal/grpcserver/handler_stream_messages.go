// Package grpcserver / handler_stream_messages.go
//
// Per-message dispatch for the WorkerControl bidi stream. Extracted from
// handler_stream.go so the Stream() lifecycle (handshake, admission,
// teardown) stays separate from message routing.
package grpcserver

import (
	"errors"
	"log"

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
		if notifyCh != nil {
			select {
			case notifyCh <- struct{}{}:
			default:
			}
		}

	case *pb.WorkerToMasterEnvelope_TaskLeaseRenewal:
		h.handleTaskRenewal(workerID, m.TaskLeaseRenewal, sess)

	case *pb.WorkerToMasterEnvelope_TaskAccepted:
		h.handleTaskAccepted(workerID, m.TaskAccepted, sess)

	case *pb.WorkerToMasterEnvelope_TaskRejected:
		h.handleTaskRejected(workerID, m.TaskRejected, sess)

	case *pb.WorkerToMasterEnvelope_TaskResult:
		h.handleTaskResult(workerID, m.TaskResult, sess)

	case *pb.WorkerToMasterEnvelope_TaskOutputDeclared:
		h.handleTaskOutputDeclared(workerID, m.TaskOutputDeclared, sess)

	case *pb.WorkerToMasterEnvelope_ArtifactUploadCompleted:
		h.handleArtifactUploadCompleted(workerID, m.ArtifactUploadCompleted, sess)

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

	default:
		log.Printf("[GRPC] Unknown message type from worker %s: %T", workerID, env.Msg)
	}
	return nil
}
