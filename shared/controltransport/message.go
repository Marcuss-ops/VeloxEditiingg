package controltransport

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// ControlMessageType identifies the kind of control message.
type ControlMessageType string

// --- Worker → Master ---
const (
	MsgHello            ControlMessageType = "hello"
	MsgHeartbeat        ControlMessageType = "heartbeat"
	MsgTaskLeaseRenewal ControlMessageType = "task_lease_renewal"
	MsgTaskAccepted     ControlMessageType = "task_accepted"
	MsgTaskRejected     ControlMessageType = "task_rejected"
	MsgTaskResult       ControlMessageType = "task_result"
	MsgCommandAck       ControlMessageType = "command_ack"
	MsgArtifactUploaded ControlMessageType = "artifact_uploaded"
	MsgGoodbye          ControlMessageType = "goodbye"

	// Artifact Commit Protocol (Fase 3.3 / 3.5) — typed
	// declare-and-completed pipeline. The legacy MsgArtifactUploaded
	// remains the v0 transport; the typed pair below is the one
	// gated by CapabilityTaskOutputDeclaredV1 /
	// CapabilityArtifactUploadCompletedV1.
	MsgTaskOutputDeclared      ControlMessageType = "task_output_declared"
	MsgArtifactUploadCompleted ControlMessageType = "artifact_upload_completed"
	MsgAssetDownloadProgress   ControlMessageType = "asset_download_progress"
	MsgArtifactUploadIntent    ControlMessageType = "artifact_upload_intent"
	MsgPrefetchLifecycleEvent  ControlMessageType = "prefetch_lifecycle_event"
)

// --- Master → Worker ---
const (
	MsgHelloAck            ControlMessageType = "hello_ack"
	MsgTaskOffer           ControlMessageType = "task_offer"
	MsgTaskLeaseGranted    ControlMessageType = "task_lease_granted"
	MsgCommand             ControlMessageType = "command"
	MsgCancelJob           ControlMessageType = "cancel_job"
	MsgDrain               ControlMessageType = "drain"
	MsgConfigurationUpdate ControlMessageType = "configuration_update"
	MsgLeaseRevoked        ControlMessageType = "lease_revoked"
	MsgPing                ControlMessageType = "ping"
	MsgTaskResultAck       ControlMessageType = "task_result_ack"

	// Artifact Commit Protocol (Fase 3.4 / 3.6) — typed
	// upload-plan-and-commit-ack pipeline. Gated by
	// CapabilityArtifactUploadPlanV1 / CapabilityTaskCommitAckV1.
	MsgArtifactUploadPlan      ControlMessageType = "artifact_upload_plan"
	MsgTaskCommitAck           ControlMessageType = "task_commit_ack"
	MsgFutureAssetPlan         ControlMessageType = "future_asset_plan"
	MsgCancelPrefetch          ControlMessageType = "cancel_prefetch"
	MsgArtifactEarlyUploadPlan ControlMessageType = "artifact_early_upload_plan"
)

// IsWorkerToMaster returns true for messages sent from worker to master.
func (t ControlMessageType) IsWorkerToMaster() bool {
	switch t {
	case MsgHello, MsgHeartbeat, MsgTaskLeaseRenewal, MsgTaskAccepted, MsgTaskRejected,
		MsgTaskResult, MsgCommandAck, MsgArtifactUploaded, MsgGoodbye,
		MsgTaskOutputDeclared, MsgArtifactUploadCompleted, MsgAssetDownloadProgress,
		MsgArtifactUploadIntent, MsgPrefetchLifecycleEvent:
		return true
	}
	return false
}

// IsMasterToWorker returns true for messages sent from master to worker.
func (t ControlMessageType) IsMasterToWorker() bool {
	switch t {
	case MsgHelloAck, MsgTaskOffer, MsgTaskLeaseGranted, MsgCommand, MsgCancelJob,
		MsgDrain, MsgConfigurationUpdate, MsgLeaseRevoked, MsgPing, MsgTaskResultAck,
		MsgArtifactUploadPlan, MsgTaskCommitAck, MsgFutureAssetPlan, MsgCancelPrefetch,
		MsgArtifactEarlyUploadPlan:
		return true
	}
	return false
}

// NewMessage creates a ControlMessage with a generated ID and timestamp.
func NewMessage(msgType ControlMessageType, workerID, protocolVersion string) ControlMessage {
	return ControlMessage{
		MessageID:       newMessageID(),
		Type:            msgType,
		WorkerID:        workerID,
		SentAt:          time.Now().UTC(),
		ProtocolVersion: protocolVersion,
	}
}

// NewTypedMessage creates a ControlMessage with a typed proto payload.
func NewTypedMessage(msgType ControlMessageType, workerID, protocolVersion string, typedPayload interface{}) ControlMessage {
	m := NewMessage(msgType, workerID, protocolVersion)
	m.TypedPayload = typedPayload
	return m
}

func newMessageID() string {
	// A5-1 audit fix: google/uuid (already a direct dependency of this
	// module) replaces the hand-rolled UUIDv4 layout. The stdlib
	// implementation is identical in wire shape (canonical 8-4-4-4-12) and
	// additionally panics-safe-fails on the RNG error the manual version
	// silently discarded (a discarded crypto/rand failure would yield a
	// predictable ID).
	return uuid.NewString()
}

// WithSession attaches a session ID to the message.
func (m ControlMessage) WithSession(sessionID string) ControlMessage {
	m.SessionID = sessionID
	return m
}

// WithSequence adds a sequence number to the message.
func (m ControlMessage) WithSequence(seq int64) ControlMessage {
	m.SequenceNumber = seq
	return m
}

// ToJSON marshals the ControlMessage to JSON bytes.
func (m ControlMessage) ToJSON() ([]byte, error) {
	return json.Marshal(m)
}

// ControlMessageFromJSON unmarshals a ControlMessage from JSON bytes.
func ControlMessageFromJSON(data []byte) (ControlMessage, error) {
	var m ControlMessage
	err := json.Unmarshal(data, &m)
	return m, err
}
