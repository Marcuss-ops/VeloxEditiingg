package controltransport

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
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
)

// IsWorkerToMaster returns true for messages sent from worker to master.
func (t ControlMessageType) IsWorkerToMaster() bool {
	switch t {
	case MsgHello, MsgHeartbeat, MsgTaskLeaseRenewal, MsgTaskAccepted, MsgTaskRejected,
		MsgTaskResult, MsgCommandAck, MsgArtifactUploaded, MsgGoodbye:
		return true
	}
	return false
}

// IsMasterToWorker returns true for messages sent from master to worker.
func (t ControlMessageType) IsMasterToWorker() bool {
	switch t {
	case MsgHelloAck, MsgTaskOffer, MsgTaskLeaseGranted, MsgCommand, MsgCancelJob,
		MsgDrain, MsgConfigurationUpdate, MsgLeaseRevoked, MsgPing:
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
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	// UUID v4 layout
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hex.EncodeToString(b[0:4]),
		hex.EncodeToString(b[4:6]),
		hex.EncodeToString(b[6:8]),
		hex.EncodeToString(b[8:10]),
		hex.EncodeToString(b[10:16]))
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
