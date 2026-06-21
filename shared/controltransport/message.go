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
	MsgLeaseRenewal     ControlMessageType = "lease_renewal"
	MsgJobAccepted      ControlMessageType = "job_accepted"
	MsgJobRejected      ControlMessageType = "job_rejected"
	MsgJobProgress      ControlMessageType = "job_progress"
	MsgCommandAck       ControlMessageType = "command_ack"
	MsgArtifactUploaded ControlMessageType = "artifact_uploaded"
	MsgJobResult        ControlMessageType = "job_result"
	MsgGoodbye          ControlMessageType = "goodbye"
)

// --- Master → Worker ---
const (
	MsgHelloAck            ControlMessageType = "hello_ack"
	MsgJobOffer            ControlMessageType = "job_offer" // full job with lease_id from SQLite CAS (push mode)
	MsgJobLeaseGranted     ControlMessageType = "job_lease_granted"
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
	case MsgHello, MsgHeartbeat, MsgLeaseRenewal, MsgJobAccepted,
		MsgJobRejected, MsgJobProgress, MsgCommandAck, MsgArtifactUploaded,
		MsgJobResult, MsgGoodbye:
		return true
	}
	return false
}

// IsMasterToWorker returns true for messages sent from master to worker.
func (t ControlMessageType) IsMasterToWorker() bool {
	switch t {
	case MsgHelloAck, MsgJobOffer, MsgJobLeaseGranted, MsgCommand, MsgCancelJob,
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

// NewMessageWithPayload creates a ControlMessage with payload data.
func NewMessageWithPayload(msgType ControlMessageType, workerID, protocolVersion string, payload map[string]interface{}) ControlMessage {
	m := NewMessage(msgType, workerID, protocolVersion)
	m.Payload = payload
	return m
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
