package controltransport

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestControlMessageRoundTrip(t *testing.T) {
	original := ControlMessage{
		MessageID:       newMessageID(),
		Type:            MsgHeartbeat,
		WorkerID:        "worker-001",
		SessionID:       "sess-123",
		SequenceNumber:  42,
		SentAt:          time.Now().UTC(),
		ProtocolVersion: ProtocolVersionCurrent,
		Payload: map[string]interface{}{
			"status":       "busy",
			"active_jobs":  2,
			"capabilities": []string{"render", "video"},
		},
	}

	data, err := original.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}

	restored, err := ControlMessageFromJSON(data)
	if err != nil {
		t.Fatalf("ControlMessageFromJSON failed: %v", err)
	}

	if restored.MessageID != original.MessageID {
		t.Errorf("MessageID mismatch: %q != %q", restored.MessageID, original.MessageID)
	}
	if restored.Type != original.Type {
		t.Errorf("Type mismatch: %q != %q", restored.Type, original.Type)
	}
	if restored.WorkerID != original.WorkerID {
		t.Errorf("WorkerID mismatch: %q != %q", restored.WorkerID, original.WorkerID)
	}
	if restored.SessionID != original.SessionID {
		t.Errorf("SessionID mismatch: %q != %q", restored.SessionID, original.SessionID)
	}
	if restored.SequenceNumber != original.SequenceNumber {
		t.Errorf("SequenceNumber mismatch: %d != %d", restored.SequenceNumber, original.SequenceNumber)
	}
	if restored.ProtocolVersion != original.ProtocolVersion {
		t.Errorf("ProtocolVersion mismatch: %q != %q", restored.ProtocolVersion, original.ProtocolVersion)
	}

	// Payload round-trip
	if len(restored.Payload) != len(original.Payload) {
		t.Errorf("Payload length mismatch: %d != %d", len(restored.Payload), len(original.Payload))
	}
	if v, ok := restored.Payload["status"].(string); !ok || v != "busy" {
		t.Errorf("Payload status mismatch: got %v", restored.Payload["status"])
	}
}

func TestControlMessageJSONOmitEmpty(t *testing.T) {
	// SessionID and Payload are omitempty — verify they're omitted when empty
	m := ControlMessage{
		MessageID:       newMessageID(),
		Type:            MsgPing,
		WorkerID:        "worker-001",
		SentAt:          time.Now().UTC(),
		ProtocolVersion: ProtocolVersionCurrent,
	}
	data, err := m.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, exists := raw["session_id"]; exists {
		t.Error("session_id should be omitted when empty")
	}
	if _, exists := raw["payload"]; exists {
		t.Error("payload should be omitted when empty")
	}
}

func TestControlMessageTypeClassification(t *testing.T) {
	workerToMaster := []ControlMessageType{
		MsgHello, MsgHeartbeat, MsgLeaseRenewal, MsgJobAccepted,
		MsgJobRejected, MsgJobProgress, MsgCommandAck, MsgArtifactUploaded,
		MsgJobResult, MsgGoodbye,
	}
	for _, mt := range workerToMaster {
		if !mt.IsWorkerToMaster() {
			t.Errorf("%s should be worker→master", mt)
		}
		if mt.IsMasterToWorker() {
			t.Errorf("%s should NOT be master→worker", mt)
		}
	}

	masterToWorker := []ControlMessageType{
		MsgHelloAck, MsgJobOffer, MsgCommand, MsgCancelJob,
		MsgDrain, MsgConfigurationUpdate, MsgLeaseRevoked, MsgPing,
	}
	for _, mt := range masterToWorker {
		if !mt.IsMasterToWorker() {
			t.Errorf("%s should be master→worker", mt)
		}
		if mt.IsWorkerToMaster() {
			t.Errorf("%s should NOT be worker→master", mt)
		}
	}
}

func TestNewMessage(t *testing.T) {
	m := NewMessage(MsgHello, "worker-001", ProtocolVersionCurrent)
	if m.MessageID == "" {
		t.Error("MessageID should not be empty")
	}
	if m.Type != MsgHello {
		t.Errorf("Type mismatch: %q", m.Type)
	}
	if m.WorkerID != "worker-001" {
		t.Errorf("WorkerID mismatch: %q", m.WorkerID)
	}
	if m.ProtocolVersion != ProtocolVersionCurrent {
		t.Errorf("ProtocolVersion mismatch: %q", m.ProtocolVersion)
	}
	if m.SentAt.IsZero() {
		t.Error("SentAt should not be zero")
	}
}

func TestNewMessageWithPayload(t *testing.T) {
	payload := map[string]interface{}{"key": "value"}
	m := NewMessageWithPayload(MsgJobResult, "w1", ProtocolVersionCurrent, payload)
	if m.Payload == nil {
		t.Fatal("Payload should not be nil")
	}
	if v, ok := m.Payload["key"].(string); !ok || v != "value" {
		t.Errorf("Payload mismatch: %v", m.Payload["key"])
	}
}

func TestMessageHelpers(t *testing.T) {
	m := NewMessage(MsgHeartbeat, "w1", ProtocolVersionCurrent)
	m = m.WithSession("sess-abc")
	m = m.WithSequence(100)

	if m.SessionID != "sess-abc" {
		t.Errorf("SessionID not set: %q", m.SessionID)
	}
	if m.SequenceNumber != 100 {
		t.Errorf("SequenceNumber not set: %d", m.SequenceNumber)
	}
}

func TestTransportErrors(t *testing.T) {
	// Sentinel errors are non-nil
	if ErrTransportClosed == nil {
		t.Error("ErrTransportClosed should be non-nil")
	}
	if ErrSessionExpired == nil {
		t.Error("ErrSessionExpired should be non-nil")
	}
	if ErrAuthFailed == nil {
		t.Error("ErrAuthFailed should be non-nil")
	}

	// TransportError wraps correctly
	te := NewTransportError("send", ErrTransportClosed, "heartbeat rejected")
	if te.Op != "send" {
		t.Errorf("Op mismatch: %q", te.Op)
	}
	if !errors.Is(te, ErrTransportClosed) {
		t.Error("TransportError should unwrap to ErrTransportClosed")
	}

	expectedMsg := "send: heartbeat rejected: transport is closed"
	if te.Error() != expectedMsg {
		t.Errorf("Error() mismatch: %q != %q", te.Error(), expectedMsg)
	}
}

func TestTransportErrorWithoutMessage(t *testing.T) {
	te := NewTransportError("connect", ErrAuthFailed, "")
	expectedMsg := "connect: authentication failed — invalid credentials"
	if te.Error() != expectedMsg {
		t.Errorf("Error() mismatch: %q != %q", te.Error(), expectedMsg)
	}
}

func TestNewMessageIDUniqueness(t *testing.T) {
	ids := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		id := newMessageID()
		if ids[id] {
			t.Fatalf("Duplicate message ID generated: %s", id)
		}
		ids[id] = true

		// Verify UUID v4 format: xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
		if len(id) != 36 {
			t.Errorf("Invalid UUID length: %d (%s)", len(id), id)
		}
		if id[14] != '4' {
			t.Errorf("Missing version nibble: %s", id)
		}
		if id[19] != '8' && id[19] != '9' && id[19] != 'a' && id[19] != 'b' {
			t.Errorf("Missing variant bits: %s", id)
		}
	}
}

