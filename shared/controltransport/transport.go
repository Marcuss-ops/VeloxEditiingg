// Package controltransport defines the common interface and types for worker↔master communication.
// Implementations (HTTP polling, gRPC stream) satisfy this interface, allowing the worker
// to operate without knowledge of the underlying transport mechanism.
package controltransport

import (
	"context"
	"time"
)

// ControlTransport defines the bidirectional communication channel between worker and master.
type ControlTransport interface {
	// Connect establishes the connection and authenticates the worker.
	// In HTTP polling mode this is a no-op (auth happens during register).
	// In gRPC mode this opens a bidirectional stream and completes the handshake.
	// Must be called before Receive() or Send().
	// Each call creates a new underlying connection; the transport is NOT reusable
	// after Close().
	Connect(ctx context.Context, hello WorkerHello) error

	// Receive returns a message channel (master→worker) and an error channel.
	// The message channel is closed when the transport is closed or an unrecoverable
	// error occurs. The error channel receives the terminal error (if any) and is
	// then closed. If the transport closes cleanly, the error channel is closed
	// with no value sent.
	// Must be called after a successful Connect().
	Receive(ctx context.Context) (<-chan ControlMessage, <-chan error, error)

	// Send transmits a message to the master.
	// Returns an error if the transport is closed or the send fails.
	Send(ctx context.Context, message ControlMessage) error

	// Close gracefully terminates the transport.
	// Sends a Goodbye message and closes the underlying connection.
	// Idempotent: calling Close on an already-closed transport is a no-op.
	// After Close(), the transport must NOT be reused — create a new instance.
	Close() error
}

// ControlMessage represents a typed message exchanged over the control transport.
// For master→worker messages, TypedPayload carries a proto message (e.g. *pb.TaskOffer).
// For worker→master sends, TypedPayload carries a typed proto message (e.g. *pb.TaskAccepted)
// that the transport converts to a WorkerToMasterEnvelope — the legacy Payload map is removed.
type ControlMessage struct {
	MessageID       string                 `json:"message_id"`
	Type            ControlMessageType     `json:"type"`
	WorkerID        string                 `json:"worker_id"`
	SessionID       string                 `json:"session_id,omitempty"`
	SequenceNumber  int64                  `json:"sequence_number,omitempty"`
	SentAt          time.Time              `json:"sent_at"`
	ProtocolVersion string                 `json:"protocol_version"`
	TypedPayload    interface{}            `json:"-"` // typed proto message (e.g. *pb.TaskOffer, *pb.TaskAccepted)
}

// WorkerHello contains the data sent during initial connection/registration.
type WorkerHello struct {
	WorkerID        string                 `json:"worker_id"`
	WorkerName      string                 `json:"worker_name"`
	Hostname        string                 `json:"hostname"`
	Version         string                 `json:"version"`
	BundleVersion   string                 `json:"bundle_version,omitempty"`
	BundleHash      string                 `json:"bundle_hash,omitempty"`
	ProtocolVersion string                 `json:"protocol_version"`
	EngineVersion   string                 `json:"engine_version,omitempty"`
	CredentialHash  string                 `json:"credential_hash,omitempty"`
	// WorkerClass is the operator-assigned fleet class (RW-PROD-005 §3 A9).
	// Binds from VELOX_WORKER_CLASS env on worker side → master WorkerInfo.Class.
	WorkerClass     string                 `json:"worker_class,omitempty"`
	// RolloutGroup is the operator-assigned rollout cohort (RW-PROD-005 §3 A9).
	// Binds from VELOX_ROLLOUT_GROUP env on worker side → master WorkerInfo.RolloutGroup.
	RolloutGroup    string                 `json:"rollout_group,omitempty"`
	Capabilities    map[string]interface{} `json:"capabilities,omitempty"`
}

// ProtocolVersionCurrent is the ONLY accepted protocol version.
// Workers that emit any other version (including empty string) are rejected
// at the gRPC handshake with FailedPrecondition.
const ProtocolVersionCurrent = "v3"

// SupportedProtocolVersions is the closed set of protocol versions the
// master accepts at the gRPC handshake. Only ProtocolVersionCurrent is
// accepted; legacy versions are no longer supported.
var SupportedProtocolVersions = []string{
	ProtocolVersionCurrent,
}

// IsSupportedProtocol reports whether `v` is the current protocol version.
// Empty strings and legacy versions are rejected.
func IsSupportedProtocol(v string) bool {
	return v == ProtocolVersionCurrent
}
