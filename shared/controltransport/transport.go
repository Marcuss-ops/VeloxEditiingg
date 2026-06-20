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
// Payload is deprecated in favor of TypedPayload for proto-typed messages.
type ControlMessage struct {
	MessageID       string                 `json:"message_id"`
	Type            ControlMessageType     `json:"type"`
	WorkerID        string                 `json:"worker_id"`
	SessionID       string                 `json:"session_id,omitempty"`
	SequenceNumber  int64                  `json:"sequence_number,omitempty"`
	SentAt          time.Time              `json:"sent_at"`
	ProtocolVersion string                 `json:"protocol_version"`
	Payload         map[string]interface{} `json:"payload,omitempty"` // deprecated: use TypedPayload
	TypedPayload    interface{}            `json:"-"`                 // typed proto message (e.g. *pb.JobOffer, *pb.Command)
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
	Capabilities    map[string]interface{} `json:"capabilities,omitempty"`
}

// ProtocolVersionCurrent is the protocol version declared by this package.
const ProtocolVersionCurrent = "2026-06-worker-v1"
