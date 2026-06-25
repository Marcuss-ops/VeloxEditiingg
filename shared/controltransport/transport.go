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
	// WorkerClass is the operator-assigned fleet class (RW-PROD-005 §3 A9).
	// Binds from VELOX_WORKER_CLASS env on worker side → master WorkerInfo.Class.
	WorkerClass     string                 `json:"worker_class,omitempty"`
	// RolloutGroup is the operator-assigned rollout cohort (RW-PROD-005 §3 A9).
	// Binds from VELOX_ROLLOUT_GROUP env on worker side → master WorkerInfo.RolloutGroup.
	RolloutGroup    string                 `json:"rollout_group,omitempty"`
	Capabilities    map[string]interface{} `json:"capabilities,omitempty"`
}

// ProtocolVersionCurrent is the protocol version declared by this package.
// v3 is the canonical version after the typed-metrics cutover (PR-5 / F2
// follow-up). Old workers that speak the calendar-named v1 still connect
// but receive a `[DEPRECATED]` log line — see IsSupportedProtocol.
const ProtocolVersionCurrent = "v3"

// ProtocolVersionLegacy is the calendar-named v1 string used by workers
// shipped before the typed-metrics cutover (pre-PR-5). The master
// accepts this version with a deprecation warning so a mixed fleet
// (some v1 workers, some v3 workers) keeps operating until operators
// finish rolling the fleet to v3.
const ProtocolVersionLegacy = "2026-06-worker-v1"

// SupportedProtocolVersions is the closed set of protocol versions the
// master accepts at the gRPC handshake. New workers MUST emit
// ProtocolVersionCurrent; older versions still work but log a
// deprecation warning.
//
// Bump policy: add a new legacy entry on a backward-compat cutover,
// remove an old entry after the fleet has been drained (6-month
// rolling window is the audit-canonical grace period).
var SupportedProtocolVersions = []string{
	ProtocolVersionCurrent, // "v3" (current; typed metrics)
	ProtocolVersionLegacy,  // "2026-06-worker-v1" (legacy; pre-typed-metrics)
}

// IsSupportedProtocol reports whether `v` is one of the protocol
// versions the master accepts. The empty string is supported because
// pre-versioned workers (builds older than this convention) emit no
// protocol_version field — we accept them with a [DEPRECATED] log on
// the master side so the heartbeat stream is healthy.
//
// Use IsDeprecatedProtocol to additionally distinguish the legacy
// calendar-named v1 from the current.
func IsSupportedProtocol(v string) bool {
	if v == "" {
		return true
	}
	for _, s := range SupportedProtocolVersions {
		if s == v {
			return true
		}
	}
	return false
}

// IsDeprecatedProtocol reports whether `v` is supported but is NOT the
// current version. Used by gRPC handler.go to emit a one-time
// [DEPRECATED] log on the Hello handshake so operators know to
// upgrade their fleet.
func IsDeprecatedProtocol(v string) bool {
	if v == "" {
		return false
	}
	if v == ProtocolVersionCurrent {
		return false
	}
	return IsSupportedProtocol(v)
}
