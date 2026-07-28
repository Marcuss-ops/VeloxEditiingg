package controltransport

import "errors"

// Typed errors for ControlTransport operations.
var (
	// ErrTransportClosed is returned when an operation is attempted on a closed transport.
	ErrTransportClosed = errors.New("transport is closed")

	// ErrSessionExpired is returned when the session token has expired.
	ErrSessionExpired = errors.New("session expired — re-registration required")

	// ErrAuthFailed is returned when worker authentication fails.
	ErrAuthFailed = errors.New("authentication failed — invalid credentials")

	// ErrUnsupportedMessage is returned when a message type is not supported by the transport.
	ErrUnsupportedMessage = errors.New("unsupported message type")

	// ErrNotConnected is returned when trying to send/receive without an active connection.
	ErrNotConnected = errors.New("not connected")

	// ErrWorkerIDCollision is returned when the master rejects the worker's
	// Hello/HelloAck handshake with codes.AlreadyExists, signaling that
	// another machine is already registered with the same worker_id on a
	// different credential. This is a hard configuration error: two
	// physical machines cannot share a single worker_id. Callers should
	// detect this via errors.Is(err, controltransport.ErrWorkerIDCollision)
	// and exit loudly (exit code 17) rather than retry with backoff —
	// retrying would mask the operational fault.
	//
	// Anti-collision invariant (RW-PROD-005 §3): the master-side handler
	// emits codes.AlreadyExists whenever CheckActiveSessionCollision finds
	// an existing ACTIVE session for the same (worker_id, session_type)
	// with a different token_hash. The race-window trigger
	// `trg_worker_sessions_one_active` (migration 094 + 095) is the
	// authoritative backstop for concurrent inserts that slip past the
	// pre-check; both paths funnel into this same sentinel on the worker
	// side so the diagnostic is uniform.
	ErrWorkerIDCollision = errors.New("worker_id already connected on a different credential (worker_id collision: two machines with the same identity)")
)

// TransportError wraps an error with additional context about the transport operation.
type TransportError struct {
	Op      string // The operation that failed (e.g., "connect", "send", "receive")
	Err     error  // The underlying error
	Message string // Optional human-readable context
}

// Error implements the error interface.
func (e *TransportError) Error() string {
	if e.Message != "" {
		return e.Op + ": " + e.Message + ": " + e.Err.Error()
	}
	return e.Op + ": " + e.Err.Error()
}

// Unwrap returns the underlying error.
func (e *TransportError) Unwrap() error {
	return e.Err
}

// NewTransportError creates a TransportError.
func NewTransportError(op string, err error, message string) *TransportError {
	return &TransportError{
		Op:      op,
		Err:     err,
		Message: message,
	}
}
