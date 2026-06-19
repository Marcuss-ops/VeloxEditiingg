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
