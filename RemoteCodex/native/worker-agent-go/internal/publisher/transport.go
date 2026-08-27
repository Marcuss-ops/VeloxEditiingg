// Package publisher implements the per-task artifact upload transports used by
// the worker after it receives an ArtifactUploadPlan from the master.
package publisher

import (
	"context"
	"errors"
	"io"

	"velox-shared/controltransport"
)

// TransportID is the canonical, wire-stable string the master writes into
// UploadTarget.transport_id.
const (
	TransportIDMasterStream         = "master-stream.v1"
	TransportIDObjectStoreMultipart = "object-store-multipart.v1"
)

// Sentinel errors. Use errors.Is rather than matching Error strings.
var (
	// ErrUnknownTransport is returned by Registry.Resolve when the supplied
	// transport_id is not registered.
	ErrUnknownTransport = errors.New("publisher: unknown transport_id")

	// ErrUploadFailed is the catch-all transport-layer failure after retries
	// are exhausted.
	ErrUploadFailed = errors.New("publisher: upload failed after retries")

	// ErrChecksumMismatch is returned when the remote-side verification pass
	// reports a SHA-256 that does not match the worker's manifest hash.
	ErrChecksumMismatch = errors.New("publisher: remote checksum mismatch")
)

// Transport abstracts one upload mechanism. Implementations must be safe to
// call from a single goroutine; the pipeline serializes per-Attempt but may fan
// out across Attempts.
type Transport interface {
	// ID returns the canonical transport_id this Transport handles.
	ID() string

	// Capabilities returns the protocol features supported by this transport.
	Capabilities() CapabilitySet

	// Upload streams the local file to the per-target destination.
	Upload(ctx context.Context, t UploadRequest) (*UploadResult, error)
}

// CapabilitySet is an immutable-by-convention transport capability list.
type CapabilitySet []string

func (s CapabilitySet) Supports(capability string) bool {
	for _, value := range s {
		if value == capability {
			return true
		}
	}
	return false
}

// SupportsProgressive reports whether a transport can execute the negotiated
// progressive-upload protocol, rather than merely advertising its name.
func SupportsProgressive(t Transport) bool {
	if t == nil || !controltransport.HasProgressiveUploadCapability(capabilityMap(t.Capabilities())) {
		return false
	}
	_, ok := t.(ProgressiveTransport)
	return ok
}

func capabilityMap(capabilities CapabilitySet) map[string]interface{} {
	m := make(map[string]interface{}, len(capabilities))
	for _, capability := range capabilities {
		m[capability] = true
	}
	return m
}

// ProgressiveTransport is an optional extension of Transport. Implementations
// must reuse their existing upload protocol and may expose progressive upload
// without affecting legacy Upload callers.
type ProgressiveTransport interface {
	Transport
	BeginProgressive(ctx context.Context, req ProgressiveUploadRequest) (ProgressiveSession, error)
	ResumeProgressive(ctx context.Context, req ProgressiveUploadRequest, completed []int) (ProgressiveSession, error)
}

// ProgressiveUploadRequest contains metadata known before the final artifact
// identity exists.
type ProgressiveUploadRequest struct {
	Target       UploadTarget
	Artifact     string
	ExpectedSize int64
	CommitToken  string
}

// ProgressiveSession uploads immutable ranges and is completed only after the
// renderer provides the final artifact identity.
type ProgressiveSession interface {
	UploadPart(ctx context.Context, partNumber int, reader io.Reader, size int64) error
	Complete(ctx context.Context, final FinalArtifactIdentity) (*UploadResult, error)
	Abort(ctx context.Context) error
}

type FinalArtifactIdentity struct {
	SHA256    string
	SizeBytes int64
	// EngineFinalized and OutputDurable are explicit protocol evidence. A
	// transport must never infer either fact from the presence of a file.
	EngineFinalized bool
	OutputDurable   bool
	// UploadedParts/ExpectedParts make the COMPLETE boundary fail closed.
	UploadedParts int
	ExpectedParts int
}

// UploadRequest is the per-target input to Transport.Upload.
type UploadRequest struct {
	// LocalPath is the on-disk file the worker wants to upload.
	LocalPath string
	// Target contains the per-manifest instructions from ArtifactUploadPlan.
	Target UploadTarget
	// WorkerSHA256 is the hex SHA-256 computed by the worker.
	WorkerSHA256 string
	// CommitToken is the short-lived master-issued token authorizing the
	// master-stream upload target. It is sent only as an HTTP header and is
	// never logged by transports.
	CommitToken string
	// Progress is invoked at least once per chunk when non-nil.
	Progress func(uploadedBytes int64)
	// Telemetry receives chunk, retry, and finalize observations.
	Telemetry UploadTelemetry
}

// UploadResult is the per-target output of Transport.Upload.
type UploadResult struct {
	// Breakdown contains measured upload timings and counters.
	Breakdown UploadBreakdown
	// UploadID is the canonical upload_id the master expects.
	UploadID string
	// UploadedBytes is the total number of bytes transferred end-to-end.
	UploadedBytes int64
	// ServerSHA256 is the SHA-256 computed by the remote side, when available.
	ServerSHA256 string
}

// UploadTarget mirrors the proto UploadTarget and the DataServer UploadTarget.
// It lives here so this package can be tested without importing the proto
// runtime.
type UploadTarget struct {
	DeclarationID string
	ArtifactID    string
	UploadID      string
	TransportID   string
	UploadURL     string
	ChunkSize     int64
	ExpiresAtUnix int64
}
