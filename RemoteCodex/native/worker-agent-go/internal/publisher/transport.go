// Package publisher implements the per-task artifact upload transports used by
// the worker after it receives an ArtifactUploadPlan from the master.
package publisher

import (
	"context"
	"errors"
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

	// Upload streams the local file to the per-target destination.
	Upload(ctx context.Context, t UploadRequest) (*UploadResult, error)
}

// UploadRequest is the per-target input to Transport.Upload.
type UploadRequest struct {
	// LocalPath is the on-disk file the worker wants to upload.
	LocalPath string
	// Target contains the per-manifest instructions from ArtifactUploadPlan.
	Target UploadTarget
	// WorkerSHA256 is the hex SHA-256 computed by the worker.
	WorkerSHA256 string
	// Progress is invoked at least once per chunk when non-nil.
	Progress func(uploadedBytes int64)
}

// UploadResult is the per-target output of Transport.Upload.
type UploadResult struct {
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
