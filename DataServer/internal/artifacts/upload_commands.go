// Package artifacts / upload_commands.go — Service inputs/outputs for
// upload-session I/O. These contracts are NOT Repository I/O: callers
// in handlers always construct them and pass them to Service methods.
//
// Hold these types in a dedicated file (rather than inlining into
// service.go / service_receive.go / service_finalize.go) so a future
// refactor that consolidates upload I/O can find them without
// grepping across the three Service files.
package artifacts

import "time"

// BeginUploadCommand is the input to Service.BeginUpload (Fase 1).
//
// The worker presents this BEFORE the bytes are streamed. The master
// uses the auth fields (worker_id, lease_id, attempt_number,
// expected_revision) and the in-memory job/attempt state to authorize
// the upload. The hint fields (kind, mime, expected_size, expected_sha)
// are NEVER trusted as authoritative — they are stored for diagnostics
// and surfaced to the worker only when the master disagrees.
type BeginUploadCommand struct {
	JobID            string
	WorkerID         string
	LeaseID          string
	AttemptNumber    int
	ExpectedRevision int

	// Worker-declared hints (diagnostic only).
	Kind              string
	MimeType          string
	ExpectedSizeBytes int64
	ExpectedSHA256    string
}

// FinalizeArtifactCommand is the master-side adapter from the gRPC
// ArtifactUploaded message. It carries ONLY the IDs / auth fields —
// the legacy `artifact_path`, `artifact_size`, `sha256` fields are
// ignored because they cannot be trusted (the master recomputes SHA
// and size from the streamed bytes).
//
// DestinationID is the optional single-destination override that
// pins the finalize writer to a single-delivery plan (mirrors the
// writer's resolveDeliveryDestinationsTx branch 1). Empty (the
// default) means "use the per-job plan resolver" — production
// callers leave it empty unless they explicitly pin routing. The
// pre-commit ffprobe gate (RW-PROD-008 A4) reads this field to
// compute the expected audio-stream count for the invariant.
type FinalizeArtifactCommand struct {
	UploadID         string
	JobID            string
	WorkerID         string
	LeaseID          string
	AttemptNumber    int
	ExpectedRevision int
	DestinationID    string
}

// FinalizeArtifactAndCompleteJobCommand has been REMOVED. The single
// legal writer of jobs.status = 'SUCCEEDED' is now the
// artifacts.FinalizationWriter.FinalizeVerified method (see
// internal/artifacts/sqlite_finalize_writer.go). Use
// artifacts.FinalizeVerifiedCommand + Service.Finalize.

// ReceiveResult is what Service.Receive returns after streaming bytes.
//
// The master-computed hash + size are stored on the row, and Status
// moves to RECEIVED so the next Finalize call can re-hash from disk
// (defence-in-depth in case Receive and the worker's Finalize report
// race or arrive out of order).
type ReceiveResult struct {
	UploadID          string
	ReceivedSizeBytes int64
	ReceivedSHA256    string
	Status            string
}

// _ ensures `time` import is preserved if a future patch adds
// timestamp fields; cheap doc-only dependency.
var _ = time.Time{}
