// Package artifacts / upload_commands.go
//
// Upload-session I/O contracts (Service inputs/outputs). These types
// used to live in artifacts/uploads.go alongside the SQLiteRepository
// implementation. After file-1/4 of the canonical-SQL-gateway
// migration moved the SQLiteRepository to store/artifact_uploads.go,
// these contracts remain in the artifacts package because:
//
//   - They are NOT Repository I/O. The Repository never sees
//     BeginUploadCommand or FinalizeArtifactCommand; callers always
//     construct them in handlers and pass them to Service methods.
//   - The ReceiveResult is what Service.Receive returns; it never
//     comes back from any DB query.
//
// Migration note: hold these types in a dedicated file (rather than
// inlining into service.go / service_receive.go / service_finalize.go)
// so a future refactor that consolidates upload I/O can find them
// without grepping across the three Service files.
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
// ignored because they cannot be trusted (see PR 2 spec, Fase 4).
type FinalizeArtifactCommand struct {
	UploadID         string
	JobID            string
	WorkerID         string
	LeaseID          string
	AttemptNumber    int
	ExpectedRevision int
}

// PR 3.5-a: FinalizeArtifactAndCompleteJobCommand has been REMOVED.
// The single legal writer of jobs.status = 'SUCCEEDED' is now the
// artifacts.FinalizationRepository.FinalizeVerified method (see
// internal/artifacts/sqlite_finalization_repository.go). Use
// artifacts.FinalizeVerifiedCommand + Service.Finalize.
//
// Historically this struct lived in artifacts/uploads.go so
// service.go's FinalizeArtifactAndCompleteJob method could use it
// directly; that method itself was deleted as part of the same
// migration.

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
