// Package artifacts / service.go
//
// Master-side authority on artifact state transitions
// (BeginUpload → Receive → Finalize). Three phase boundaries:
//
//   - BeginUpload — validation + atomic insert via UploadSessionWriter.
//   - Receive     — streaming + master-computed hash + post-write verify
//                   via the typed store.UploadRepository.
//   - Finalize    — blob promotion + FinalizationWriter atomic tx
//                   (sole jobs.status='SUCCEEDED' writer) +
//                   ArtifactReader post-tx read.
//
// The Service is the only layer that computes SHA-256 / size from the
// actual bytes — workers cannot influence the canonical storage_key,
// the artifact status, or the job status; they can only REQUEST a
// transition that the master authorizes + verifies.
//
// Persistence is composed from narrowly-scoped readers/writers. The
// Service never holds *sql.DB directly — read paths go through
// AuthReader, write paths through UploadSessionWriter /
// FinalizationWriter / ArtifactReader. The Reconciler retains a
// *sql.DB because its cleanup sweeps operate on tables the typed
// repos do not yet expose (sql-allowlist marker at top of
// reconciler.go).
package artifacts

import (
	"time"

	"velox-server/internal/platform/clock"
	"velox-server/internal/store"
)

// defaultUploadTTL matches the spec's reconciler rule
// ("blob finale senza riga DB dopo 24h → elimina") so the same window
// sweeps both orphaned upload sessions and orphaned final blobs.
const defaultUploadTTL = 24 * time.Hour

// Service composes the persistence surfaces and the blob store for the
// three upload-finalization phases. Auth reads are isolated behind
// AuthReader so the auth path never sees a raw *sql.DB.
//
// None of these fields are optional at runtime — NewService panics on
// nil for each so a misconfigured composition fails fast at startup
// instead of silently producing no SUCCEEDED.
type Service struct {
	repo           store.UploadRepository
	uploadWriter   UploadSessionWriter
	finalizeWriter FinalizationWriter
	artifactReader ArtifactReader
	auth           AuthReader
	blobStore      store.BlobStore
	clock          clock.Clock

	uploadTTL time.Duration
}

// NewService composes the dependencies Service needs.
//
//   - repo: per-session CRUD (state machine + read loads + chunks).
//   - uploadWriter: atomic paired-insert of artifacts + artifact_uploads.
//   - finalizeWriter: atomic verified-finalization tx; the sole legal
//     writer of jobs.status='SUCCEEDED'.
//   - artifactReader: read-only artifact projection; consumed by the
//     idempotent COMPLETED path and downstream callers.
//   - blobStore: FilesystemBlobStore in production, NopBlobStore in tests.
//   - auth: read-only auth queries (job state, attempt identity,
//     per-job uniqueness gate). Hides *sql.DB from Service.
//
// All artifacts-package SQLite components share the same *sql.DB so
// the finalize tx can join with concurrent updates on
// artifact_uploads; the AuthReader holds that DB but Service never
// sees the handle.
func NewService(
	repo store.UploadRepository,
	uploadWriter UploadSessionWriter,
	finalizeWriter FinalizationWriter,
	artifactReader ArtifactReader,
	blobStore store.BlobStore,
	auth AuthReader,
	c clock.Clock,
) *Service {
	if c == nil {
		c = clock.System{}
	}
	if repo == nil {
		panic("artifacts: NewService requires a non-nil UploadRepository")
	}
	if uploadWriter == nil {
		panic("artifacts: NewService requires a non-nil UploadSessionWriter")
	}
	if finalizeWriter == nil {
		panic("artifacts: NewService requires a non-nil FinalizationWriter (sole writer of jobs.status='SUCCEEDED')")
	}
	if artifactReader == nil {
		panic("artifacts: NewService requires a non-nil ArtifactReader")
	}
	if blobStore == nil {
		panic("artifacts: NewService requires a non-nil BlobStore")
	}
	if auth == nil {
		panic("artifacts: NewService requires a non-nil AuthReader")
	}
	return &Service{
		repo:           repo,
		uploadWriter:   uploadWriter,
		finalizeWriter: finalizeWriter,
		artifactReader: artifactReader,
		auth:           auth,
		blobStore:      blobStore,
		clock:          c,
		uploadTTL:      defaultUploadTTL,
	}
}

// WithUploadTTL adjusts the upload session expiry window (tests).
func (s *Service) WithUploadTTL(d time.Duration) *Service {
	s.uploadTTL = d
	return s
}
