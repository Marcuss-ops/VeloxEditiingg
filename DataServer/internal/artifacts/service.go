// Package artifacts / service.go
//
// Master-side authority on artifact state transitions
// (BeginUpload → Receive → Finalize). Three phase boundaries:
//
//   - BeginUpload — validation + atomic insert via UploadSessionWriter.
//   - Receive     — streaming + master-computed hash + post-write verify
//     via the typed store.UploadRepository.
//   - Finalize    — blob promotion + FinalizationWriter atomic tx
//     (sole jobs.status='SUCCEEDED' writer) +
//     ArtifactReader post-tx read.
//
// The Service is the only layer that computes SHA-256 / size from the
// actual bytes — workers cannot influence the canonical storage_key,
// the artifact status, or the job status; they can only REQUEST a
// transition that the master authorizes + verifies.
//
// Persistence is composed from narrowly-scoped readers/writers. The
// Service never holds a database handle directly — read paths go through
// AuthReader, write paths through UploadSessionWriter /
// FinalizationWriter / ArtifactReader. The Reconciler also delegates
// every persistence operation to its typed repository.
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
// AuthReader so the auth path never sees a raw database handle.
//
// None of these fields are optional at runtime — NewService panics
// on nil for each so a misconfigured composition fails fast at
// startup instead of silently producing no SUCCEEDED.
type Service struct {
	repo           store.UploadRepository
	uploadWriter   UploadSessionWriter
	finalizeWriter FinalizationWriter
	artifactReader ArtifactReader
	auth           AuthReader
	blobStore      store.BlobStore
	clock          clock.Clock
	// deliveryCounter is the purpose-built typed reader used by
	// the VELOX_FFPROBE_VERIFY_ON_FINALIZE invariant (see
	// service_finalize_ffprobe.go). Required at construction:
	// NewService panics if the caller passes nil so a production
	// deployment cannot silently run the gate with no counter.
	deliveryCounter JobDeliveryCounter

	uploadTTL   time.Duration
	ffprobeMode ffprobeInvariantMode
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
//     per-job uniqueness gate). Hides the database handle from Service.
//   - c: clock; nil substitutes clock.System (production default).
//   - deliveryCounter: JobDeliveryCounter typed reader consumed by
//     the VELOX_FFPROBE_VERIFY_ON_FINALIZE gate (RW-PROD-008 A4).
//     Required (panics on nil) so a production deployment never
//     silently runs the gate with no counter wired. Future Postgres
//     support can swap in a parallel implementation without touching
//     Service.
//
// All store adapters share the same database pool so the finalization
// transaction coordinates with concurrent upload-session updates;
// Service itself never sees the database handle.
func NewService(
	repo store.UploadRepository,
	uploadWriter UploadSessionWriter,
	finalizeWriter FinalizationWriter,
	artifactReader ArtifactReader,
	blobStore store.BlobStore,
	auth AuthReader,
	c clock.Clock,
	deliveryCounter JobDeliveryCounter,
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
	if deliveryCounter == nil {
		panic("artifacts: NewService requires a non-nil JobDeliveryCounter (post-finalize ffprobe invariant — RW-PROD-008 A4)")
	}
	return &Service{
		repo:            repo,
		uploadWriter:    uploadWriter,
		finalizeWriter:  finalizeWriter,
		artifactReader:  artifactReader,
		auth:            auth,
		blobStore:       blobStore,
		clock:           c,
		deliveryCounter: deliveryCounter,
		uploadTTL:       defaultUploadTTL,
	}
}

// WithUploadTTL adjusts the upload session expiry window (tests).
func (s *Service) WithUploadTTL(d time.Duration) *Service {
	s.uploadTTL = d
	return s
}

// WithFFProbeMode injects the pre-commit ffprobe invariant mode
// parsed from the captured config value (VELOX_FFPROBE_VERIFY_ON_FINALIZE
// is read exactly once at bootstrap; the Service never consults the
// process environment). Empty/invalid literals stay Off, matching the
// strict-literal contract documented in service_finalize_ffprobe.go.
func (s *Service) WithFFProbeMode(mode string) *Service {
	s.ffprobeMode = parseFFProbeMode(mode)
	return s
}
