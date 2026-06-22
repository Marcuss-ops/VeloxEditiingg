// Package artifacts / finalization_repository.go — PR 3.5-a.
package artifacts

import (
	"context"
	"time"

	"velox-server/internal/store"
)

// CreateArtifactAndUploadSessionCommand is the input to
// FinalizationRepository.CreateArtifactAndUploadSession. It replaces the
// previous two-step BeginUpload pattern (artifacts INSERT + artifact_uploads
// INSERT) with a single atomic transaction.
type CreateArtifactAndUploadSessionCommand struct {
	ArtifactID       string
	UploadID         string
	JobID            string
	AttemptID        int64
	Kind             string
	WorkerID         string
	LeaseID          string
	AttemptNumber    int
	ExpectedRevision int

	// StorageProvider on the artifacts row. Defaults to "local" when
	// empty so callers that don't care can omit it; verifies the
	// single-INSERT path matches the existing storage_provider enums.
	StorageProvider string

	// Worker-declared hints (diagnostic only; never trusted by master).
	ExpectedMIME        string
	ExpectedSizeBytes   int64
	ExpectedSHA256      string
	TemporaryStorageKey string

	// CreatedAt / ExpiresAt: zero values are filled in by the
	// repository (CreatedAt = now, ExpiresAt = now + 24h defaultUploadTTL)
	// so callers can pass zero-time without poisoning the schema.
	CreatedAt time.Time
	ExpiresAt time.Time
}

// FinalizeVerifiedCommand is the input to
// FinalizationRepository.FinalizeVerified — the single SQL transaction
// that promotes a job to SUCCEEDED via a verified artifact.
//
// No other code path may flip jobs.status to SUCCEEDED; the
// FinalizationRepository.FinalizeVerified method is the only legal
// writer. Enforced by:
//
//  1. The scan test (scan_test.go) — grep across the data server for
//     `SET status = 'SUCCEEDED'` is rejected outside
//     sqlite_finalization_repository.go.
//  2. The narrow FinalizationRepository interface — the broader
//     JobRepository has no method that accepts SUCCEEDED.
type FinalizeVerifiedCommand struct {
	UploadID         string
	ArtifactID       string
	JobID            string
	WorkerID         string
	LeaseID          string
	AttemptNumber    int
	ExpectedRevision int

	// Master-computed values from Receive().
	StorageProvider string
	StorageKey      string
	SHA256          string
	SizeBytes       int64
	MIMEType        string

	VerifiedAt time.Time

	// DestinationID is the delivery destination to create a job_deliveries row for.
	// If empty, the first enabled destination for this job is used.
	DestinationID string
}

// DeliveryPlanResolver returns the destination IDs that should receive
// the just-verified artifact. Implementations decide the per-job
// destination set (GLOBAL + per-job plans); the spec separates this
// from the transactional insert in FinalizationRepository.FinalizeVerified
// so the planning logic stays outside the writer lock.
type DeliveryPlanResolver interface {
	ResolveDestinations(ctx context.Context, jobID, artifactID string) ([]string, error)
}

// FinalizationRepository is the strict, narrow persistence contract for
// the two operations that require multi-table atomicity across the
// artifact upload / verification / completion pipeline:
//
//   - CreateArtifactAndUploadSession: insert `artifacts` + `artifact_uploads`
//     rows in one transaction (PR 3.5-b 4.2).
//   - FinalizeVerified: single transaction that flips jobs RUNNING →
//     SUCCEEDED, artifacts STAGING → READY, job_attempts → SUCCEEDED,
//     per-destination job_deliveries rows, and the
//     artifact_uploads FINALIZING → COMPLETED flip.
//     Legacy outbox events (ARTIFACT_READY, JOB_SUCCEEDED,
//     DELIVERY_CREATED) were removed in PR #2 cleanup/outbox-legacy-drain.
//
// Callers MUST NOT call any JobRepository method that touches
// jobs.status = 'SUCCEEDED'. LifecycleService does not expose this.
// Handlers (HTTP/gRPC/worker upload) do not hold a reference to this
// interface; only bootstrap composition wires it.
type FinalizationRepository interface {
	CreateArtifactAndUploadSession(ctx context.Context, cmd CreateArtifactAndUploadSessionCommand) error

	FinalizeVerified(ctx context.Context, cmd FinalizeVerifiedCommand) (*store.Artifact, error)
}
