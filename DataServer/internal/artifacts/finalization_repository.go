// Package artifacts / finalization_repository.go
//
// Persistence boundary interfaces + the input Command structs. No
// generic repository type: each interface is a single-method, single-tx
// surface scoped to one step of the verified-finalization pipeline.
//
// The order matters for the reader: read this file before
// sqlite_upload_session_writer.go / sqlite_finalize_writer.go /
// sqlite_artifact_reader.go.
package artifacts

import (
	"context"
	"time"
)

// CreateArtifactAndUploadSessionCommand is the input to
// UploadSessionWriter.CreateArtifactAndUploadSession.
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
	// empty so callers that don't care can omit it.
	StorageProvider string

	// Worker-declared hints (diagnostic only; never trusted by master).
	ExpectedMIME        string
	ExpectedSizeBytes   int64
	ExpectedSHA256      string
	TemporaryStorageKey string

	// CreatedAt / ExpiresAt: zero values are filled in by the writer
	// (CreatedAt=now, ExpiresAt=now+defaultUploadTTL=24h) so callers
	// can pass zero-time without poisoning the schema.
	CreatedAt time.Time
	ExpiresAt time.Time
}

// FinalizeVerifiedCommand is the input to FinalizationWriter.FinalizeVerified.
//
// No other code path may flip jobs.status='SUCCEEDED'; enforced by
// scan_test.go and the narrow FinalizationWriter interface.
type FinalizeVerifiedCommand struct {
	UploadID         string
	ArtifactID       string
	JobID            string
	AttemptID        string
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

	// DestinationID is the delivery destination to create a job_deliveries
	// row for. If empty, the delivery-plan resolver (or fallback
	// all-enabled destinations SELECT inside the tx) chooses.
	DestinationID string
}

// DeliveryDestination is the per-destination projection the finalize
// writer consumes. Resolvers return one of these per (job_id,
// artifact_id) pair; the writer reads MaxAttempts to stamp durable
// attempt caps onto job_deliveries at INSERT time.
//
// Step 5/8 of the canonical-purity plan: the rich per-destination
// retry_budget lives on job_delivery_plans.retry_budget (migration 069)
// and is surfaced here so durable max_attempts survives across worker
// restarts and runner crashes.
//
// MaxAttempts == 0 is allowed in the projection but the writer
// applies schema DEFAULT 5 at INSERT time so legacy plans inserted
// before migration 069 (no retry_budget column) continue to behave
// identically to the historical all-enable-destinations SELECT path.
type DeliveryDestination struct {
	DestinationID string
	MaxAttempts   int
}

// DeliveryPlanResolver returns the per-destination set the finalize
// writer should insert into job_deliveries. The writer consumes the
// resolved set inside the same *sql.Tx that INSERTs job_deliveries.
//
// Implementations decide the per-job destination set (per-job plans +
// optional global fallback); the resolver stays outside the writer
// lock so the planning logic is independently testable.
//
// Step 5/8: the interface returns []DeliveryDestination (with
// MaxAttempts) rather than []string. Older callers that only need the
// destination IDs can ignore MaxAttempts; the writer always reads it.
type DeliveryPlanResolver interface {
	ResolveDestinations(ctx context.Context, jobID, artifactID string) ([]DeliveryDestination, error)
}
