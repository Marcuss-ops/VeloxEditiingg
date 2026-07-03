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

// DeliveryPlanResolver returns the destination IDs that should receive
// the just-verified artifact. The finalize writer consumes the resolved
// set inside the same *sql.Tx that INSERTs job_deliveries.
//
// Implementations decide the per-job destination set (GLOBAL +
// per-job plans); the resolver stays outside the writer lock so the
// planning logic is independently testable.
type DeliveryPlanResolver interface {
	ResolveDestinations(ctx context.Context, jobID, artifactID string) ([]string, error)
}
