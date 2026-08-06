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

	"velox-server/internal/deliverycontract"
	"velox-server/internal/store"
)

// CreateArtifactAndUploadSessionCommand is retained as the application-side
// command used by BeginUpload. The store adapter receives the cycle-free
// store.CreateUploadSessionParams projection.
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

	// ExpectedAudioStreams is computed from the same explicit delivery plan
	// before the async finalization transaction and persisted with the probe.
	ExpectedAudioStreams int

	// DestinationID is an explicit delivery destination override. Empty
	// means the explicit per-job delivery-plan resolver must provide the
	// targets; it never selects a global destination.
	DestinationID string

	// AsyncProbe keeps the artifact in VERIFYING and defers job delivery
	// materialization to the dedicated media probe worker.
	AsyncProbe bool
}

// DeliveryDestination is the per-destination projection the finalize
// writer consumes. Resolvers return one of these per (job_id,
// artifact_id) pair; the writer reads MaxAttempts to stamp durable
// attempt caps onto job_deliveries at materialization time.
//
// Step 5/8 of the canonical-purity plan: the rich per-destination
// retry_budget lives on job_delivery_plans.retry_budget (migration 069)
// and is surfaced here so durable max_attempts survives across worker
// restarts and runner crashes.
//
// MaxAttempts == 0 is allowed in the projection but the writer
// applies schema DEFAULT 5 when materializing legacy plan rows that
// omit retry_budget.
type DeliveryDestination = deliverycontract.DeliveryDestination

// DeliveryPlanResolver returns the per-destination set the finalize
// writer should insert into job_deliveries. The store adapter consumes the
// resolved set inside the same finalization transaction.
//
// Implementations decide the per-job explicit destination set; the
// resolver stays outside the writer lock so the planning logic is
// independently testable.
//
// Step 5/8: the interface returns []DeliveryDestination (with
// MaxAttempts) rather than []string. Older callers that only need the
// destination IDs can ignore MaxAttempts; the writer always reads it.
type DeliveryPlanResolver = deliverycontract.DeliveryPlanResolver

// UploadSessionWriter is the narrow application port for BeginUpload.
// SQL implementations live in internal/store; artifacts only supplies the
// persistence projection and never owns a database handle.
type UploadSessionWriter interface {
	CreateArtifactAndUploadSession(ctx context.Context, params store.CreateUploadSessionParams) error
}

// FinalizationWriter is the narrow application port for verified finalization.
// The store adapter owns the transaction and all SQL statements.
type FinalizationWriter interface {
	FinalizeVerified(ctx context.Context, cmd FinalizeVerifiedCommand) (*store.Artifact, error)
}
