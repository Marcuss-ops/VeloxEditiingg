// Package status provides canonical string constants for all state-machine
// statuses used across DataServer. Every package that reads or writes these
// values MUST use the constants defined here. String literals like "RUNNING"
// or "STAGING" in business logic are prohibited — use status.JobRunning etc.
//
// Job statuses are defined in store/jobs_writer_types.go (JobStatus type).
// This file defines artifact, upload, and attempt statuses.
package status

// ── Artifact statuses (artifacts table) ──────────────────────────────────────

type ArtifactStatus string

const (
	ArtifactStaging     ArtifactStatus = "STAGING"
	ArtifactReady       ArtifactStatus = "READY"
	ArtifactQuarantined ArtifactStatus = "QUARANTINED"
	ArtifactDeleted     ArtifactStatus = "DELETED"
	ArtifactFailed      ArtifactStatus = "FAILED"
)

// ── Artifact upload statuses (artifact_uploads table) ───────────────────────

type UploadStatus string

const (
	UploadCreated   UploadStatus = "CREATED"
	UploadUploading UploadStatus = "UPLOADING"
	UploadReceived  UploadStatus = "RECEIVED"
	UploadVerifying UploadStatus = "VERIFYING"
	UploadFinalizing UploadStatus = "FINALIZING"
	UploadCompleted UploadStatus = "COMPLETED"
	UploadFailed    UploadStatus = "FAILED"
	UploadExpired   UploadStatus = "EXPIRED"
)

// ── Job attempt statuses (job_attempts table) ────────────────────────────────

type AttemptStatus string

const (
	AttemptCreating       AttemptStatus = "CREATING"
	AttemptRunning        AttemptStatus = "RUNNING"
	AttemptProcessing     AttemptStatus = "PROCESSING"
	AttemptRenderFinished AttemptStatus = "RENDER_FINISHED"
	AttemptSucceeded      AttemptStatus = "SUCCEEDED"
	AttemptFailed         AttemptStatus = "FAILED"
	AttemptCancelled      AttemptStatus = "CANCELLED"
)

// ── Asset statuses (assets table) ────────────────────────────────────────────
// Defined canonically in internal/assets/asset.go; do NOT duplicate here.
// Use assets.AssetStatusStaging, assets.AssetStatusReady, etc.

// ── Delivery statuses (job_deliveries table) ─────────────────────────────────

type DeliveryStatus string

const (
	DeliveryPending   DeliveryStatus = "PENDING"
	DeliverySucceeded DeliveryStatus = "SUCCEEDED"
	DeliveryFailed    DeliveryStatus = "FAILED"
)
