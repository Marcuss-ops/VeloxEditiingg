package artifacts

// ── Artifact statuses (artifacts table) ──────────────────────────────────────
//
// Upload-status enum (UploadStatus + UploadCreated etc.) lives on
// store.UploadStatus in internal/store. Callers in this package
// reference store.UploadCreated etc. directly. The ArtifactStatus +
// AttemptStatus blocks below stay because their typed values are
// consumed via taskattempts.AttemptStatusXxx + storage.go string
// comparisons, and neither table is owned by a typed repository yet.

// ArtifactStatus is the typed status for artifact rows.
type ArtifactStatus string

const (
	ArtifactStaging     ArtifactStatus = "STAGING"
	ArtifactReady       ArtifactStatus = "READY"
	ArtifactQuarantined ArtifactStatus = "QUARANTINED"
	ArtifactDeleted     ArtifactStatus = "DELETED"
	ArtifactFailed      ArtifactStatus = "FAILED"
)

// ── Job attempt statuses (job_attempts table) ──────────────────────────────

// AttemptStatus is the typed status for job_attempts rows.
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
