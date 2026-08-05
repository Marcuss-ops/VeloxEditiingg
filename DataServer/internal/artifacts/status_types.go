package artifacts

// ── Artifact statuses (artifacts table) ──────────────────────────────────────
//
// Upload-status enum (UploadStatus + UploadCreated etc.) lives on
// store.UploadStatus in internal/store. Callers in this package
// reference store.UploadCreated etc. directly. The ArtifactStatus +
// AttemptStatus blocks below stay because their typed values are
// consumed via taskattempts.AttemptStatusXxx + storage.go string
// comparisons, and neither table is owned by a typed repository yet.

// ArtifactState is the canonical lifecycle state for a produced artifact.
// Artifact readiness is independent from every delivery attempt: a READY
// artifact remains READY when a destination is pending or fails.
type ArtifactState string

const (
	ArtifactStaging     ArtifactState = "STAGING"
	ArtifactVerifying   ArtifactState = "VERIFYING"
	ArtifactReady       ArtifactState = "READY"
	ArtifactQuarantined ArtifactState = "QUARANTINED"
	ArtifactDeleted     ArtifactState = "DELETED"
	ArtifactFailed      ArtifactState = "FAILED"
)

// ArtifactStatus is retained as a source-compatible alias. New code should
// use ArtifactState to make the artifact/delivery boundary explicit.
type ArtifactStatus = ArtifactState

func (s ArtifactState) IsTerminal() bool {
	return s == ArtifactReady || s == ArtifactQuarantined || s == ArtifactDeleted || s == ArtifactFailed
}

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
