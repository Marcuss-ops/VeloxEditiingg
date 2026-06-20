package artifacts

// ── Artifact statuses (artifacts table) ──────────────────────────────────────

// ArtifactStatus is the typed status for artifact rows.
type ArtifactStatus string

const (
	ArtifactStaging     ArtifactStatus = "STAGING"
	ArtifactReady       ArtifactStatus = "READY"
	ArtifactQuarantined ArtifactStatus = "QUARANTINED"
	ArtifactDeleted     ArtifactStatus = "DELETED"
	ArtifactFailed      ArtifactStatus = "FAILED"
)

// ── Artifact upload statuses (artifact_uploads table) ───────────────────────

// UploadStatus is the typed status for artifact_uploads rows.
type UploadStatus string

const (
	UploadCreated    UploadStatus = "CREATED"
	UploadUploading  UploadStatus = "UPLOADING"
	UploadReceived   UploadStatus = "RECEIVED"
	UploadVerifying  UploadStatus = "VERIFYING"
	UploadFinalizing UploadStatus = "FINALIZING"
	UploadCompleted  UploadStatus = "COMPLETED"
	UploadFailed     UploadStatus = "FAILED"
	UploadExpired    UploadStatus = "EXPIRED"
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
