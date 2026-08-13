package artifacts

// ── Artifact statuses (artifacts table) ──────────────────────────────────────
//
// Upload-status enum (UploadStatus + UploadCreated etc.) lives on
// store.UploadStatus in internal/store. Callers in this package
// reference store.UploadCreated etc. directly.

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
