package deliveries

import "context"

// DriveStreamRelay overlaps master-stream artifact transfer with a Drive
// resumable upload. Relay errors are optimization failures: callers keep the
// durable master artifact path alive and let the normal DeliveryRunner upload
// it after verification.
type DriveStreamRelay interface {
	PrepareArtifact(ctx context.Context, uploadID, artifactID, jobID string) error
	RelayAvailableChunks(ctx context.Context, uploadID string) error
	CompleteRelay(ctx context.Context, uploadID string) error
	VerifyRelay(ctx context.Context, uploadID, sha256 string, sizeBytes int64) error
	AbortRelay(ctx context.Context, uploadID string) error
}

type DriveRelayEvidence struct {
	RemoteID      string
	RemoteURL     string
	FolderID      string
	SHA256        string
	SizeBytes     int64
	ArtifactID    string
	DestinationID string
	PublicationID string
}

type DriveRelayEvidenceReader interface {
	GetVerifiedDriveRelay(ctx context.Context, artifactID, destinationID, publicationID string) (*DriveRelayEvidence, error)
}
