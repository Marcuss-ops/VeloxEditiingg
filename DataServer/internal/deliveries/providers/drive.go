// Package deliveries/providers: Drive adapter.
//
// DriveProvider wraps internal/integrations/drive.Service through the
// deliveries.Provider interface so the runner can call it without
// importing Drive-specific packages.
package providers

import (
	"context"

	"velox-server/internal/deliveries"
	integrationsDrive "velox-server/internal/integrations/drive"
	"velox-server/internal/store"
)

// DriveProvider is the production Drive adapter.
type DriveProvider struct {
	service   *integrationsDrive.Service
	blobStore store.BlobStore
}

// NewDriveProvider constructs a DriveProvider. nil service is allowed for
// tests; Deliver then returns ErrProviderNotConfigured.
func NewDriveProvider(svc *integrationsDrive.Service, blobStore store.BlobStore) *DriveProvider {
	return &DriveProvider{service: svc, blobStore: blobStore}
}

// Name returns "drive".
func (d *DriveProvider) Name() string { return "drive" }

// Deliver pushes an artifact file to Drive.
//
// Idempotency: relies on the upload-idempotency-key stamped on
// (artifact_id, destination_id) by the runner. Drive treats duplicate
// uploads as idempotent when the source SHA-256 matches the previously
// uploaded blob.
func (d *DriveProvider) Deliver(ctx context.Context, artifact *store.Artifact, destination *deliveries.Destination, deliveryID, idempotencyKey string) (*deliveries.Result, error) {
	if d == nil || d.service == nil {
		return nil, deliveries.ErrProviderNotConfigured
	}
	if destination == nil {
		return nil, deliveries.ErrProviderPermanent
	}

	filePath, err := resolveArtifactFilePath(d.blobStore, artifact)
	if err != nil {
		return nil, err
	}

	uploadRes, err := d.service.UploadVideo(ctx, filePath, artifact.ID, destination.FolderID, deliveryID)
	if err != nil {
		return nil, err
	}
	return &deliveries.Result{
		Success:   uploadRes.Success,
		RemoteID:  uploadRes.FileID,
		RemoteURL: uploadRes.WebViewLink,
		ProviderMeta: map[string]interface{}{
			"folder_link": uploadRes.FolderLink,
		},
	}, nil
}
