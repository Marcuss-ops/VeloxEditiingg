// Package deliveries/providers: Drive adapter.
//
// DriveProvider wraps internal/integrations/drive.Service through the
// deliveries.Provider interface so the runner can call it without
// importing Drive-specific packages.
package providers

import (
	"context"
	"log"

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
//
// Uses blobStore to read the artifact's bytes. Falls back to artifact.LocalPath
// if the blob store is not configured (legacy path).
func (d *DriveProvider) Deliver(ctx context.Context, artifact *store.Artifact, destination *deliveries.Destination, deliveryID, idempotencyKey string) (*deliveries.Result, error) {
	if d == nil || d.service == nil {
		return nil, deliveries.ErrProviderNotConfigured
	}
	if artifact == nil || destination == nil {
		return nil, deliveries.ErrProviderPermanent
	}

	// Resolve the file path: prefer storage_key (canonical path) over LocalPath.
	filePath := artifact.StorageKey
	if filePath == "" {
		filePath = artifact.LocalPath
	}
	if filePath == "" {
		return nil, deliveries.ErrProviderPermanent
	}

	// If blobStore is available, verify the file exists at storage_key.
	if d.blobStore != nil {
		f, err := d.blobStore.ReadFinal(filePath)
		if err != nil {
			log.Printf("[DRIVE] Cannot read artifact %s at %s, falling back to LocalPath %s: %v",
				artifact.ID, filePath, artifact.LocalPath, err)
			if artifact.LocalPath != "" {
				filePath = artifact.LocalPath
			} else {
				return nil, deliveries.ErrProviderPermanent
			}
		} else {
			f.Close()
		}
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
