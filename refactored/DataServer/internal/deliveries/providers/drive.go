// Package deliveries/providers: Drive adapter skeleton.
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
	service *integrationsDrive.Service
	tokens  string // tokens dir, propagated for future config-only wiring
}

// NewDriveProvider constructs a DriveProvider. nil service is allowed for
// tests; Deliver then returns ErrProviderNotConfigured.
func NewDriveProvider(svc *integrationsDrive.Service) *DriveProvider {
	return &DriveProvider{service: svc}
}

// Name returns "drive".
func (d *DriveProvider) Name() string { return "drive" }

// Deliver pushes an artifact file to Drive.
//
// Idempotency: relies on the upload-idempotency-key stamped on
// (artifact_id, destination_id) by the runner. Drive treats duplicate
// uploads as idempotent when the source SHA-256 matches the previously
// uploaded blob.
func (d *DriveProvider) Deliver(ctx context.Context, artifact *store.Artifact, destination *deliveries.Destination) (*deliveries.Result, error) {
	if d == nil || d.service == nil {
		return nil, deliveries.ErrProviderNotConfigured
	}
	if artifact == nil || destination == nil {
		return nil, deliveries.ErrProviderPermanent
	}
	// Real implementation hooks Drive's UploadVideo here. Once we wire
	// the runner end-to-end we'll thread the destination.FolderID through
	// to driveapi.UploadConfig and capture the resulting FileID +
	// WebViewLink on deliveries.Result.
	uploadRes, err := d.service.UploadVideo(ctx, artifact.LocalPath, artifact.ID, destination.FolderID)
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
