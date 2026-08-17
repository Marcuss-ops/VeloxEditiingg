// Package deliveries/providers: Drive adapter.
//
// DriveProvider wraps internal/integrations/drive.Service through the
// deliveries.Provider interface so the runner can call it without
// importing Drive-specific packages.
package providers

import (
	"context"
	"encoding/json"
	"strings"

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
// Idempotency: the Drive adapter persists the delivery ID in Drive file
// properties and reuses the matching remote file before uploading. The
// runner's durable lease prevents normal concurrent duplicates; the remote
// property lookup also covers retries after a lease expires.
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

	marker := deliveryID
	if marker == "" {
		marker = idempotencyKey
	}
	uploadRes, err := d.service.UploadVideo(ctx, filePath, artifact.ID, driveFolderReference(destination), marker)
	if err != nil {
		return nil, err
	}
	return &deliveries.Result{
		Success:   uploadRes.Success,
		RemoteID:  uploadRes.FileID,
		RemoteURL: uploadRes.WebViewLink,
		ProviderMeta: map[string]interface{}{
			"folder_link": uploadRes.FolderLink,
			// Network vs local-buffer split: lets the runner's telemetry
			// separate Drive round-trip time from local disk read time.
			"upload_network_ms":      uploadRes.NetworkMS,
			"upload_local_buffer_ms": uploadRes.LocalBufferMS,
		},
	}, nil
}

func driveFolderReference(destination *deliveries.Destination) string {
	if destination == nil {
		return ""
	}
	folder := strings.TrimSpace(destination.FolderID)
	var metadata map[string]interface{}
	if raw := strings.TrimSpace(destination.DeliveryMetadataJSON); raw != "" {
		if json.Unmarshal([]byte(raw), &metadata) == nil {
			if requested, ok := metadata["folder_id"].(string); ok && strings.TrimSpace(requested) != "" {
				folder = strings.TrimSpace(requested)
			}
		}
	}
	if marker := strings.Index(folder, "/folders/"); marker >= 0 {
		folder = folder[marker+len("/folders/"):]
	}
	if query := strings.IndexByte(folder, '?'); query >= 0 {
		folder = folder[:query]
	}
	return strings.Trim(folder, "/")
}
