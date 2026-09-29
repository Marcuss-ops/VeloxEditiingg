// Package deliveries/providers: Drive adapter.
//
// DriveProvider wraps internal/integrations/drive.Service through the
// deliveries.Provider interface so the runner can call it without
// importing Drive-specific packages.
package providers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"

	"velox-server/internal/deliveries"
	integrationsDrive "velox-server/internal/integrations/drive"
	"velox-server/internal/repository"
)

// DriveProvider is the production Drive adapter.
type DriveProvider struct {
	service   *integrationsDrive.Service
	blobStore repository.BlobStore
	relay     deliveries.DriveRelayEvidenceReader
}

func (d *DriveProvider) WithRelayEvidenceReader(reader deliveries.DriveRelayEvidenceReader) *DriveProvider {
	d.relay = reader
	return d
}

// NewDriveProvider constructs a DriveProvider. nil service is allowed for
// tests; Deliver then returns ErrProviderNotConfigured.
func NewDriveProvider(svc *integrationsDrive.Service, blobStore repository.BlobStore) *DriveProvider {
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
func (d *DriveProvider) Deliver(ctx context.Context, artifact *repository.Artifact, destination *deliveries.Destination, deliveryID, idempotencyKey string) (*deliveries.Result, error) {
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
	if d.relay != nil && artifact != nil && artifact.SHA256 != "" && artifact.SizeBytes > 0 && destination.DestinationID != "" && destination.PublicationID != "" {
		evidence, evidenceErr := d.relay.GetVerifiedDriveRelay(ctx, artifact.ID, destination.DestinationID, destination.PublicationID)
		if evidenceErr != nil {
			log.Printf("[DELIVERY][DRIVE] relay evidence unavailable; using verified-artifact upload artifact=%s destination=%s: %v", artifact.ID, destination.DestinationID, evidenceErr)
		}
		if evidence != nil && evidence.RemoteID != "" && strings.EqualFold(evidence.SHA256, artifact.SHA256) && evidence.SizeBytes == artifact.SizeBytes {
			remote, lookupErr := d.service.FindRelayFile(ctx, evidence.FolderID, artifact.ID, destination.DestinationID, destination.PublicationID)
			if lookupErr != nil {
				log.Printf("[DELIVERY][DRIVE] relay remote reconciliation unavailable; using verified-artifact upload artifact=%s destination=%s: %v", artifact.ID, destination.DestinationID, lookupErr)
			} else if remote != nil && !remote.Trashed && remote.ID == evidence.RemoteID && remote.Size == artifact.SizeBytes {
				return &deliveries.Result{Success: true, RemoteID: remote.ID, RemoteURL: remote.WebViewLink, ProviderMeta: map[string]interface{}{"relay": true}}, nil
			}
		}
	}
	fileName := strings.TrimSpace(artifact.VideoTitle)
	if fileName == "" {
		log.Printf("[DELIVERY][DRIVE] missing submitted video title; using job-based filename job=%s artifact=%s", artifact.JobID, artifact.ID)
		fileName = "Video " + artifact.JobID
	}
	uploadRes, err := d.service.UploadVideoNamed(ctx, filePath, artifact.ID, fileName, driveFolderReference(destination), marker)
	if err != nil {
		return nil, classifyDriveError(err)
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

// classifyDriveError projects Drive authentication failures into the
// deliveries contract. Missing/expired credentials must become BLOCKED_AUTH;
// retrying them with exponential backoff cannot repair the operator state.
func classifyDriveError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, integrationsDrive.ErrNotAuthenticated) {
		return fmt.Errorf("%w: %w", deliveries.ErrProviderAuth, err)
	}
	var apiErr *integrationsDrive.APIError
	if errors.As(err, &apiErr) && (apiErr.StatusCode == 401 || apiErr.StatusCode == 403) {
		return fmt.Errorf("%w: %w", deliveries.ErrProviderAuth, err)
	}
	return err
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
