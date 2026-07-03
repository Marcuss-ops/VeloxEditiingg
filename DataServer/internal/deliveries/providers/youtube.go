// Package deliveries/providers: YouTube adapter.
//
// YouTubeProvider wraps internal/integrations/youtube.Service through the
// deliveries.Provider interface.
package providers

import (
	"context"
	"log"

	"velox-server/internal/deliveries"
	integrationsYouTube "velox-server/internal/integrations/youtube"
	"velox-server/internal/store"
)

// YouTubeProvider is the production YouTube adapter.
type YouTubeProvider struct {
	service   *integrationsYouTube.Service
	blobStore store.BlobStore
}

// NewYouTubeProvider constructs a YouTubeProvider. nil service is allowed
// for tests; Deliver then returns ErrProviderNotConfigured.
func NewYouTubeProvider(svc *integrationsYouTube.Service, blobStore store.BlobStore) *YouTubeProvider {
	return &YouTubeProvider{service: svc, blobStore: blobStore}
}

// Name returns "youtube".
func (y *YouTubeProvider) Name() string { return "youtube" }

// Deliver pushes an artifact to YouTube via the channel resolved from
// destination.ChannelID + language.
//
// Idempotency: YouTube accepts re-uploads of the same bytes when the
// caller supplies a matching content-length + mime; the runner is
// responsible for the idempotency_key stamping on job_deliveries so
// subsequent claims produce the same YouTube video id.
//
// Uses blobStore to read the artifact's bytes. Falls back to artifact.LocalPath
// if the blob store is not configured (legacy path).
func (y *YouTubeProvider) Deliver(ctx context.Context, artifact *store.Artifact, destination *deliveries.Destination, deliveryID, idempotencyKey string) (*deliveries.Result, error) {
	if y == nil || y.service == nil {
		return nil, deliveries.ErrProviderNotConfigured
	}
	if artifact == nil || destination == nil {
		return nil, deliveries.ErrProviderPermanent
	}

	// Resolve the file path: prefer storage_key (canonical) over LocalPath.
	filePath := artifact.StorageKey
	if filePath == "" {
		filePath = artifact.LocalPath
	}
	if filePath == "" {
		return nil, deliveries.ErrProviderPermanent
	}

	// If blobStore is available, verify the file exists at storage_key.
	if y.blobStore != nil {
		f, err := y.blobStore.ReadFinal(filePath)
		if err != nil {
			log.Printf("[YOUTUBE] Cannot read artifact %s at %s, falling back to LocalPath %s: %v",
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

	cfg := integrationsYouTube.UploadConfig{
		Title:            artifact.ID,
		Description:      "",
		PrivacyStatus:    "private",
		IdempotencyToken: deliveryID,
	}

	uploadRes, err := y.service.UploadVideo(ctx, destination.ChannelID, filePath, cfg)
	if err != nil {
		return nil, err
	}
	return &deliveries.Result{
		Success:   uploadRes.Status == "uploaded",
		RemoteID:  uploadRes.VideoID,
		RemoteURL: uploadRes.YouTubeURL,
	}, nil
}
