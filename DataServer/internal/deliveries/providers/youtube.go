// Package deliveries/providers: YouTube adapter.
//
// YouTubeProvider wraps internal/integrations/youtube.Service through the
// deliveries.Provider interface.
package providers

import (
	"context"

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
func (y *YouTubeProvider) Deliver(ctx context.Context, artifact *store.Artifact, destination *deliveries.Destination, deliveryID, idempotencyKey string) (*deliveries.Result, error) {
	if y == nil || y.service == nil {
		return nil, deliveries.ErrProviderNotConfigured
	}
	if destination == nil {
		return nil, deliveries.ErrProviderPermanent
	}

	filePath, err := resolveArtifactFilePath(y.blobStore, artifact)
	if err != nil {
		return nil, err
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
