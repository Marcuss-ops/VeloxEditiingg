// Package deliveries/providers: YouTube adapter.
//
// YouTubeProvider wraps internal/integrations/youtube.Service through the
// deliveries.Provider interface.
package providers

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

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

	cfg, err := youtubeUploadConfig(artifact.ID, destination, deliveryID)
	if err != nil {
		return nil, err
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

func youtubeUploadConfig(defaultTitle string, destination *deliveries.Destination, deliveryID string) (integrationsYouTube.UploadConfig, error) {
	config := integrationsYouTube.UploadConfig{
		Title:            defaultTitle,
		PrivacyStatus:    "private",
		ChannelID:        destination.ChannelID,
		IdempotencyToken: deliveryID,
	}
	if strings.TrimSpace(destination.DeliveryMetadataJSON) == "" || destination.DeliveryMetadataJSON == "{}" {
		return config, nil
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal([]byte(destination.DeliveryMetadataJSON), &envelope); err != nil {
		return config, fmt.Errorf("%w: invalid delivery metadata: %v", deliveries.ErrProviderPermanent, err)
	}
	raw := json.RawMessage(destination.DeliveryMetadataJSON)
	if nested, ok := envelope["video_metadata"]; ok {
		raw = nested
	}
	var metadata struct {
		Title         string   `json:"title"`
		Description   string   `json:"description"`
		Tags          []string `json:"tags"`
		CategoryID    string   `json:"category_id"`
		PrivacyStatus string   `json:"privacy_status"`
		PublishAt     string   `json:"publish_at"`
		ThumbnailPath string   `json:"thumbnail_path"`
	}
	if err := json.Unmarshal(raw, &metadata); err != nil {
		return config, fmt.Errorf("%w: invalid video metadata: %v", deliveries.ErrProviderPermanent, err)
	}
	if strings.TrimSpace(metadata.Title) != "" {
		config.Title = metadata.Title
	}
	config.Description = metadata.Description
	config.Tags = metadata.Tags
	config.CategoryID = metadata.CategoryID
	if strings.TrimSpace(metadata.PrivacyStatus) != "" {
		config.PrivacyStatus = strings.ToLower(strings.TrimSpace(metadata.PrivacyStatus))
	}
	config.PublishAt = strings.TrimSpace(metadata.PublishAt)
	config.ThumbnailPath = strings.TrimSpace(metadata.ThumbnailPath)
	// YouTube only accepts publishAt for a private video. Enforce the
	// platform rule at the adapter boundary so a scheduled delivery cannot
	// accidentally become public immediately.
	if config.PublishAt != "" {
		config.PrivacyStatus = "private"
	}
	return config, nil
}
