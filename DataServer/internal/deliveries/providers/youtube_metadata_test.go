package providers

import (
	"errors"
	"testing"

	"velox-server/internal/deliveries"
)

func TestYouTubeUploadConfigUsesPersistedMetadata(t *testing.T) {
	cfg, err := youtubeUploadConfig("artifact-1", &deliveries.Destination{
		ChannelID:            "channel-1",
		DeliveryMetadataJSON: `{"video_metadata":{"title":"Scheduled title","description":"Description","tags":["one","two"],"privacy_status":"public","publish_at":"2026-07-20T18:00:00+02:00","category_id":"22"}}`,
	}, "delivery-1")
	if err != nil {
		t.Fatalf("youtubeUploadConfig: %v", err)
	}
	if cfg.Title != "Scheduled title" || cfg.Description != "Description" {
		t.Fatalf("metadata not applied: %#v", cfg)
	}
	if cfg.ChannelID != "channel-1" || cfg.IdempotencyToken != "delivery-1" {
		t.Fatalf("destination identity not applied: %#v", cfg)
	}
	if cfg.PrivacyStatus != "private" || cfg.PublishAt == "" {
		t.Fatalf("scheduled upload must be private with publish_at: %#v", cfg)
	}
}

func TestYouTubeUploadConfigRejectsInvalidJSON(t *testing.T) {
	_, err := youtubeUploadConfig("artifact-1", &deliveries.Destination{
		DeliveryMetadataJSON: `{invalid`,
	}, "delivery-1")
	if err == nil {
		t.Fatal("expected invalid metadata error")
	}
	if !errors.Is(err, deliveries.ErrProviderPermanent) {
		t.Fatalf("expected permanent provider error, got %v", err)
	}
}
