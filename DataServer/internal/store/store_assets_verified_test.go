package store

import (
	"context"
	"strings"
	"testing"
)

func TestSQLiteAssetRepository_InsertWithMediaMetadataIsAtomic(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/verified-assets.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	repo := NewSQLiteAssetRepository(s)
	ctx := context.Background()
	metadata := MediaMetadataRecord{
		AssetID:               "audio-ok",
		Container:             "mov",
		DurationMs:            1000,
		AudioCodec:            "aac",
		AudioSampleRate:       48000,
		AudioChannels:         2,
		MetadataVerifiedAt:    "2026-08-12T00:00:00Z",
		MetadataSchemaVersion: 1,
	}
	asset := AssetRecord{
		AssetID:   "audio-ok",
		Kind:      "final_audio",
		Status:    "READY",
		SHA256:    strings.Repeat("a", 64),
		MimeType:  "audio/mp4",
		SizeBytes: 123, StorageProvider: "local",
		StorageKey: "/tmp/audio-ok.m4a",
		VerifiedAt: "2026-08-12T00:00:00Z",
	}
	if err := repo.InsertWithMediaMetadata(ctx, asset, metadata); err != nil {
		t.Fatalf("successful InsertWithMediaMetadata: %v", err)
	}
	if got, err := repo.GetByID(ctx, asset.AssetID); err != nil || got == nil || got.Status != "READY" || got.VerifiedAt == "" {
		t.Fatalf("asset after successful insert = %#v, err=%v", got, err)
	}
	if got, err := repo.GetMediaMetadata(ctx, asset.AssetID); err != nil || got == nil || !got.Verified() {
		t.Fatalf("metadata after successful insert = %#v, err=%v", got, err)
	}

	if _, err := s.DB().ExecContext(ctx, `
CREATE TRIGGER fail_verified_metadata
BEFORE INSERT ON asset_media_metadata
BEGIN
  SELECT RAISE(ABORT, 'forced metadata failure');
END;`); err != nil {
		t.Fatalf("create metadata trigger: %v", err)
	}
	failedAsset := asset
	failedAsset.AssetID = "audio-rollback"
	failedAsset.SHA256 = strings.Repeat("b", 64)
	failedMetadata := metadata
	failedMetadata.AssetID = failedAsset.AssetID
	if err := repo.InsertWithMediaMetadata(ctx, failedAsset, failedMetadata); err == nil {
		t.Fatal("InsertWithMediaMetadata(triggered) = nil, want rollback error")
	}
	if got, err := repo.GetByID(ctx, failedAsset.AssetID); err != nil || got != nil {
		t.Fatalf("asset after metadata failure = %#v, err=%v; asset insert must roll back", got, err)
	}
	if got, err := repo.GetMediaMetadata(ctx, failedAsset.AssetID); err != nil || got != nil {
		t.Fatalf("metadata after metadata failure = %#v, err=%v; metadata insert must roll back", got, err)
	}
}
