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

func TestAssetSourceLookupUsesWindowSpecificCertificate(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/source-evidence.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	repo := NewSQLiteAssetRepository(s)
	ctx := context.Background()
	asset := AssetRecord{AssetID: "same-bytes", Kind: "video_segment", Status: "READY", SHA256: strings.Repeat("a", 64), MimeType: "video/mp4", SizeBytes: 100, StorageProvider: "local", StorageKey: "fragment.mp4", MetadataJSON: `{"producer":"original"}`}
	if err := repo.Insert(ctx, asset); err != nil {
		t.Fatal(err)
	}
	refs := []string{"prepared-window:first", "prepared-window:second", "external-source", "certified-video-source:source"}
	for i, ref := range refs {
		metadata := []string{`{"window_certificate":"first"}`, `{"window_certificate":"second"}`, `{"external":true}`, `{"source_certificate":"source"}`}[i]
		if err := repo.InsertSource(ctx, AssetSourceRecord{SourceID: ref, AssetID: asset.AssetID, SourceType: "worker_prepared", SourceReference: ref, MetadataJSON: metadata}); err != nil {
			t.Fatal(err)
		}
	}
	batch, err := repo.GetBySourceReferences(ctx, refs)
	if err != nil {
		t.Fatal(err)
	}
	for i, ref := range refs {
		want := []string{`{"window_certificate":"first"}`, `{"window_certificate":"second"}`, asset.MetadataJSON, `{"source_certificate":"source"}`}[i]
		single, err := repo.GetBySourceReference(ctx, ref)
		if err != nil || single == nil {
			t.Fatalf("single %s: %+v %v", ref, single, err)
		}
		if single.MetadataJSON != want || batch[ref] == nil || batch[ref].MetadataJSON != want {
			t.Fatalf("reference %s has wrong evidence: single=%+v batch=%+v", ref, single, batch[ref])
		}
	}
}
