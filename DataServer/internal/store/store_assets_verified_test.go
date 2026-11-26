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

func TestSourceCertificateLookupPrefersNanosecondPrecisionUpgrade(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/source-certificate-upgrade.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	repo := NewSQLiteAssetRepository(s)
	ctx := context.Background()
	asset := AssetRecord{AssetID: "certified-source", Kind: "video_segment", Status: "READY", SHA256: strings.Repeat("a", 64), MimeType: "video/mp4", SizeBytes: 100, StorageProvider: "local", StorageKey: "fragment.mp4"}
	if err := repo.Insert(ctx, asset); err != nil {
		t.Fatal(err)
	}
	reference := "certified-video-source:" + asset.SHA256 + ":video-window-preparer.v2"
	for _, source := range []AssetSourceRecord{
		{SourceID: "z-old-source-proof", AssetID: asset.AssetID, SourceType: "video.source.certificate.v1", SourceReference: reference, MetadataJSON: `{"chunks":null}`, CreatedAt: "2026-11-26T10:00:00Z"},
		{SourceID: "a-upgraded-source-proof", AssetID: asset.AssetID, SourceType: "video.source.certificate.v1", SourceReference: reference, MetadataJSON: `{"chunks":{"chunk_size":1048576}}`, CreatedAt: "2026-11-26T10:00:00.000000001Z"},
	} {
		if err := repo.InsertSource(ctx, source); err != nil {
			t.Fatal(err)
		}
	}
	got, err := repo.GetBySourceReference(ctx, reference)
	if err != nil || got == nil || got.MetadataJSON != `{"chunks":{"chunk_size":1048576}}` {
		t.Fatalf("single lookup returned %#v err=%v; want nanosecond proof upgrade", got, err)
	}
	batch, err := repo.GetBySourceReferences(ctx, []string{reference})
	if err != nil || batch[reference] == nil || batch[reference].MetadataJSON != got.MetadataJSON {
		t.Fatalf("batch lookup = %#v err=%v; single=%#v", batch[reference], err, got)
	}
}

func TestVideoSourceCertificateRegistryNeedsNoAssetBlob(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/source-certificate-registry.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	repo := NewSQLiteAssetRepository(s)
	ctx := context.Background()
	reference := "certified-video-source:" + strings.Repeat("a", 64) + ":profile-r1"
	first := `{"sha256":"` + strings.Repeat("a", 64) + `","chunks":null}`
	if err := repo.PutVideoSourceCertificate(ctx, reference, strings.Repeat("a", 64), 1234, "profile-r1", first, "2026-11-26T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	got, err := repo.GetVideoSourceCertificates(ctx, []string{reference})
	if err != nil || got[reference] != first {
		t.Fatalf("independent source certificate lookup = %#v, err=%v", got, err)
	}
	upgraded := `{"sha256":"` + strings.Repeat("a", 64) + `","chunks":{"chunk_size":1048576}}`
	if replaced, err := repo.ReplaceVideoSourceCertificate(ctx, reference, first, upgraded); err != nil || !replaced {
		t.Fatalf("compare-and-swap evidence upgrade = %v, err=%v", replaced, err)
	}
	if replaced, err := repo.ReplaceVideoSourceCertificate(ctx, reference, first, `{"conflict":true}`); err != nil || replaced {
		t.Fatalf("stale evidence writer replaced current row: replaced=%v err=%v", replaced, err)
	}
	got, err = repo.GetVideoSourceCertificates(ctx, []string{reference})
	if err != nil || got[reference] != upgraded {
		t.Fatalf("certificate after stale writer = %#v, err=%v", got, err)
	}
	var assetCount int
	if err := s.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM assets`).Scan(&assetCount); err != nil || assetCount != 0 {
		t.Fatalf("certificate registration created asset rows: count=%d err=%v", assetCount, err)
	}
}
