package workercache

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"velox-shared/assetref"
)

func testNormalizationProfile() NormalizationProfile {
	return NormalizationProfile{
		NormalizerVersion: 1,
		Codec:             "h264",
		Width:             1920,
		Height:            1080,
		FPSNum:            30,
		FPSDen:            1,
		PixelFormat:       "yuv420p",
		ColorProperties:   "bt709/limited",
		Profile:           "high",
		Level:             "4.1",
		GOPPolicy:         "keyint=60:scenecut=0",
		EncoderSettings:   "preset=medium:crf=18",
	}
}

func TestDerivedAssetKeyIsDeterministicAndProfileScoped(t *testing.T) {
	source := assetref.ContentHash(strings.Repeat("a", 64))
	profile := testNormalizationProfile()

	first, err := DerivedAssetKey(source, profile)
	if err != nil {
		t.Fatalf("DerivedAssetKey: %v", err)
	}
	second, err := DerivedAssetKey(source, profile)
	if err != nil {
		t.Fatalf("DerivedAssetKey repeat: %v", err)
	}
	if first != second {
		t.Fatalf("same source/profile produced different keys: %q vs %q", first, second)
	}
	if !strings.HasPrefix(string(first), derivedAssetKeyNamespace) {
		t.Fatalf("key %q missing namespace %q", first, derivedAssetKeyNamespace)
	}

	changed := profile
	changed.Width = 1280
	third, err := DerivedAssetKey(source, changed)
	if err != nil {
		t.Fatalf("DerivedAssetKey changed profile: %v", err)
	}
	if third == first {
		t.Fatal("changed output width reused the original derived key")
	}

	changedVersion := profile
	changedVersion.NormalizerVersion++
	fourth, err := DerivedAssetKey(source, changedVersion)
	if err != nil {
		t.Fatalf("DerivedAssetKey changed version: %v", err)
	}
	if fourth == first {
		t.Fatal("changed normalizer version reused the original derived key")
	}
}

func TestDerivedAssetKeyRejectsIncompleteProfileAndSource(t *testing.T) {
	if _, err := DerivedAssetKey(assetref.ContentHash(strings.Repeat("b", 64)), NormalizationProfile{}); err == nil {
		t.Fatal("incomplete profile was accepted")
	}
	if _, err := DerivedAssetKey(assetref.ContentHash("not-a-sha"), testNormalizationProfile()); err == nil {
		t.Fatal("invalid source hash was accepted")
	}
}

func TestCacheDerivedAssetUsesCanonicalIndexAndOnlyCompleteHits(t *testing.T) {
	c := newTestCache(t)
	ctx := context.Background()
	source := assetref.ContentHash(strings.Repeat("c", 64))
	profile := testNormalizationProfile()

	if _, found, err := c.FindDerived(ctx, source, profile); err != nil || found {
		t.Fatalf("empty FindDerived: found=%v err=%v", found, err)
	}

	key, err := c.StoreDerived(ctx, source, profile, Entry{
		LocalPath:        filepath.Join(t.TempDir(), "normalized.mp4"),
		ContentHash:      assetref.ContentHash(strings.Repeat("d", 64)),
		SizeBytes:        1234,
		DownloadComplete: true,
	})
	if err != nil {
		t.Fatalf("StoreDerived: %v", err)
	}
	got, found, err := c.FindDerived(ctx, source, profile)
	if err != nil || !found {
		t.Fatalf("FindDerived after store: found=%v err=%v", found, err)
	}
	if got.AssetKey != key || got.ContentHash != assetref.ContentHash(strings.Repeat("d", 64)) {
		t.Fatalf("derived entry = %+v, want key=%q and verified output hash", got, key)
	}

	otherSource := assetref.ContentHash(strings.Repeat("e", 64))
	if _, found, err := c.FindDerived(ctx, otherSource, profile); err != nil || found {
		t.Fatalf("different source unexpectedly hit: found=%v err=%v", found, err)
	}
}

func TestCacheStoreDerivedRequiresCompleteMatchingEntry(t *testing.T) {
	c := newTestCache(t)
	ctx := context.Background()
	source := assetref.ContentHash(strings.Repeat("f", 64))
	profile := testNormalizationProfile()
	canonicalKey, err := DerivedAssetKey(source, profile)
	if err != nil {
		t.Fatalf("DerivedAssetKey: %v", err)
	}

	if _, err := c.StoreDerived(ctx, source, profile, Entry{LocalPath: "/tmp/partial.mp4"}); err == nil {
		t.Fatal("incomplete derived entry was accepted")
	}
	if _, err := c.StoreDerived(ctx, source, profile, Entry{
		AssetKey:         "wrong-key",
		LocalPath:        "/tmp/normalized.mp4",
		DownloadComplete: true,
	}); err == nil {
		t.Fatal("entry with non-canonical key was accepted")
	}
	if _, err := c.StoreDerived(ctx, source, profile, Entry{
		AssetKey:         canonicalKey,
		LocalPath:        "/tmp/normalized.mp4",
		DownloadComplete: true,
	}); err != nil {
		t.Fatalf("canonical complete entry rejected: %v", err)
	}
	if _, err := c.StoreDerived(ctx, source, profile, Entry{
		LocalPath:        "/tmp/duplicate.mp4",
		DownloadComplete: true,
	}); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("duplicate StoreDerived err=%v, want ErrDuplicate", err)
	}
}
