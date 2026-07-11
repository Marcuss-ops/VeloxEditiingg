package providers

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"velox-server/internal/deliveries"
	"velox-server/internal/store"
)

func TestResolveArtifactFilePathUsesBlobStore(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	blobStore, err := store.NewFilesystemBlobStore(filepath.Join(root, "staging"), filepath.Join(root, "final"))
	if err != nil {
		t.Fatal(err)
	}
	finalPath := blobStore.FinalPath("job-1", "artifact-1", ".mp4")
	if err := os.MkdirAll(filepath.Dir(finalPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(finalPath, []byte("video"), 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := resolveArtifactFilePath(blobStore, &store.Artifact{ID: "artifact-1", StorageKey: finalPath})
	if err != nil {
		t.Fatalf("resolve artifact path: %v", err)
	}
	if !filepath.IsAbs(got) || got != finalPath {
		t.Fatalf("path = %q, want absolute %q", got, finalPath)
	}
}

func TestResolveArtifactFilePathUsesExplicitLegacyFallback(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	blobStore, err := store.NewFilesystemBlobStore(filepath.Join(root, "staging"), filepath.Join(root, "final"))
	if err != nil {
		t.Fatal(err)
	}
	legacy := filepath.Join(root, "legacy.mp4")
	if err := os.WriteFile(legacy, []byte("video"), 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := resolveArtifactFilePath(blobStore, &store.Artifact{
		ID:         "artifact-1",
		StorageKey: filepath.Join(root, "missing.mp4"),
		LocalPath:  legacy,
	})
	if err != nil {
		t.Fatalf("resolve legacy path: %v", err)
	}
	if got != legacy {
		t.Fatalf("path = %q, want %q", got, legacy)
	}
}

func TestResolveArtifactFilePathRejectsAmbiguousRelativeKey(t *testing.T) {
	t.Parallel()
	_, err := resolveArtifactFilePath(nil, &store.Artifact{ID: "artifact-1", StorageKey: "relative/video.mp4"})
	if err == nil {
		t.Fatal("expected an error")
	}
	if !errors.Is(err, deliveries.ErrProviderPermanent) {
		t.Fatalf("error = %v, want permanent classification", err)
	}
}
