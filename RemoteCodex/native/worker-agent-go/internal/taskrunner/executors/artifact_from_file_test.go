package executors

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

// artifact_from_file_test.go pins the streaming SHA-256 in artifactFromFile:
// the executor finalize hash must equal a whole-buffer sha256.Sum256 for the
// same bytes (digest contract), while never buffering the file in memory.

func TestArtifactFromFile_HashMatchesBufferedSHA(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.mp4")

	// Exceed the 1 MiB copy buffer so the streamer is forced to read more
	// than one chunk; this catches a short-read or premature-Sum bug that a
	// tiny fixture would miss.
	content := make([]byte, (1<<20)+137)
	for i := range content {
		content[i] = byte(i * 31)
	}
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	got, err := artifactFromFile("video/mp4", path)
	if err != nil {
		t.Fatalf("artifactFromFile: %v", err)
	}

	want := sha256.Sum256(content)
	if got.Hash != hex.EncodeToString(want[:]) {
		t.Fatalf("streamed hash = %s, want %s", got.Hash, hex.EncodeToString(want[:]))
	}
	if got.SizeBytes != int64(len(content)) {
		t.Fatalf("SizeBytes = %d, want %d", got.SizeBytes, len(content))
	}
	if got.URI != path {
		t.Fatalf("URI = %q, want %q", got.URI, path)
	}
	if got.Type != "video/mp4" {
		t.Fatalf("Type = %q, want %q", got.Type, "video/mp4")
	}
}

func TestArtifactFromFile_RejectsEmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.mp4")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	if _, err := artifactFromFile("video/mp4", path); err == nil {
		t.Fatal("artifactFromFile on an empty file must fail")
	}
}

func TestArtifactFromFile_MissingFileFails(t *testing.T) {
	dir := t.TempDir()
	if _, err := artifactFromFile("video/mp4", filepath.Join(dir, "nope.mp4")); err == nil {
		t.Fatal("artifactFromFile on a missing file must fail")
	}
}
