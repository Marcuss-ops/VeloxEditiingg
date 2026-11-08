package assets

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func TestStagePreparedFragmentLinksOrClonesAndHashes(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "prepared.mp4")
	content := []byte("prepared-fragment-bytes")
	if err := os.WriteFile(src, content, 0o600); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(root, "staging", "prepared.mp4")
	written, sha, outcome, err := stagePreparedFragment(src, dst, 1<<20, true)
	if err != nil {
		t.Fatalf("stage prepared fragment: %v", err)
	}
	if written != int64(len(content)) {
		t.Fatalf("written = %d, want %d", written, len(content))
	}
	want := sha256.Sum256(content)
	if sha != hex.EncodeToString(want[:]) {
		t.Fatalf("sha = %q, want %q", sha, hex.EncodeToString(want[:]))
	}
	if outcome == preparedStageCopy {
		t.Fatalf("expected clone or hardlink on a local filesystem, got %q", outcome)
	}
	staged, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(staged) != string(content) {
		t.Fatalf("staged bytes differ: %q", staged)
	}
}

func TestStagePreparedFragmentCopyFallback(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "prepared.mp4")
	content := []byte("copy-fallback-bytes")
	if err := os.WriteFile(src, content, 0o600); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(root, "staging", "prepared.mp4")
	written, sha, outcome, err := stagePreparedFragmentWithOptions(src, dst, 1<<20, true, false, false)
	if err != nil {
		t.Fatalf("copy fallback: %v", err)
	}
	if outcome != preparedStageCopy {
		t.Fatalf("outcome = %q, want copy", outcome)
	}
	if written != int64(len(content)) {
		t.Fatalf("written = %d, want %d", written, len(content))
	}
	want := sha256.Sum256(content)
	if sha != hex.EncodeToString(want[:]) {
		t.Fatalf("sha = %q, want %q", sha, hex.EncodeToString(want[:]))
	}
}

func TestStagePreparedFragmentRejectsOversize(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "prepared.mp4")
	if err := os.WriteFile(src, []byte("too-large"), 0o600); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(root, "staging", "prepared.mp4")
	if _, _, _, err := stagePreparedFragment(src, dst, 4, true); err == nil {
		t.Fatal("expected an oversize error")
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Fatalf("staging file should not exist after rejection, stat err = %v", err)
	}
}
