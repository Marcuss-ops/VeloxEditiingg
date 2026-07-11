package bundle

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestBundleHashMatches_OK: drive the happy path. cfg hash == file hash.
func TestBundleHashMatches_OK(t *testing.T) {
	dir := t.TempDir()
	const hash = "abc123-real"
	if err := os.WriteFile(filepath.Join(dir, BundleHashFilename), []byte(hash+"\n"), 0o644); err != nil {
		t.Fatalf("setup: write BUNDLE_HASH.txt: %v", err)
	}
	if err := BundleHashMatches(hash, dir); err != nil {
		t.Fatalf("BundleHashMatches OK: %v", err)
	}
}

// TestBundleHashMatches_MissingFile: file absent under dir → fail-closed.
func TestBundleHashMatches_MissingFile(t *testing.T) {
	dir := t.TempDir() // no BUNDLE_HASH.txt
	err := BundleHashMatches("anything", dir)
	if err == nil {
		t.Fatalf("expected error when BUNDLE_HASH.txt absent")
	}
	if msg := err.Error(); msg == "" || (msg != "" && !strings.Contains(msg, "bundle_version_mismatch")) {
		t.Fatalf("error must carry 'bundle_version_mismatch' code; got %q", msg)
	}
}

// TestBundleHashMatches_EmptyExpected: empty cfg.BundleHash is itself a fail.
func TestBundleHashMatches_EmptyExpected(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, BundleHashFilename), []byte("disk-real\n"), 0o644); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := BundleHashMatches("", dir); err == nil {
		t.Fatalf("expected mismatch on empty expected")
	} else if !strings.Contains(err.Error(), "bundle_version_mismatch") {
		t.Fatalf("error code missing: %q", err.Error())
	}
}

// TestBundleHashMatches_Mismatch: cfg != disk → fail-closed.
func TestBundleHashMatches_Mismatch(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, BundleHashFilename), []byte("disk-value\n"), 0o644); err != nil {
		t.Fatalf("setup: %v", err)
	}
	err := BundleHashMatches("cfg-value", dir)
	if err == nil {
		t.Fatalf("expected mismatch")
	}
	if !strings.Contains(err.Error(), "bundle_version_mismatch") {
		t.Fatalf("error must carry code: %q", err.Error())
	}
}

// TestBundleHashMatches_BlankFile: an empty BUNDLE_HASH.txt is treated as
// missing (RW-PROD-003 A8 invariant; a blank hash that matches an empty
// cfg.BundleHash would re-introduce the very mismatch class the gate
// exists to prevent).
func TestBundleHashMatches_BlankFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, BundleHashFilename), []byte("\n"), 0o644); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := BundleHashMatches("non-empty", dir); err == nil {
		t.Fatalf("expected mismatch when file is blank")
	}
}

// TestBundleHashMatches_VersionsPath: alternate candidates under
// versions/current/ should resolve. Mirrors main.go's readTextFileFirst
// path search.
func TestBundleHashMatches_VersionsPath(t *testing.T) {
	dir := t.TempDir()
	versionsDir := filepath.Join(dir, "versions", "current")
	if err := os.MkdirAll(versionsDir, 0o755); err != nil {
		t.Fatalf("setup: %v", err)
	}
	const hash = "versioned-hash"
	if err := os.WriteFile(filepath.Join(versionsDir, BundleHashFilename), []byte(hash+"\n"), 0o644); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := BundleHashMatches(hash, dir); err != nil {
		t.Fatalf("versions/current lookup failed: %v", err)
	}
}

// TestCanonicalCandidates_PathOrder: surface-stable order.
func TestCanonicalCandidates_PathOrder(t *testing.T) {
	got := CanonicalCandidates("/opt/velox", BundleHashFilename)
	want := []string{
		"/opt/velox/" + BundleHashFilename,
		"/opt/velox/versions/current/" + BundleHashFilename,
		"/opt/velox/" + BundleHashFilename, // baseDir default
	}
	if len(got) != len(want) {
		t.Fatalf("len mismatch got=%d want=%d", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("idx %d: got=%q want=%q", i, got[i], want[i])
		}
	}
}
