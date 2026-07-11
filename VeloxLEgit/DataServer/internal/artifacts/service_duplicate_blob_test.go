package artifacts

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// testBlobStore is a minimal store.BlobStore implementation for testing
// materializeDuplicateFinalBlob without pulling in the store package
// (which depends on CGO via go-sqlite3). Only FinalDir() is exercised
// by the test; the rest are no-ops.
type testBlobStore struct {
	finalDir string
}

func (t *testBlobStore) StagingPath(_, _, _ string) (string, error) { return "", nil }
func (t *testBlobStore) FinalPath(_, _, _ string) string             { return "" }
func (t *testBlobStore) PromoteToFinal(_, _ string) (string, error)  { return "", nil }
func (t *testBlobStore) RemoveStaging(_ string) error                { return nil }
func (t *testBlobStore) ReadFinal(_ string) (*os.File, error)        { return nil, nil }
func (t *testBlobStore) StagingDir() string                           { return t.finalDir }
func (t *testBlobStore) FinalDir() string                             { return t.finalDir }

// TestIsArtifactStorageKeyConflict_NilAndSubstring verifies the
// substring-based classifier for the (storage_provider, storage_key)
// UNIQUE-constraint error raised by FinalizeVerified's INSERT/UPDATE
// on the artifacts table.
func TestIsArtifactStorageKeyConflict_NilAndSubstring(t *testing.T) {
	t.Parallel()

	// nil err → false
	if isArtifactStorageKeyConflict(nil) {
		t.Error("nil err must classify as non-conflict")
	}

	// Exact SQLite UNIQUE-constraint string → true
	conflictErr := errors.New(`UNIQUE constraint failed: artifacts.storage_provider, artifacts.storage_key`)
	if !isArtifactStorageKeyConflict(conflictErr) {
		t.Error("UNIQUE-constraint substring must classify as conflict")
	}

	// Driver-prefixed variant → true (substring match)
	wrapped := errors.New(`sqlite3: UNIQUE constraint failed: artifacts.storage_provider, artifacts.storage_key: some context`)
	if !isArtifactStorageKeyConflict(wrapped) {
		t.Error("driver-prefixed UNIQUE-constraint substring must classify as conflict")
	}

	// Generic error → false
	generic := errors.New("some other error")
	if isArtifactStorageKeyConflict(generic) {
		t.Error("generic error must classify as non-conflict")
	}

	// Different UNIQUE-constraint (different table/columns) → false
	diffTable := errors.New("UNIQUE constraint failed: jobs.job_id")
	if isArtifactStorageKeyConflict(diffTable) {
		t.Error("UNIQUE-constraint on different table/columns must classify as non-conflict")
	}
}

// TestMakeDuplicateStorageKey_EmptyInputs verifies the alt-key
// derivation: empty/whitespace inputs are rejected; both present
// produces <base>.dup-<id><ext>.
func TestMakeDuplicateStorageKey_EmptyInputs(t *testing.T) {
	t.Parallel()

	// Empty storageKey → error
	if _, err := makeDuplicateStorageKey("", "art-1"); err == nil {
		t.Error("empty storageKey must error")
	}

	// Empty artifactID → error
	if _, err := makeDuplicateStorageKey("sha256-abc.bin", ""); err == nil {
		t.Error("empty artifactID must error")
	}

	// Whitespace-only inputs → error (trimmed to empty)
	if _, err := makeDuplicateStorageKey("   ", "art-1"); err == nil {
		t.Error("whitespace-only storageKey must error")
	}
	if _, err := makeDuplicateStorageKey("sha256-abc.bin", "   "); err == nil {
		t.Error("whitespace-only artifactID must error")
	}

	// Both present with extension → <base>.dup-<id><ext>
	got, err := makeDuplicateStorageKey("sha256-abc.bin", "art-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "sha256-abc.dup-art-1.bin"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}

	// Both present without extension → <base>.dup-<id> (no trailing ext)
	got, err = makeDuplicateStorageKey("sha256-abc", "art-2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want = "sha256-abc.dup-art-2"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// TestMaterializeDuplicateFinalBlob_Idempotent verifies the hardlink
// fallback: first call creates the target, second call is a no-op
// (idempotent on existing target). nil receiver / nil blobStore
// must error.
func TestMaterializeDuplicateFinalBlob_Idempotent(t *testing.T) {
	t.Parallel()

	t.Run("nil_receiver", func(t *testing.T) {
		t.Parallel()
		var s *Service
		if err := s.materializeDuplicateFinalBlob("src", "dst"); err == nil {
			t.Error("nil receiver must error")
		}
	})

	t.Run("nil_blobstore", func(t *testing.T) {
		t.Parallel()
		s := &Service{}
		if err := s.materializeDuplicateFinalBlob("src", "dst"); err == nil {
			t.Error("nil blobStore must error")
		}
	})

	t.Run("hardlink_then_idempotent", func(t *testing.T) {
		t.Parallel()
		tempDir := t.TempDir()
		bs := &testBlobStore{finalDir: tempDir}
		s := &Service{blobStore: bs}

		// Create a source file with known content
		sourceKey := "job-x/art-1/sha256-abc.bin"
		sourcePath := filepath.Join(bs.FinalDir(), filepath.FromSlash(sourceKey))
		if err := os.MkdirAll(filepath.Dir(sourcePath), 0o755); err != nil {
			t.Fatalf("mkdir source: %v", err)
		}
		if err := os.WriteFile(sourcePath, []byte("hello world"), 0o644); err != nil {
			t.Fatalf("write source: %v", err)
		}

		// Derive the alt target key (same as the real code would)
		altKey, err := makeDuplicateStorageKey(sourceKey, "art-1-dup")
		if err != nil {
			t.Fatalf("makeDuplicateStorageKey: %v", err)
		}
		altPath := filepath.Join(bs.FinalDir(), filepath.FromSlash(altKey))

		// First call: must hardlink the source to the target
		if err := s.materializeDuplicateFinalBlob(sourceKey, altKey); err != nil {
			t.Fatalf("first materializeDuplicateFinalBlob: %v", err)
		}
		if _, err := os.Stat(altPath); err != nil {
			t.Errorf("target must exist after first call: %v", err)
		}

		// Second call: idempotent (target already exists → nil)
		if err := s.materializeDuplicateFinalBlob(sourceKey, altKey); err != nil {
			t.Errorf("second call must be idempotent: %v", err)
		}
		if _, err := os.Stat(altPath); err != nil {
			t.Errorf("target must still exist after second call: %v", err)
		}

		// Verify the target content matches the source (hardlink sanity)
		gotContent, err := os.ReadFile(altPath)
		if err != nil {
			t.Fatalf("read target: %v", err)
		}
		if string(gotContent) != "hello world" {
			t.Errorf("target content mismatch: got %q, want %q", string(gotContent), "hello world")
		}
	})
}
