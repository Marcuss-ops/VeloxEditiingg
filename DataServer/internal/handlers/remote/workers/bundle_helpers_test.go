package workers

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBundleDirCandidatesIncludeInstallLayouts(t *testing.T) {
	dataDir := filepath.Join(string(filepath.Separator), "opt", "velox", "current", "data")
	candidates := bundleDirCandidates(dataDir)

	want := filepath.Join(string(filepath.Separator), "opt", "velox", "current", "refactored", "DataServer", "data", "worker_downloads")
	found := false
	for _, candidate := range candidates {
		if candidate == want {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected candidate %q in %v", want, candidates)
	}
}

func TestComputeBundleHashFromManifestFallback(t *testing.T) {
	dir := t.TempDir()
	want := "da65a9fb92abc0b2b80d6a46cba030cedf8b250de25a1ab494d4868ee6e49af2"
	manifest := []byte(`{"build_hash":"` + want + `","bundle_hash":"` + want + `"}`)
	if err := os.WriteFile(filepath.Join(dir, "manifest_v2.json"), manifest, 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	if got := computeBundleHashFromManifest(dir); got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}
