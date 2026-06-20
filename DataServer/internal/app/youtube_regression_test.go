package app

import (
	"path/filepath"
	"testing"

	"velox-server/internal/config"
	"velox-server/internal/store"
)

// TestYouTubeModule_EagerBuildWithRealStoreAndDataDir is the regression
// test for PR15.1's lifecycle bug. Before the fix, NewYouTubeModule was
// a thin struct constructor and the integration
// *integrations/youtube.Service was built lazily inside RegisterRoutes —
// bootstrap.go read ytMod.Service() BEFORE the registry's RegisterRoutes
// call to wire the YouTube delivery provider, so the provider was
// silently dropped.
//
// After the fix, NewYouTubeModule eagerly builds the service AND storage
// when given a real SQLite store and a non-empty dataDir. This test
// pins both invariants so any regression to "lazy-build" is caught at
// unit-test speed.
func TestYouTubeModule_EagerBuildWithRealStoreAndDataDir(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "velox_yt.db")

	sqliteStore, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("open SQLite: %v", err)
	}
	t.Cleanup(func() { _ = sqliteStore.Close() })

	cfg := &config.Config{}
	dataDir := tmpDir // non-empty -> Storage() must be non-nil

	m, err := NewYouTubeModule(cfg, dataDir, sqliteStore)
	if err != nil {
		t.Fatalf("NewYouTubeModule with real store must succeed (PR15.1 invariant): %v", err)
	}
	if m == nil {
		t.Fatal("expected non-nil module")
	}

	// The critical invariant: Service() is non-nil immediately after the
	// constructor returns. RegisterRoutes was never called.
	svc := m.Service()
	if svc == nil {
		t.Fatal("Service() must be non-nil after NewYouTubeModule(real store) — " +
			"otherwise bootstrap's delivery-provider registration reads nil " +
			"and the YouTube provider is silently dropped (PR15.1 lifecyle bug)")
	}

	// Storage() is also eagerly built when dataDir != "".
	if m.Storage() == nil {
		t.Fatal("Storage() must be non-nil after NewYouTubeModule(real store, dataDir) — " +
			"manager/feed paths depend on it")
	}

	// QuotaManager must be wired (registered to SetStore/SetDB during build).
	if svc.GetQuotaManager() == nil {
		t.Fatal("Service().GetQuotaManager() must be non-nil (PR15.1 contract)")
	}
}

// TestYouTubeModule_EagerBuild_NilDataDir_OmitsStorage locks the
// complementary behavior: with dataDir == "" the constructor still
// builds Service() (it doesn't depend on dataDir) but Storage() stays
// nil. This is the path that lets operator deployments pass empty
// dataDir legitimately without panics.
func TestYouTubeModule_EagerBuild_NilDataDir_OmitsStorage(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "velox_yt_empty_datadir.db")

	sqliteStore, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("open SQLite: %v", err)
	}
	t.Cleanup(func() { _ = sqliteStore.Close() })

	cfg := &config.Config{}

	m, err := NewYouTubeModule(cfg, "", sqliteStore)
	if err != nil {
		t.Fatalf("NewYouTubeModule: %v", err)
	}
	if m == nil {
		t.Fatal("expected non-nil module")
	}
	if m.Service() == nil {
		t.Error("Service() must be non-nil even with empty dataDir")
	}
	if m.Storage() != nil {
		t.Error("Storage() must be nil when dataDir is empty")
	}
}
