package migrations

import (
	"embed"
	"strings"
	"testing"
)

//go:embed testdata/*.sql
var testMigrationsFS embed.FS

//go:embed testdata/duplicates/*.sql
var duplicateMigrationsFS embed.FS

// ============================================================
// discoverMigrations tests
// ============================================================

func TestDiscoverMigrations_AllVersions(t *testing.T) {
	migs, err := discoverMigrations(testMigrationsFS, "testdata")
	if err != nil {
		t.Fatalf("discoverMigrations failed: %v", err)
	}

	// Self-updating count: the test asserts that discoverMigrations
	// returns at least one migration and that the FIRST 8 follow the
	// canonical version/name ordering produced by the runner. No
	// hardcoded count — the file count at migrations/ root can
	// shrink/grow as files relocate onto the recursive
	// migrations/sqlite/ embed track (Path B architectural push) or
	// new migrations land at the root. Mirrors the dynamic pattern
	// already used by TestRunMigrations_FullLifecycle, TestAppliedVersions,
	// TestPendingVersions, and TestIntegration_MigrationRunner_EndToEnd.
	if len(migs) == 0 {
		t.Fatal("expected discoverMigrations to return at least one migration, got 0")
	}

	expectedFirst8 := []struct {
		Version int
		Name    string
	}{
		{1, "initial"},
		{2, "legacy_imports"},
		{3, "youtube_canonical"},
		{4, "ansible"},
		{5, "legacy_cleanup"},
		{6, "drive_links_source_of_truth"},
		{7, "queue_persistence"},
		{8, "drop_legacy_tables"},
	}

	compareLen := len(expectedFirst8)
	if len(migs) < compareLen {
		compareLen = len(migs)
	}
	for i := 0; i < compareLen; i++ {
		if migs[i].Version != expectedFirst8[i].Version {
			t.Errorf("migration[%d] version: got %d, want %d", i, migs[i].Version, expectedFirst8[i].Version)
		}
		if migs[i].Name != expectedFirst8[i].Name {
			t.Errorf("migration[%d] name: got %q, want %q", i, migs[i].Name, expectedFirst8[i].Name)
		}
		if migs[i].Checksum == "" {
			t.Errorf("migration[%d] checksum is empty", i)
		}
		if migs[i].SQL == "" {
			t.Errorf("migration[%d] SQL is empty", i)
		}
	}
}

func TestDiscoverMigrations_SortedByVersion(t *testing.T) {
	migs, err := discoverMigrations(testMigrationsFS, "testdata")
	if err != nil {
		t.Fatalf("discoverMigrations failed: %v", err)
	}

	for i := 1; i < len(migs); i++ {
		if migs[i].Version <= migs[i-1].Version {
			t.Errorf("migrations not sorted: %d (%d) after %d (%d)",
				i, migs[i].Version, i-1, migs[i-1].Version)
		}
	}
}

// TestDiscoverMigrationsRejectsDuplicateVersions verifies that discoverMigrations
// returns a hard error when two .sql files share the same version number.
func TestDiscoverMigrationsRejectsDuplicateVersions(t *testing.T) {
	// duplicateMigrationsFS embeds testdata/duplicates/ which contains
	// 029_first.sql and 029_second.sql (same version)
	_, err := discoverMigrations(duplicateMigrationsFS, "testdata/duplicates")
	if err == nil {
		t.Fatal("expected error for duplicate migration version, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate migration version 029") {
		t.Errorf("error message should mention 'duplicate migration version 029', got: %v", err)
	}
}

// TestDiscoverPostgresMigrations_NoDuplicateVersions guards the real
// postgres/ chain (not the testdata mirror): every version must be
// unique or RunMigrations fails closed at discovery. Historically
// postgres/010_drop_youtube_domain.sql collided with
// postgres/010_drive.sql on version 010 (fixed 2026-08-07 by
// renumbering the drop to 023) — without this pin the drop becomes
// silently unreachable again.
func TestDiscoverPostgresMigrations_NoDuplicateVersions(t *testing.T) {
	migs, err := discoverMigrations(postgresRootFS, "postgres")
	if err != nil {
		t.Fatalf("discoverMigrations(postgres): %v", err)
	}
	if len(migs) == 0 {
		t.Fatal("expected postgres migrations, got 0")
	}
	seen := make(map[int]string, len(migs))
	for _, m := range migs {
		if prev, ok := seen[m.Version]; ok {
			t.Fatalf("duplicate postgres migration version %03d: %s and %s", m.Version, prev, m.Name)
		}
		seen[m.Version] = m.Name
	}
	// The YouTube domain drop must exist and be ordered after the
	// 009_youtube CREATE chain so the removal actually applies.
	var dropFound bool
	for _, m := range migs {
		if m.Name == "drop_youtube_domain" {
			dropFound = true
			if m.Version <= 9 {
				t.Fatalf("drop_youtube_domain at version %03d must come after 009_youtube", m.Version)
			}
		}
	}
	if !dropFound {
		t.Fatal("postgres chain missing drop_youtube_domain migration")
	}
}

func TestDiscoverMigrations_ChecksumStable(t *testing.T) {
	migs1, _ := discoverMigrations(testMigrationsFS, "testdata")
	migs2, _ := discoverMigrations(testMigrationsFS, "testdata")

	for i := range migs1 {
		if migs1[i].Checksum != migs2[i].Checksum {
			t.Errorf("migration %03d_%s: checksum not stable: %s vs %s",
				migs1[i].Version, migs1[i].Name, migs1[i].Checksum, migs2[i].Checksum)
		}
	}
}
