// Package store_test provides external tests for the store package,
// verifying that SQLiteStore correctly implements interfaces defined
// in other packages without creating import cycles.
package store_test

import (
	"testing"

	"velox-server/internal/handlers/remote/ansible"
	"velox-server/internal/integrations/youtube"
	"velox-server/internal/store"
	"velox-server/internal/store/migrations"
)

// ============================================================
// Compile-time interface compliance checks
//
// These will fail to compile if SQLiteStore does not implement
// all the methods required by each interface.
// ============================================================

var _ youtube.YouTubeStore = (*store.SQLiteStore)(nil)
var _ ansible.AnsibleComputerStore = (*store.SQLiteStore)(nil)

// ============================================================
// Runtime interface compliance tests
//
// These verify that the SQLiteStore methods actually work when
// accessed through the interface types, not just at compile time.
// ============================================================

func TestInterface_YouTubeStore_CompileTime(t *testing.T) {
	// This test exists as documentation — the real check is the
	// compile-time assertion above. If the code compiles, the
	// interface is satisfied.
	//
	// SQLiteStore implements:
	//   - ListYouTubeGroups()
	//   - UpsertYouTubeGroup(name, description, privacy, channels, rawJSON)
	//   - GetYouTubeCache(key) / SetYouTubeCache(key, ts, data)
	//   - CleanupYouTubeCache(maxAge) / ClearYouTubeCache()
	//   - MigrateYouTubeCache(entries)
	//   - ListYouTubeChannelMetadata()
	//   - UpsertYouTubeChannelMetadata(id, title, tokenPath, language, addedDate, lastUsed, rawJSON)

	s := newTestStore(t)
	var ytStore youtube.YouTubeStore = s
	_ = ytStore // suppress unused warning
}

func TestInterface_AnsibleComputerStore_CompileTime(t *testing.T) {
	// This test exists as documentation — the real check is the
	// compile-time assertion above. If the code compiles, the
	// interface is satisfied.
	//
	// SQLiteStore implements:
	//   - Legacy: GetAnsibleComputer, ListAnsibleComputers,
	//     UpsertAnsibleComputer, DeleteAnsibleComputer,
	//     MigrateAnsibleComputersFromJSON
	//   - Structured: UpsertAnsibleHost, DeleteAnsibleHost,
	//     GetAnsibleHost, ListAnsibleHosts

	s := newTestStore(t)
	var acStore ansible.AnsibleComputerStore = s
	_ = acStore // suppress unused warning
}

// ============================================================
// Runtime method tests through YouTubeStore interface
// ============================================================

func TestInterface_YouTubeStore_Cache(t *testing.T) {
	s := newTestStore(t)
	var ytStore youtube.YouTubeStore = s

	// Set and Get via interface
	if err := ytStore.SetYouTubeCache("test-key", 1000, `{"value":1}`); err != nil {
		t.Fatalf("SetYouTubeCache via interface: %v", err)
	}

	ts, data, err := ytStore.GetYouTubeCache("test-key")
	if err != nil {
		t.Fatalf("GetYouTubeCache via interface: %v", err)
	}
	if ts != 1000 {
		t.Errorf("timestamp: got %d, want 1000", ts)
	}
	if data != `{"value":1}` {
		t.Errorf("data: got %q, want %q", data, `{"value":1}`)
	}

	// Miss
	ts, data, err = ytStore.GetYouTubeCache("nonexistent")
	if err != nil {
		t.Fatalf("GetYouTubeCache miss via interface: %v", err)
	}
	if ts != 0 || data != "" {
		t.Errorf("expected zero value on miss, got ts=%d data=%q", ts, data)
	}

	// Clear via interface
	if err := ytStore.ClearYouTubeCache(); err != nil {
		t.Fatalf("ClearYouTubeCache via interface: %v", err)
	}

	_, data, _ = ytStore.GetYouTubeCache("test-key")
	if data != "" {
		t.Error("expected empty after clear via interface")
	}
}

func TestInterface_YouTubeStore_LegacyGroups(t *testing.T) {
	s := newTestStore(t)

	// Skip if migration 008 has been applied (legacy tables dropped)
	versions, _ := migrations.AppliedVersions(s.DB())
	hasMigration008 := false
	for _, v := range versions {
		if v >= 8 {
			hasMigration008 = true
			break
		}
	}
	if hasMigration008 {
		t.Skip("youtube_groups table dropped by migration 008")
	}

	var ytStore youtube.YouTubeStore = s

	// Upsert and List via interface
	if err := ytStore.UpsertYouTubeGroup("TestGroup", "A test group", "public", []string{"UC_a", "UC_b"}, ""); err != nil {
		t.Fatalf("UpsertYouTubeGroup via interface: %v", err)
	}

	groups, err := ytStore.ListYouTubeGroups()
	if err != nil {
		t.Fatalf("ListYouTubeGroups via interface: %v", err)
	}
	if len(groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(groups))
	}
	if groups[0]["name"] != "TestGroup" {
		t.Errorf("name: got %v, want %q", groups[0]["name"], "TestGroup")
	}
}

func TestInterface_YouTubeStore_LegacyChannelMetadata(t *testing.T) {
	s := newTestStore(t)

	// Skip if migration 008 has been applied (legacy tables dropped)
	versions, _ := migrations.AppliedVersions(s.DB())
	hasMigration008 := false
	for _, v := range versions {
		if v >= 8 {
			hasMigration008 = true
			break
		}
	}
	if hasMigration008 {
		t.Skip("youtube_channel_metadata table dropped by migration 008")
	}

	var ytStore youtube.YouTubeStore = s

	// Upsert and List via interface
	if err := ytStore.UpsertYouTubeChannelMetadata("UC_legacy", "Legacy Channel", "/tokens/test.json", "en", "2024-01-01", "2024-06-01", `{"source":"test"}`); err != nil {
		t.Fatalf("UpsertYouTubeChannelMetadata via interface: %v", err)
	}

	meta, err := ytStore.ListYouTubeChannelMetadata()
	if err != nil {
		t.Fatalf("ListYouTubeChannelMetadata via interface: %v", err)
	}
	if len(meta) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(meta))
	}
	if meta["UC_legacy"]["title"] != "Legacy Channel" {
		t.Errorf("title: got %v, want %q", meta["UC_legacy"]["title"], "Legacy Channel")
	}
}

// ============================================================
// Runtime method tests through AnsibleComputerStore interface
// ============================================================

func TestInterface_AnsibleComputerStore_StructuredHosts(t *testing.T) {
	s := newTestStore(t)
	var acStore ansible.AnsibleComputerStore = s

	// Upsert via interface
	host := store.AnsibleHostFields{
		Host:       "test-via-interface",
		AnsibleUser: "pierone",
		Enabled:    true,
		Group:      "production",
		Tags:       []string{"web", "nginx"},
	}
	if err := acStore.UpsertAnsibleHost(host); err != nil {
		t.Fatalf("UpsertAnsibleHost via interface: %v", err)
	}

	// Get via interface
	got, err := acStore.GetAnsibleHost("test-via-interface")
	if err != nil {
		t.Fatalf("GetAnsibleHost via interface: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil host")
	}
	if got.Host != "test-via-interface" {
		t.Errorf("Host: got %q, want %q", got.Host, "test-via-interface")
	}
	if got.Enabled != true {
		t.Errorf("Enabled: got %v, want true", got.Enabled)
	}
	if len(got.Tags) != 2 || got.Tags[0] != "web" {
		t.Errorf("Tags: got %v, want [web nginx]", got.Tags)
	}

	// List via interface
	hosts, err := acStore.ListAnsibleHosts()
	if err != nil {
		t.Fatalf("ListAnsibleHosts via interface: %v", err)
	}
	if len(hosts) != 1 {
		t.Fatalf("expected 1 host, got %d", len(hosts))
	}

	// Delete via interface
	if err := acStore.DeleteAnsibleHost("test-via-interface"); err != nil {
		t.Fatalf("DeleteAnsibleHost via interface: %v", err)
	}
	_, err = acStore.GetAnsibleHost("test-via-interface")
	if err == nil {
		t.Error("expected error after delete via interface")
	}
}

func TestInterface_AnsibleComputerStore_LegacyComputers(t *testing.T) {
	s := newTestStore(t)

	// Skip if migration 008 has been applied (legacy tables dropped)
	versions, _ := migrations.AppliedVersions(s.DB())
	hasMigration008 := false
	for _, v := range versions {
		if v >= 8 {
			hasMigration008 = true
			break
		}
	}
	if hasMigration008 {
		t.Skip("ansible_computers table dropped by migration 008")
	}

	var acStore ansible.AnsibleComputerStore = s

	// Upsert legacy via interface
	if err := acStore.UpsertAnsibleComputer("legacy-host", `{"host":"legacy-host","enabled":true,"group":"legacy"}`); err != nil {
		t.Fatalf("UpsertAnsibleComputer via interface: %v", err)
	}

	// Get legacy via interface
	raw, err := acStore.GetAnsibleComputer("legacy-host")
	if err != nil {
		t.Fatalf("GetAnsibleComputer via interface: %v", err)
	}
	if raw == "" {
		t.Fatal("expected non-empty raw JSON")
	}

	// List legacy via interface
	computers, err := acStore.ListAnsibleComputers()
	if err != nil {
		t.Fatalf("ListAnsibleComputers via interface: %v", err)
	}
	if len(computers) != 1 {
		t.Fatalf("expected 1 legacy computer, got %d", len(computers))
	}

	// Delete legacy via interface
	if err := acStore.DeleteAnsibleComputer("legacy-host"); err != nil {
		t.Fatalf("DeleteAnsibleComputer via interface: %v", err)
	}
	raw, _ = acStore.GetAnsibleComputer("legacy-host")
	if raw != "" {
		t.Error("expected empty after delete via interface")
	}
}

// ============================================================
// Helpers
// ============================================================

func newTestStore(t *testing.T) *store.SQLiteStore {
	t.Helper()
	dbPath := t.TempDir() + "/interface_test.db"
	s, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
