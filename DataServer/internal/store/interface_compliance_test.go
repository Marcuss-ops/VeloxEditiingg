// Package store_test provides external tests for the store package,
// verifying that SQLiteStore correctly implements interfaces defined
// in other packages without creating import cycles.
package store_test

import (
	"testing"

	"velox-server/internal/handlers/remote/ansible"
	"velox-server/internal/integrations/youtube"
	"velox-server/internal/store"
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
	//   - ListYouTubeGroupsV2()
	//   - UpsertYouTubeGroupV2(name, groupType, description, privacy)
	//   - GetYouTubeCache(key) / SetYouTubeCache(key, ts, data)
	//   - CleanupYouTubeCache(maxAge) / ClearYouTubeCache()
	//   - MigrateYouTubeCache(entries)

	s := newTestStore(t)
	var ytStore youtube.YouTubeStore = s
	_ = ytStore // suppress unused warning
}

func TestInterface_AnsibleComputerStore_CompileTime(t *testing.T) {
	// This test exists as documentation — the real check is the
	// compile-time assertion above. If the code compiles, the
	// interface is satisfied.
	//
	// AnsibleComputerStore interface (structured ansible_hosts):
	//   - UpsertAnsibleHost, DeleteAnsibleHost,
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
	t.Skip("ListYouTubeGroups and UpsertYouTubeGroup removed in PR 3.5-b — use ListYouTubeGroupsV2 and UpsertYouTubeGroupV2")
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
