// Package store_test provides external tests for the store package,
// verifying that SQLiteStore correctly implements interfaces defined
// in other packages without creating import cycles.
package store_test

import (
	"testing"

	"velox-server/internal/handlers/remote/ansible"
	"velox-server/internal/store"
)

// ============================================================
// Compile-time interface compliance checks
//
// These will fail to compile if SQLiteStore does not implement
// all the methods required by each interface.
// ============================================================

var _ ansible.AnsibleComputerStore = (*store.SQLiteStore)(nil)

// ============================================================
// Runtime interface compliance tests
//
// These verify that the SQLiteStore methods actually work when
// accessed through the interface types, not just at compile time.
// ============================================================

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
// Runtime method tests through AnsibleComputerStore interface
// ============================================================

func TestInterface_AnsibleComputerStore_StructuredHosts(t *testing.T) {
	s := newTestStore(t)
	var acStore ansible.AnsibleComputerStore = s

	// Upsert via interface
	host := store.AnsibleHostFields{
		Host:        "test-via-interface",
		AnsibleUser: "pierone",
		Enabled:     true,
		Group:       "production",
		Tags:        []string{"web", "nginx"},
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
