package fleet

import (
	"testing"

	"velox-shared/identity"
)

func mustAdd(t *testing.T, reg *WorkerRegistry, e WorkerRegistryEntry) {
	t.Helper()
	if err := reg.AddWorker(e); err != nil {
		t.Fatal(err)
	}
}

// TestNewSSHClientFromRegistry pins the Phase 9 inventory-unification
// seam: the SSH client target map is derived from the canonical
// WorkerRegistry, and rows missing host or user are excluded (so a
// partially-populated inventory fails per-target at Run time instead of
// producing a malformed ssh invocation).
func TestNewSSHClientFromRegistry(t *testing.T) {
	reg := NewWorkerRegistry()
	mustAdd(t, reg, WorkerRegistryEntry{WorkerID: identity.ParseWorkerID("w-1"), Host: "10.0.0.1", SSHUser: "pierone"})
	mustAdd(t, reg, WorkerRegistryEntry{WorkerID: identity.ParseWorkerID("w-2"), Host: "10.0.0.2", SSHUser: "ubuntu"})
	// Defense-in-depth: even if an entry with empty host/user ever reached
	// the registry, the SSH derivation must skip it. (AddWorker normally
	// rejects empty-host rows, so this exercises the derivation guard.)
	_ = reg.AddWorker(WorkerRegistryEntry{WorkerID: identity.ParseWorkerID("w-3"), Host: "", SSHUser: "ops"})

	client, ok := NewSSHClientFromRegistry(reg).(*sshClient)
	if !ok {
		t.Fatalf("NewSSHClientFromRegistry returned %T, want *sshClient", client)
	}
	if len(client.targets) != 2 {
		t.Fatalf("target map has %d entries, want 2", len(client.targets))
	}
	if _, ok := client.targets["w-1"]; !ok {
		t.Errorf("w-1 missing from targets")
	}
	if _, ok := client.targets["w-3"]; ok {
		t.Errorf("w-3 (empty host) must be skipped")
	}
}

// TestNewSSHClientFromRegistry_NilRegistry returns a usable empty client.
func TestNewSSHClientFromRegistry_NilRegistry(t *testing.T) {
	client := NewSSHClientFromRegistry(nil)
	if _, ok := client.(*sshClient); !ok {
		t.Fatalf("nil registry returned %T, want *sshClient", client)
	}
}
