package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"velox-server/internal/store"
)

func osWriteFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o600)
}

const legacyInventoryFixture = `# Velox — fleet-operator inventory (legacy static form).
[velox_workers]
worker_57_129  ansible_host=57.129.132.133 ansible_user=pierone ansible_ssh_private_key_file=~/.ssh/id_ed25519_velox worker_id=host_57_129_132_133
worker_523925  ansible_host=51.222.204.158 ansible_user=ubuntu ansible_ssh_private_key_file=~/.ssh/id_ed25519_velox worker_id=velox-worker-523925eb

# A worker without a worker_id cannot be imported.
worker_orphan  ansible_host=10.0.0.99 ansible_user=ops

[master]
master_host  ansible_host=127.0.0.1 ansible_user=root
`

// TestParseLegacyInventory pins the Phase 9 one-time migration parser:
// worker-bearing rows are imported, rows without worker_id are skipped,
// and non-worker groups are ignored.
func TestParseLegacyInventory(t *testing.T) {
	nodes, skipped, err := parseLegacyInventory(strings.NewReader(legacyInventoryFixture))
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 {
		t.Fatalf("parsed %d nodes, want 2: %+v", len(nodes), nodes)
	}
	if nodes[0].WorkerID != "host_57_129_132_133" || nodes[0].Host != "57.129.132.133" || nodes[0].User != "pierone" {
		t.Errorf("node[0] = %+v", nodes[0])
	}
	if nodes[0].Alias != "worker_57_129" || nodes[0].Group != "velox_workers" {
		t.Errorf("node[0] alias/group = %q/%q", nodes[0].Alias, nodes[0].Group)
	}
	// Two rows are skipped: the orphan (no worker_id) and the master row
	// (worker group row without worker_id). Neither can back a node.
	if len(skipped) != 2 {
		t.Fatalf("skipped = %v, want 2 rows (orphan + master)", skipped)
	}
	if !strings.Contains(skipped[0], "worker_orphan") && !strings.Contains(skipped[1], "worker_orphan") {
		t.Fatalf("skipped = %v, want the orphan row", skipped)
	}
}

// TestRunSyncWorkerNodesApplySeedsRegistry pins the end-to-end command:
// after --apply, ListWorkerNodes returns exactly the imported rows.
func TestRunSyncWorkerNodesApplySeedsRegistry(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "nodes.db")
	inventoryPath := filepath.Join(dir, "inventory.ini")
	if _, err := store.NewSQLiteStore(dbPath); err != nil {
		t.Fatal(err)
	}
	if err := osWriteFile(inventoryPath, legacyInventoryFixture); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	if err := run([]string{"sync-worker-nodes", "--db", dbPath, "--inventory", inventoryPath}, &stdout, &stderr); err != nil {
		t.Fatalf("run sync: %v stderr=%q", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "summary: 2 worker nodes (synced), 2 skipped") {
		t.Fatalf("stdout=%q", stdout.String())
	}

	db, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	nodes, err := db.ListWorkerNodes()
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 {
		t.Fatalf("ListWorkerNodes() = %d, want 2", len(nodes))
	}
	// Ordered by worker_id: host_57_129_132_133 sorts before velox-worker-*.
	if nodes[0].SSHHost != "57.129.132.133" || nodes[0].WorkerID != "host_57_129_132_133" {
		t.Fatalf("nodes[0] = %+v", nodes[0])
	}
}

func TestRunSyncWorkerNodesDryRunDoesNotWrite(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "nodes.db")
	inventoryPath := filepath.Join(dir, "inventory.ini")
	if _, err := store.NewSQLiteStore(dbPath); err != nil {
		t.Fatal(err)
	}
	if err := osWriteFile(inventoryPath, legacyInventoryFixture); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	if err := run([]string{"sync-worker-nodes", "--db", dbPath, "--inventory", inventoryPath, "--dry-run"}, &stdout, &stderr); err != nil {
		t.Fatalf("run dry-run: %v stderr=%q", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "summary: 2 worker nodes (dry-run), 2 skipped") {
		t.Fatalf("stdout=%q", stdout.String())
	}
	if !strings.Contains(stdout.String(), "would-upsert") {
		t.Fatalf("dry-run should report would-upsert, got %q", stdout.String())
	}

	db, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	nodes, err := db.ListWorkerNodes()
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 0 {
		t.Fatalf("dry-run wrote %d nodes, want 0", len(nodes))
	}
}
