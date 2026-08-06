package store

import (
	"testing"
)

func openWorkerNodesDB(t *testing.T) *SQLiteStore {
	t.Helper()
	db, err := NewSQLiteStore(t.TempDir() + "/worker-nodes.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func seedWorkerNode(t *testing.T, db *SQLiteStore, h AnsibleHostFields) {
	t.Helper()
	if err := db.UpsertAnsibleHost(h); err != nil {
		t.Fatal(err)
	}
}

// TestListWorkerNodes_CanonicalView pins the Phase 9 canonical inventory
// projection: only enabled rows carrying a worker_id are returned; the
// worker_id is the identity and host_group maps to Environment.
func TestListWorkerNodes_CanonicalView(t *testing.T) {
	db := openWorkerNodesDB(t)

	seedWorkerNode(t, db, AnsibleHostFields{
		Host: "57.129.132.133", AnsibleUser: "pierone", SecretRef: "file:ssh_1",
		Group: "production", WorkerID: "host_57_129_132_133", Enabled: true,
	})
	seedWorkerNode(t, db, AnsibleHostFields{
		Host: "51.222.204.158", AnsibleUser: "ubuntu", SecretRef: "file:ssh_2",
		Group: "production", WorkerID: "velox-worker-523925eb", Enabled: true,
	})
	// Disabled row with a worker_id must NOT appear (runtime consumers
	// would otherwise try to SSH a drained node).
	seedWorkerNode(t, db, AnsibleHostFields{
		Host: "149.56.131.97", AnsibleUser: "pierone", SecretRef: "file:ssh_3",
		Group: "staging", WorkerID: "velox-worker-13197", Enabled: false,
	})
	// Enabled row WITHOUT a worker_id must NOT appear (no canonical
	// identity to key the SSH registry on).
	seedWorkerNode(t, db, AnsibleHostFields{
		Host: "10.0.0.9", AnsibleUser: "ops", SecretRef: "file:ssh_4",
		Group: "infra", WorkerID: "", Enabled: true,
	})

	nodes, err := db.ListWorkerNodes()
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 {
		t.Fatalf("ListWorkerNodes() = %d nodes, want 2", len(nodes))
	}
	got := map[string]WorkerNode{}
	for _, n := range nodes {
		got[n.WorkerID] = n
	}
	for _, wantID := range []string{"host_57_129_132_133", "velox-worker-523925eb"} {
		n, ok := got[wantID]
		if !ok {
			t.Fatalf("worker %s missing from canonical view", wantID)
		}
		if n.SSHHost == "" || n.SSHUser == "" {
			t.Errorf("worker %s: incomplete SSH connectivity (host=%q user=%q)", wantID, n.SSHHost, n.SSHUser)
		}
		if n.Environment != "production" {
			t.Errorf("worker %s: Environment=%q want production (host_group projection)", wantID, n.Environment)
		}
		if !n.Enabled {
			t.Errorf("worker %s: Enabled=false want true", wantID)
		}
	}
}

// TestListWorkerNodes_WithDisabled includes drained rows for audit
// tooling while the runtime view excludes them.
func TestListWorkerNodes_WithDisabled(t *testing.T) {
	db := openWorkerNodesDB(t)
	seedWorkerNode(t, db, AnsibleHostFields{
		Host: "149.56.131.97", AnsibleUser: "pierone", SecretRef: "file:ssh_3",
		Group: "staging", WorkerID: "velox-worker-13197", Enabled: false,
	})
	nodes, err := db.ListWorkerNodes()
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 0 {
		t.Fatalf("ListWorkerNodes() = %d, want 0 (disabled excluded)", len(nodes))
	}
	all, err := db.ListWorkerNodesWithDisabled()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 || all[0].WorkerID != "velox-worker-13197" {
		t.Fatalf("ListWorkerNodesWithDisabled() = %+v, want the disabled worker", all)
	}
	if all[0].Enabled {
		t.Errorf("disabled worker reported Enabled=true")
	}
}
