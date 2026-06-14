package store

import (
	"path/filepath"
	"testing"
)

// ============================================================
// ansible_hosts tests
// ============================================================

func TestAnsibleHostsCRUD(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	// Create
	host := AnsibleHostFields{
		Host:         "test-vm-01",
		AnsibleUser:  "pierone",
		SSHKeyPath:   "/home/pierone/.ssh/id_ed25519",
		SecretRef:    "vault://ssh/pierone@test-vm-01",
		Enabled:      true,
		Availability: "online",
		Group:        "production",
		Subgroup:     "web",
		Tags:         []string{"nginx", "letsencrypt"},
		Notes:        "Main web server",
		LinkedWorkerID: "worker-01",
	}
	if err := s.UpsertAnsibleHost(host); err != nil {
		t.Fatalf("UpsertAnsibleHost failed: %v", err)
	}

	// Get
	got, err := s.GetAnsibleHost("test-vm-01")
	if err != nil {
		t.Fatalf("GetAnsibleHost failed: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil host")
	}
	if got.Host != host.Host {
		t.Errorf("Host: got %q, want %q", got.Host, host.Host)
	}
	if got.AnsibleUser != host.AnsibleUser {
		t.Errorf("AnsibleUser: got %q, want %q", got.AnsibleUser, host.AnsibleUser)
	}
	if got.SecretRef != host.SecretRef {
		t.Errorf("SecretRef: got %q, want %q", got.SecretRef, host.SecretRef)
	}
	if got.Enabled != host.Enabled {
		t.Errorf("Enabled: got %v, want %v", got.Enabled, host.Enabled)
	}
	if got.Availability != host.Availability {
		t.Errorf("Availability: got %q, want %q", got.Availability, host.Availability)
	}
	if got.Group != host.Group {
		t.Errorf("Group: got %q, want %q", got.Group, host.Group)
	}
	if len(got.Tags) != 2 || got.Tags[0] != "nginx" {
		t.Errorf("Tags: got %v, want %v", got.Tags, host.Tags)
	}
	if got.CreatedAt == "" {
		t.Error("expected CreatedAt to be set")
	}
	if got.UpdatedAt == "" {
		t.Error("expected UpdatedAt to be set")
	}
}

func TestAnsibleHostsUpdate(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	host := AnsibleHostFields{
		Host:    "test-vm-02",
		Enabled: true,
		Group:   "staging",
	}
	if err := s.UpsertAnsibleHost(host); err != nil {
		t.Fatalf("first UpsertAnsibleHost failed: %v", err)
	}

	// Update fields
	host.Enabled = false
	host.Availability = "offline"
	host.Group = "maintenance"
	host.Notes = "Under maintenance"
	if err := s.UpsertAnsibleHost(host); err != nil {
		t.Fatalf("second UpsertAnsibleHost failed: %v", err)
	}

	got, err := s.GetAnsibleHost("test-vm-02")
	if err != nil {
		t.Fatalf("GetAnsibleHost failed: %v", err)
	}
	if got.Enabled != false {
		t.Error("expected host to be disabled after update")
	}
	if got.Availability != "offline" {
		t.Errorf("Availability: got %q, want %q", got.Availability, "offline")
	}
	if got.Group != "maintenance" {
		t.Errorf("Group: got %q, want %q", got.Group, "maintenance")
	}
	if got.Notes != "Under maintenance" {
		t.Errorf("Notes: got %q, want %q", got.Notes, "Under maintenance")
	}
}

func TestAnsibleHostsListAndDelete(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	// List empty
	hosts, err := s.ListAnsibleHosts()
	if err != nil {
		t.Fatalf("ListAnsibleHosts (empty) failed: %v", err)
	}
	if len(hosts) != 0 {
		t.Fatalf("expected 0 hosts, got %d", len(hosts))
	}

	// Create two hosts
	s.UpsertAnsibleHost(AnsibleHostFields{Host: "host-a", Group: "web"})
	s.UpsertAnsibleHost(AnsibleHostFields{Host: "host-b", Group: "db"})

	hosts, err = s.ListAnsibleHosts()
	if err != nil {
		t.Fatalf("ListAnsibleHosts failed: %v", err)
	}
	if len(hosts) != 2 {
		t.Fatalf("expected 2 hosts, got %d", len(hosts))
	}
	// Results should be ordered by host
	if hosts[0].Host != "host-a" || hosts[1].Host != "host-b" {
		t.Errorf("expected order host-a, host-b; got %s, %s", hosts[0].Host, hosts[1].Host)
	}

	// Delete
	if err := s.DeleteAnsibleHost("host-a"); err != nil {
		t.Fatalf("DeleteAnsibleHost failed: %v", err)
	}
	hosts, err = s.ListAnsibleHosts()
	if err != nil {
		t.Fatalf("ListAnsibleHosts after delete failed: %v", err)
	}
	if len(hosts) != 1 {
		t.Fatalf("expected 1 host after delete, got %d", len(hosts))
	}
	if hosts[0].Host != "host-b" {
		t.Errorf("expected remaining host-b, got %s", hosts[0].Host)
	}

	// Get deleted should fail
	_, err = s.GetAnsibleHost("host-a")
	if err == nil {
		t.Error("expected error for deleted host")
	}
}

func TestAnsibleHostsEmptyTags(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	host := AnsibleHostFields{Host: "no-tags", Enabled: true, Tags: []string{}}
	if err := s.UpsertAnsibleHost(host); err != nil {
		t.Fatalf("UpsertAnsibleHost failed: %v", err)
	}

	got, err := s.GetAnsibleHost("no-tags")
	if err != nil {
		t.Fatalf("GetAnsibleHost failed: %v", err)
	}
	if got.Tags == nil {
		t.Error("expected non-nil Tags (empty slice)")
	}
	if len(got.Tags) != 0 {
		t.Errorf("expected empty Tags, got %v", got.Tags)
	}
}

func TestAnsibleHostsSecretRef(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	host := AnsibleHostFields{
		Host:      "secret-vm",
		SecretRef: "vault://ssh/prod/web-01",
	}
	if err := s.UpsertAnsibleHost(host); err != nil {
		t.Fatalf("UpsertAnsibleHost failed: %v", err)
	}

	got, err := s.GetAnsibleHost("secret-vm")
	if err != nil {
		t.Fatalf("GetAnsibleHost failed: %v", err)
	}
	if got.SecretRef != "vault://ssh/prod/web-01" {
		t.Errorf("SecretRef: got %q, want %q", got.SecretRef, "vault://ssh/prod/web-01")
	}
}

// ============================================================
// ansible_runs tests
// ============================================================

func TestAnsibleRunsCRUD(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	// Create
	runID := "run-test-001"
	err := s.UpsertAnsibleRun(runID, "deploy", "site.yml", "success", 1000, 2000, 0,
		`["ansible site.yml -l host-a"]`, "All tasks completed successfully", "Setup preamble", "https://master.local", "env")
	if err != nil {
		t.Fatalf("UpsertAnsibleRun failed: %v", err)
	}

	// Get
	got, err := s.GetAnsibleRun(runID)
	if err != nil {
		t.Fatalf("GetAnsibleRun failed: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil run")
	}
	if got["run_id"] != runID {
		t.Errorf("run_id: got %v, want %q", got["run_id"], runID)
	}
	if got["action"] != "deploy" {
		t.Errorf("action: got %v, want %q", got["action"], "deploy")
	}
	if got["status"] != "success" {
		t.Errorf("status: got %v, want %q", got["status"], "success")
	}
	if got["return_code"] != 0 {
		t.Errorf("return_code: got %v, want 0", got["return_code"])
	}
}

func TestAnsibleRunsListOrder(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	// Create runs with different start times
	s.UpsertAnsibleRun("run-001", "deploy", "site.yml", "success", 3000, 4000, 0, "[]", "", "", "", "")
	s.UpsertAnsibleRun("run-002", "deploy", "site.yml", "failed", 1000, 2000, 1, "[]", "", "", "", "")
	s.UpsertAnsibleRun("run-003", "deploy", "site.yml", "success", 2000, 2500, 0, "[]", "", "", "", "")

	runs, err := s.ListAnsibleRuns(10)
	if err != nil {
		t.Fatalf("ListAnsibleRuns failed: %v", err)
	}
	if len(runs) != 3 {
		t.Fatalf("expected 3 runs, got %d", len(runs))
	}
	// Should be DESC by started_at: 3000, 2000, 1000
	if runs[0]["run_id"] != "run-001" {
		t.Errorf("expected first run-001, got %v", runs[0]["run_id"])
	}
	if runs[1]["run_id"] != "run-003" {
		t.Errorf("expected second run-003, got %v", runs[1]["run_id"])
	}
	if runs[2]["run_id"] != "run-002" {
		t.Errorf("expected third run-002, got %v", runs[2]["run_id"])
	}
}

func TestAnsibleRunsDeleteCascade(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	runID := "run-delete-cascade"
	s.UpsertAnsibleRun(runID, "deploy", "site.yml", "success", 1000, 2000, 0, "[]", "", "", "", "")
	s.AddAnsibleRunHost(runID, "host-a")
	s.AddAnsibleRunHost(runID, "host-b")

	// Delete run
	if err := s.DeleteAnsibleRun(runID); err != nil {
		t.Fatalf("DeleteAnsibleRun failed: %v", err)
	}

	// Verify run gone
	_, err := s.GetAnsibleRun(runID)
	if err == nil {
		t.Error("expected error for deleted run")
	}

	// Verify run hosts cascade deleted
	hosts, err := s.ListAnsibleRunHosts(runID)
	if err != nil {
		t.Fatalf("ListAnsibleRunHosts failed: %v", err)
	}
	if len(hosts) != 0 {
		t.Errorf("expected 0 hosts after cascade delete, got %d", len(hosts))
	}
}

func TestAnsibleRunsUpdate(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	runID := "run-update-test"
	s.UpsertAnsibleRun(runID, "deploy", "site.yml", "running", 1000, 0, 0, "[]", "", "", "", "")

	// Update: mark as completed
	s.UpsertAnsibleRun(runID, "deploy", "site.yml", "success", 1000, 5000, 0, `["step 1", "step 2"]`, "Output here", "", "", "")

	got, err := s.GetAnsibleRun(runID)
	if err != nil {
		t.Fatalf("GetAnsibleRun failed: %v", err)
	}
	if got["status"] != "success" {
		t.Errorf("status: got %v, want %q", got["status"], "success")
	}
	if got["ended_at"] != int64(5000) {
		t.Errorf("ended_at: got %v, want 5000", got["ended_at"])
	}
}

func TestAnsibleRunsDefaultLimit(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	// Create 3 runs
	for i := 0; i < 3; i++ {
		s.UpsertAnsibleRun("run-limit-"+string(rune('0'+i)), "deploy", "site.yml", "success", int64(i*1000), 0, 0, "[]", "", "", "", "")
	}

	// Call with limit=0 should use default (200)
	runs, err := s.ListAnsibleRuns(0)
	if err != nil {
		t.Fatalf("ListAnsibleRuns(0) failed: %v", err)
	}
	if len(runs) != 3 {
		t.Errorf("expected 3 runs with default limit, got %d", len(runs))
	}
}

// ============================================================
// ansible_run_hosts tests
// ============================================================

func TestAnsibleRunHostsAddAndList(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	runID := "run-hosts-test"
	s.UpsertAnsibleRun(runID, "deploy", "site.yml", "success", 1000, 2000, 0, "[]", "", "", "", "")

	// Add hosts
	if err := s.AddAnsibleRunHost(runID, "host-a"); err != nil {
		t.Fatalf("AddAnsibleRunHost failed: %v", err)
	}
	if err := s.AddAnsibleRunHost(runID, "host-b"); err != nil {
		t.Fatalf("AddAnsibleRunHost failed: %v", err)
	}

	hosts, err := s.ListAnsibleRunHosts(runID)
	if err != nil {
		t.Fatalf("ListAnsibleRunHosts failed: %v", err)
	}
	if len(hosts) != 2 {
		t.Fatalf("expected 2 hosts, got %d", len(hosts))
	}
	// Ordered by host
	if hosts[0] != "host-a" || hosts[1] != "host-b" {
		t.Errorf("expected host-a, host-b; got %v", hosts)
	}
}

func TestAnsibleRunHostsIdempotent(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	runID := "run-idempotent"
	s.UpsertAnsibleRun(runID, "deploy", "site.yml", "success", 1000, 2000, 0, "[]", "", "", "", "")

	// Add same host twice (INSERT OR IGNORE)
	s.AddAnsibleRunHost(runID, "host-a")
	s.AddAnsibleRunHost(runID, "host-a")

	hosts, err := s.ListAnsibleRunHosts(runID)
	if err != nil {
		t.Fatalf("ListAnsibleRunHosts failed: %v", err)
	}
	if len(hosts) != 1 {
		t.Errorf("expected 1 host (idempotent), got %d", len(hosts))
	}
}

// ============================================================
// Legacy ansible_computers tests
// ============================================================

func TestAnsibleComputersLegacy(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	// Skip if legacy table doesn't exist (dropped by migration 008)
	var exists int
	_ = s.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='ansible_computers'`).Scan(&exists)
	if exists == 0 {
		t.Skip("ansible_computers table dropped by migration 008")
	}

	// Upsert
	if err := s.UpsertAnsibleComputer("legacy-host", `{"host":"legacy-host","enabled":true}`); err != nil {
		t.Fatalf("UpsertAnsibleComputer failed: %v", err)
	}

	// Get
	raw, err := s.GetAnsibleComputer("legacy-host")
	if err != nil {
		t.Fatalf("GetAnsibleComputer failed: %v", err)
	}
	if raw == "" {
		t.Fatal("expected non-empty raw JSON")
	}

	// List
	computers, err := s.ListAnsibleComputers()
	if err != nil {
		t.Fatalf("ListAnsibleComputers failed: %v", err)
	}
	if len(computers) != 1 {
		t.Fatalf("expected 1 legacy computer, got %d", len(computers))
	}

	// Delete
	if err := s.DeleteAnsibleComputer("legacy-host"); err != nil {
		t.Fatalf("DeleteAnsibleComputer failed: %v", err)
	}
	raw, err = s.GetAnsibleComputer("legacy-host")
	if err != nil {
		t.Fatalf("GetAnsibleComputer after delete failed: %v", err)
	}
	if raw != "" {
		t.Error("expected empty after delete")
	}
}

// openTestDB is a helper shared across store test files.
func openTestDB(t *testing.T) *SQLiteStore {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "velox_test.db")
	s, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	return s
}
