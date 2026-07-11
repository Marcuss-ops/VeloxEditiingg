// P0.5 tests for the DB-as-source-of-truth inventory generation
// (manager_computers.go::GenerateInventory). Six tests pin the
// per-host contract: fail-fast on missing/invalid secret_ref, never
// log the resolved secret value, skip disabled hosts, default empty
// host_group to velox_workers. Uses a stub AnsibleComputerStore
// backed by a map so the tests stay fast and isolated (no real
// SQLite, no migration runner needed).
package ansible

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"velox-server/internal/store"
)

// stubAnsibleStore is an in-memory implementation of the
// AnsibleComputerStore interface. It backs ListAnsibleHosts with a
// deterministic alphabetical order (matching the real *SQLiteStore
// `ORDER BY host` clause) so test assertions on INI output don't
// have to do set-equivalence.
type stubAnsibleStore struct {
	mu    sync.Mutex
	hosts map[string]store.AnsibleHostFields
}

func newStubAnsibleStore() *stubAnsibleStore {
	return &stubAnsibleStore{hosts: map[string]store.AnsibleHostFields{}}
}

func (s *stubAnsibleStore) UpsertAnsibleHost(h store.AnsibleHostFields) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hosts[h.Host] = h
	return nil
}

func (s *stubAnsibleStore) DeleteAnsibleHost(host string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.hosts, host)
	return nil
}

func (s *stubAnsibleStore) GetAnsibleHost(host string) (*store.AnsibleHostFields, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.hosts[host]
	if !ok {
		return nil, fmt.Errorf("not found: %s", host)
	}
	return &h, nil
}

func (s *stubAnsibleStore) ListAnsibleHosts() ([]store.AnsibleHostFields, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]store.AnsibleHostFields, 0, len(s.hosts))
	for _, h := range s.hosts {
		out = append(out, h)
	}
	// Stable alphabetical order so INI diffs are deterministic.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1].Host > out[j].Host; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out, nil
}

func (s *stubAnsibleStore) CountAnsibleHosts() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.hosts), nil
}

func (s *stubAnsibleStore) CountAnsibleHostsEnabled() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, h := range s.hosts {
		if h.Enabled {
			n++
		}
	}
	return n, nil
}

// captureLog swaps the default log writer with a buffer for the
// duration of the test and returns the buffer so the caller can
// assert on captured output. Restores the previous writer on cleanup.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(prev) })
	return &buf
}

// newTestManager wires a manager with a fresh stub store, an empty
// secrets dir, and returns the manager + the dir (so the caller can
// pre-populate secret files). The dir is auto-cleaned by t.TempDir.
func newTestManager(t *testing.T) (*AnsibleComputerManager, string) {
	t.Helper()
	secretsDir := t.TempDir()
	store := newStubAnsibleStore()
	// NewSecretResolver reads secretsDir to build refs; the dataDir
	// passed to NewAnsibleComputerManager is irrelevant for these
	// tests because we never call UpsertAnsibleHost with an SSHPassword
	// (the migration path that uses dataDir).
	m := NewAnsibleComputerManager(t.TempDir(), store)
	// NewAnsibleComputerManager creates its own secretsDir under
	// dataDir; we don't use it because we resolve directly via the
	// store's SecretRef. Replace the secretResolver with one pointed
	// at the test's temp dir so file: refs work.
	m.secretResolver = NewSecretResolver(secretsDir)
	return m, secretsDir
}

// writeSecretFile creates a secret file with the given content and
// returns the canonical file: secret_ref string.
func writeSecretFile(t *testing.T, secretsDir, host, content string) string {
	t.Helper()
	name := "ssh_host_" + sanitizeSecretFilename(host)
	path := filepath.Join(secretsDir, name)
	if err := os.WriteFile(path, []byte(content+"\n"), 0600); err != nil {
		t.Fatalf("write secret file: %v", err)
	}
	return "file:" + name
}

// ──────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────

// TestGenerateInventory_HappyPath: two enabled hosts with valid
// secret_refs. INI contains both section headers + both host lines.
// Per-host log line ends in secret_status=ok.
func TestGenerateInventory_HappyPath(t *testing.T) {
	m, secretsDir := newTestManager(t)
	st := m.store.(*stubAnsibleStore)
	_ = st.UpsertAnsibleHost(store.AnsibleHostFields{
		Host: "velox-worker-1", AnsibleUser: "velox-deploy", Enabled: true,
		Group: "velox_workers", SecretRef: writeSecretFile(t, secretsDir, "velox-worker-1", "secret-A"),
	})
	_ = st.UpsertAnsibleHost(store.AnsibleHostFields{
		Host: "velox-worker-2", AnsibleUser: "velox-deploy", Enabled: true,
		Group: "velox_workers", SecretRef: writeSecretFile(t, secretsDir, "velox-worker-2", "secret-B"),
	})

	logBuf := captureLog(t)

	ini, err := m.GenerateInventory(GenerateInventoryOptions{})
	if err != nil {
		t.Fatalf("GenerateInventory: %v", err)
	}
	if !strings.Contains(ini, "[velox_workers]") {
		t.Errorf("INI missing [velox_workers] section:\n%s", ini)
	}
	if !strings.Contains(ini, "velox-worker-1 ") {
		t.Errorf("INI missing velox-worker-1 line:\n%s", ini)
	}
	if !strings.Contains(ini, "velox-worker-2 ") {
		t.Errorf("INI missing velox-worker-2 line:\n%s", ini)
	}
	if !strings.Contains(logBuf.String(), "host=velox-worker-1") {
		t.Errorf("log missing per-host entry for velox-worker-1: %s", logBuf.String())
	}
	if !strings.Contains(logBuf.String(), "secret_status=ok") {
		t.Errorf("log missing secret_status=ok: %s", logBuf.String())
	}
}

// TestGenerateInventory_SkipsDisabled: one enabled + one disabled.
// The disabled host is filtered out before log/INI emit, so the
// INI has only the enabled host and the log has only the enabled
// host's line.
func TestGenerateInventory_SkipsDisabled(t *testing.T) {
	m, secretsDir := newTestManager(t)
	st := m.store.(*stubAnsibleStore)
	_ = st.UpsertAnsibleHost(store.AnsibleHostFields{
		Host: "enabled-host", AnsibleUser: "u", Enabled: true,
		Group: "velox_workers", SecretRef: writeSecretFile(t, secretsDir, "enabled-host", "secret"),
	})
	_ = st.UpsertAnsibleHost(store.AnsibleHostFields{
		Host: "disabled-host", AnsibleUser: "u", Enabled: false,
		Group: "velox_workers", SecretRef: writeSecretFile(t, secretsDir, "disabled-host", "secret"),
	})

	logBuf := captureLog(t)

	ini, err := m.GenerateInventory(GenerateInventoryOptions{})
	if err != nil {
		t.Fatalf("GenerateInventory: %v", err)
	}
	if strings.Contains(ini, "disabled-host") {
		t.Errorf("INI should not contain disabled host:\n%s", ini)
	}
	if !strings.Contains(ini, "enabled-host") {
		t.Errorf("INI missing enabled host:\n%s", ini)
	}
	if strings.Contains(logBuf.String(), "disabled-host") {
		t.Errorf("log should not mention disabled host: %s", logBuf.String())
	}
}

// TestGenerateInventory_FailsOnMissingSecretRef: a host with
// SecretRef == "" makes GenerateInventory return an error whose
// message includes "missing secret_ref". The per-host log line
// fires BEFORE the error, with secret_status=missing.
func TestGenerateInventory_FailsOnMissingSecretRef(t *testing.T) {
	m, _ := newTestManager(t)
	st := m.store.(*stubAnsibleStore)
	_ = st.UpsertAnsibleHost(store.AnsibleHostFields{
		Host: "no-secret", AnsibleUser: "u", Enabled: true,
		Group: "velox_workers", SecretRef: "", // empty
	})

	logBuf := captureLog(t)

	_, err := m.GenerateInventory(GenerateInventoryOptions{})
	if err == nil {
		t.Fatalf("expected error on missing secret_ref, got nil")
	}
	if !strings.Contains(err.Error(), "missing secret_ref") {
		t.Errorf("error %q should mention 'missing secret_ref'", err.Error())
	}
	if !strings.Contains(err.Error(), "host=no-secret") {
		t.Errorf("error %q should mention 'host=no-secret'", err.Error())
	}
	if !strings.Contains(logBuf.String(), "host=no-secret") {
		t.Errorf("log missing per-host entry: %s", logBuf.String())
	}
	if !strings.Contains(logBuf.String(), "secret_status=missing") {
		t.Errorf("log should say secret_status=missing: %s", logBuf.String())
	}
}

// TestGenerateInventory_FailsOnInvalidSecretRef: a host whose
// SecretRef points at a non-existent file (or an unknown scheme)
// makes GenerateInventory return an error mentioning the bad ref
// and the resolver's error message — but NEVER the resolved secret
// value (which the resolver never even produces for a missing file).
func TestGenerateInventory_FailsOnInvalidSecretRef(t *testing.T) {
	m, _ := newTestManager(t)
	st := m.store.(*stubAnsibleStore)
	_ = st.UpsertAnsibleHost(store.AnsibleHostFields{
		Host: "bad-ref", AnsibleUser: "u", Enabled: true,
		Group: "velox_workers", SecretRef: "file:ssh_host_nonexistent", // file doesn't exist
	})

	logBuf := captureLog(t)

	_, err := m.GenerateInventory(GenerateInventoryOptions{})
	if err == nil {
		t.Fatalf("expected error on invalid secret_ref, got nil")
	}
	if !strings.Contains(err.Error(), "invalid secret_ref") {
		t.Errorf("error %q should mention 'invalid secret_ref'", err.Error())
	}
	if !strings.Contains(err.Error(), "host=bad-ref") {
		t.Errorf("error %q should mention 'host=bad-ref'", err.Error())
	}
	if !strings.Contains(logBuf.String(), "secret_status=missing") {
		t.Errorf("log should say secret_status=missing: %s", logBuf.String())
	}
}

// TestGenerateInventory_NeverLogsSecretValue: the resolved secret
// value MUST NOT appear in any log line. It IS expected in the INI
// as ansible_ssh_pass (password-fallback for SSH auth). The
// secret_ref SCHEME may appear in logs (e.g., "file:ssh_host_x").
func TestGenerateInventory_NeverLogsSecretValue(t *testing.T) {
	const secretValue = "SUPERSECRET-NEVER-LOG-12345"
	m, secretsDir := newTestManager(t)
	st := m.store.(*stubAnsibleStore)
	_ = st.UpsertAnsibleHost(store.AnsibleHostFields{
		Host: "leaky-host", AnsibleUser: "u", Enabled: true,
		Group: "velox_workers", SecretRef: writeSecretFile(t, secretsDir, "leaky-host", secretValue),
	})

	logBuf := captureLog(t)

	ini, err := m.GenerateInventory(GenerateInventoryOptions{})
	if err != nil {
		t.Fatalf("GenerateInventory: %v", err)
	}

	// MUST NOT appear in any log line (P0.5 contract).
	if strings.Contains(logBuf.String(), secretValue) {
		t.Errorf("RESOLVED SECRET VALUE LEAKED INTO LOG:\n%s", logBuf.String())
	}

	// Sanity: the secret value should NOT appear in INI (password
	// fallback is deliberately excluded to avoid sshpass overriding
	// key-based auth). The secret_ref SCHEME may appear in logs.
	if strings.Contains(ini, secretValue) {
		t.Errorf("RESOLVED SECRET VALUE LEAKED INTO INI:\n%s", ini)
	}

	// Sanity: the secret_ref SCHEME may appear (e.g., "file:ssh_host_leaky-host").
	if !strings.Contains(logBuf.String(), "file:ssh_host_leaky-host") {
		t.Errorf("log should reference the secret_ref scheme (for operator audit), got: %s", logBuf.String())
	}
}

// TestGenerateInventory_DefaultsHostGroup: a host with empty Group
// falls back to "velox_workers" for the INI section header. The
// canonical unit name uses the worker unit (group contains "worker"
// in the fallback name).
func TestGenerateInventory_DefaultsHostGroup(t *testing.T) {
	m, secretsDir := newTestManager(t)
	st := m.store.(*stubAnsibleStore)
	_ = st.UpsertAnsibleHost(store.AnsibleHostFields{
		Host: "defaulted-host", AnsibleUser: "u", Enabled: true,
		Group: "", // empty — should default to velox_workers
		SecretRef: writeSecretFile(t, secretsDir, "defaulted-host", "secret"),
	})

	logBuf := captureLog(t)

	ini, err := m.GenerateInventory(GenerateInventoryOptions{})
	if err != nil {
		t.Fatalf("GenerateInventory: %v", err)
	}
	if !strings.Contains(ini, "[velox_workers]") {
		t.Errorf("INI should default empty group to [velox_workers]:\n%s", ini)
	}
	if !strings.Contains(logBuf.String(), "unit=velox-worker-defaulted-host.service") {
		t.Errorf("log should derive worker unit for defaulted group: %s", logBuf.String())
	}
}
