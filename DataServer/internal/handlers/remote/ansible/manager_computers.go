package ansible

import (
	"log"
	"path/filepath"
	"time"

	"velox-server/internal/store"
)

// AnsibleComputer represents a computer in the Ansible inventory.
type AnsibleComputer struct {
	Host             string   `json:"host"`
	AnsibleUser      string   `json:"ansible_user"`
	SSHPassword      string   `json:"ssh_password,omitempty"`
	SSHKeyPath       string   `json:"ssh_key_path,omitempty"`
	Enabled          bool     `json:"enabled"`
	Availability     string   `json:"availability"`
	Group            string   `json:"group"`
	Subgroup         string   `json:"subgroup"`
	Tags             []string `json:"tags"`
	Notes            string   `json:"notes"`
	CreatedAt        string   `json:"created_at"`
	UpdatedAt        string   `json:"updated_at"`
	LastSeenAt       string   `json:"last_seen_at"`
	LastErrorAt      string   `json:"last_error_at,omitempty"`
	LastLinkedAt     string   `json:"last_linked_at,omitempty"`
	LastRunID        string   `json:"last_run_id,omitempty"`
	LastRunAction    string   `json:"last_run_action,omitempty"`
	LastRunRC        int      `json:"last_run_rc,omitempty"`
	LastLogLevel     string   `json:"last_log_level,omitempty"`
	LastLogMessage   string   `json:"last_log_message,omitempty"`
	LastLogSource    string   `json:"last_log_source,omitempty"`
	LastErrorMessage string   `json:"last_error_message,omitempty"`
	LinkedWorkerID   string   `json:"linked_worker_id,omitempty"`
	WorkerID         string   `json:"worker_id,omitempty"`
}

// AnsibleComputerStore defines the SQLite operations for ansible computers.
type AnsibleComputerStore interface {
	UpsertAnsibleHost(fields store.AnsibleHostFields) error
	DeleteAnsibleHost(host string) error
	GetAnsibleHost(host string) (*store.AnsibleHostFields, error)
	ListAnsibleHosts() ([]store.AnsibleHostFields, error)
	CountAnsibleHosts() (int, error)
	CountAnsibleHostsEnabled() (int, error)
}

// AnsibleComputerManager owns the Ansible computers inventory.
//
// PR-ANSIBLE-SOT: the previous in-RAM `computers map[string]AnsibleComputer`
// mirror is REMOVED. SQLite (`ansible_hosts`) is the single source of
// truth — every read (`GetComputer`, `ListComputers`, `Count`,
// `CountEnabled`, `GetSecretRef`) hits the store on every call. The
// bootstrap-time `loadFromSQLite` + `SetStore` are gone; the store is
// mandatory at construction. Linear DB roundtrips replace the O(N)
// in-RAM loops that the mirror allowed.
type AnsibleComputerManager struct {
	dataDir        string
	store          AnsibleComputerStore
	secretResolver *SecretResolver
}

// NewAnsibleComputerManager creates a new computer manager.
//
// store is required: passing nil returns a no-op manager whose reads
// return the zero-value / empty result so the test-mode contract is
// preserved (test fixtures without a backing store still construct
// without panic and report "no computers" / zero counts).
//
// PR-ANSIBLE-SOT: the previous `(dataDir string)` only signature is
// replaced because `SetStore`-then-`loadFromSQLite` was eliminated —
// the store is wired once at construction. Callers in app/ansible.go
// pass `m.store` directly.
func NewAnsibleComputerManager(dataDir string, store AnsibleComputerStore) *AnsibleComputerManager {
	secretsDir := filepath.Join(dataDir, "secrets", "ansible")
	return &AnsibleComputerManager{
		dataDir:        dataDir,
		store:          store,
		secretResolver: NewSecretResolver(secretsDir),
	}
}

// ansibleHostFieldsToComputer converts structured fields to AnsibleComputer.
// The secret_ref is used to check if a password was stored — if the secret file
// exists, BuildSecretRef will return the ref and we know auth is configured.
func ansibleHostFieldsToComputer(h store.AnsibleHostFields) AnsibleComputer {
	return AnsibleComputer{
		Host:             h.Host,
		AnsibleUser:      h.AnsibleUser,
		SSHKeyPath:       h.SSHKeyPath,
		SSHPassword:      "",
		Enabled:          h.Enabled,
		Availability:     h.Availability,
		Group:            h.Group,
		Subgroup:         h.Subgroup,
		Tags:             h.Tags,
		Notes:            h.Notes,
		LinkedWorkerID:   h.LinkedWorkerID,
		WorkerID:         h.WorkerID,
		LastSeenAt:       h.LastSeenAt,
		LastErrorAt:      h.LastErrorAt,
		LastErrorMessage: h.LastErrorMessage,
		LastLinkedAt:     h.LastLinkedAt,
		LastRunID:        h.LastRunID,
		LastRunAction:    h.LastRunAction,
		LastRunRC:        h.LastRunRC,
		LastLogLevel:     h.LastLogLevel,
		LastLogMessage:   h.LastLogMessage,
		LastLogSource:    h.LastLogSource,
		CreatedAt:        h.CreatedAt,
		UpdatedAt:        h.UpdatedAt,
	}
}

// computerToAnsibleHostFields converts AnsibleComputer to structured fields.
// If c.SSHPassword is set, it is migrated to a secret file and the resulting
// secret_ref is persisted. Plaintext passwords are never stored in the database.
func computerToAnsibleHostFields(c AnsibleComputer, resolver *SecretResolver) store.AnsibleHostFields {
	secretRef := ""

	if c.SSHPassword != "" && resolver != nil {
		ref, err := resolver.MigrateSSHPassword(c.Host, c.SSHPassword)
		if err != nil {
			log.Printf("[SECRET] Failed to migrate password for %s: %v", c.Host, err)
		} else {
			secretRef = ref
		}
	}

	if secretRef == "" && resolver != nil {
		secretRef = resolver.BuildSecretRef(c.Host)
	}

	return store.AnsibleHostFields{
		Host:             c.Host,
		AnsibleUser:      c.AnsibleUser,
		SSHKeyPath:       c.SSHKeyPath,
		SecretRef:        secretRef,
		Enabled:          c.Enabled,
		Availability:     c.Availability,
		Group:            c.Group,
		Subgroup:         c.Subgroup,
		Tags:             c.Tags,
		Notes:            c.Notes,
		LinkedWorkerID:   c.LinkedWorkerID,
		WorkerID:         c.WorkerID,
		LastSeenAt:       c.LastSeenAt,
		LastErrorAt:      c.LastErrorAt,
		LastErrorMessage: c.LastErrorMessage,
		LastLinkedAt:     c.LastLinkedAt,
		LastRunID:        c.LastRunID,
		LastRunAction:    c.LastRunAction,
		LastRunRC:        c.LastRunRC,
		LastLogLevel:     c.LastLogLevel,
		LastLogMessage:   c.LastLogMessage,
		LastLogSource:    c.LastLogSource,
		CreatedAt:        c.CreatedAt,
		UpdatedAt:        c.UpdatedAt,
	}
}

// ListComputers returns the full inventory as a fresh SQLite read.
//
// PR-ANSIBLE-SOT: every read goes through the `ListAnsibleHosts()` SQL
// query — no in-RAM mirror, no SetStore+loadFromSQLite bootstrap. With
// a nil store the method still produces an empty map so callers
// iterating `for _, c := range m.ListComputers()` stay panic-free.
func (m *AnsibleComputerManager) ListComputers() map[string]AnsibleComputer {
	result := map[string]AnsibleComputer{}
	if m.store == nil {
		return result
	}
	hosts, err := m.store.ListAnsibleHosts()
	if err != nil {
		log.Printf("[ANSIBLE] ListComputers: ListAnsibleHosts: %v", err)
		return result
	}
	for _, h := range hosts {
		result[h.Host] = ansibleHostFieldsToComputer(h)
	}
	return result
}

// GetComputer returns a specific computer by host name from SQLite.
//
// PR-ANSIBLE-SOT: a single `GetAnsibleHost` query replaces the in-RAM
// map lookup. With a nil store the method returns the zero AnsibleComputer
// and `false` so callers retain their pre-existing panic-free path.
func (m *AnsibleComputerManager) GetComputer(id string) (AnsibleComputer, bool) {
	if m.store == nil {
		return AnsibleComputer{}, false
	}
	h, err := m.store.GetAnsibleHost(id)
	if err != nil {
		// sql.ErrNoRows → ok=false, zero value; other errors logged but
		// also surface ok=false so the audit endpoint degrades gracefully
		// rather than mis-reporting stale data.
		return AnsibleComputer{}, false
	}
	if h == nil {
		return AnsibleComputer{}, false
	}
	return ansibleHostFieldsToComputer(*h), true
}

// SaveComputer upserts a computer in SQLite.
//
// PR-ANSIBLE-SOT: the in-RAM `m.computers[host] = computer` assignment
// is replaced by a single `UpsertAnsibleHost` call. SSHPassword is
// migrated to a secret file by `persistToAnsibleHosts` so plaintext
// passwords never reach the database.
func (m *AnsibleComputerManager) SaveComputer(computer AnsibleComputer) error {
	if m.store == nil {
		return nil
	}
	computer.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	return m.store.UpsertAnsibleHost(computerToAnsibleHostFields(computer, m.secretResolver))
}

// DeleteComputer removes a computer from SQLite.
//
// PR-ANSIBLE-SOT: the in-RAM `delete(m.computers, id)` is replaced by a
// single `DeleteAnsibleHost` call.
func (m *AnsibleComputerManager) DeleteComputer(id string) error {
	if m.store == nil {
		return nil
	}
	return m.store.DeleteAnsibleHost(id)
}

// Count returns the total number of computers via a SQL `COUNT(*)` query.
//
// PR-ANSIBLE-SOT: replaces the O(N) in-RAM `len(m.computers)` with a
// constant-cost aggregate. With a nil store the manager reports 0.
func (m *AnsibleComputerManager) Count() int {
	if m.store == nil {
		return 0
	}
	n, err := m.store.CountAnsibleHosts()
	if err != nil {
		log.Printf("[ANSIBLE] CountAnsibleHosts: %v", err)
		return 0
	}
	return n
}

// CountEnabled returns the number of enabled computers via a SQL
// `COUNT(*) WHERE enabled=1` query.
//
// PR-ANSIBLE-SOT: replaces the O(N) in-RAM `if c.Enabled { count++ }` with
// a constant-cost aggregate. With a nil store the manager reports 0.
func (m *AnsibleComputerManager) CountEnabled() int {
	if m.store == nil {
		return 0
	}
	n, err := m.store.CountAnsibleHostsEnabled()
	if err != nil {
		log.Printf("[ANSIBLE] CountAnsibleHostsEnabled: %v", err)
		return 0
	}
	return n
}

// GetSecretRef returns the secret_ref for a host (for inventory generation).
// Used by AnsibleRunManager to reference secrets instead of plaintext passwords.
//
// PR-ANSIBLE-SOT: the in-RAM `m.computers[host]` existence check is
// replaced by a single `GetAnsibleHost` query — host existence is
// validated against SQLite before the secret_ref is constructed.
func (m *AnsibleComputerManager) GetSecretRef(host string) string {
	if m.store == nil {
		return ""
	}
	if _, err := m.store.GetAnsibleHost(host); err != nil {
		return ""
	}
	return m.secretResolver.BuildSecretRef(host)
}

// ResolveSecret resolves a secret_ref to the actual secret value.
// Pure helper; doesn't hit the store.
func (m *AnsibleComputerManager) ResolveSecret(secretRef string) (string, error) {
	return m.secretResolver.Resolve(secretRef)
}
