package ansible

import (
	"encoding/json"
	"log"
	"path/filepath"
	"sync"
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
// Both the legacy raw_json interface and the new structured interface are provided.
type AnsibleComputerStore interface {
	// Legacy methods (ansible_computers table — raw_json)
	GetAnsibleComputer(host string) (string, error)
	ListAnsibleComputers() (map[string]json.RawMessage, error)
	UpsertAnsibleComputer(host, rawJSON string) error
	DeleteAnsibleComputer(host string) error
	MigrateAnsibleComputersFromJSON(computers map[string]json.RawMessage) (int, error)

	// New structured methods (ansible_hosts table)
	UpsertAnsibleHost(fields store.AnsibleHostFields) error
	DeleteAnsibleHost(host string) error
	GetAnsibleHost(host string) (*store.AnsibleHostFields, error)
	ListAnsibleHosts() ([]store.AnsibleHostFields, error)
}

// AnsibleComputerManager manages the Ansible computers inventory.
type AnsibleComputerManager struct {
	dataDir      string
	store        AnsibleComputerStore
	computers    map[string]AnsibleComputer
	secretResolver *SecretResolver
	mu           sync.RWMutex
}

// NewAnsibleComputerManager creates a new Ansible computer manager.
func NewAnsibleComputerManager(dataDir string) *AnsibleComputerManager {
	secretsDir := filepath.Join(dataDir, "secrets", "ansible")
	return &AnsibleComputerManager{
		dataDir:        dataDir,
		computers:      make(map[string]AnsibleComputer),
		secretResolver: NewSecretResolver(secretsDir),
	}
}

// SetStore sets the SQLite store and loads from it.
func (m *AnsibleComputerManager) SetStore(store AnsibleComputerStore) {
	m.store = store
	m.loadFromSQLite()
}

// loadFromSQLite loads computers from SQLite (legacy ansible_computers table).
func (m *AnsibleComputerManager) loadFromSQLite() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.store == nil {
		return
	}

	// Try loading from new structured ansible_hosts first
	hosts, err := m.store.ListAnsibleHosts()
	if err == nil && len(hosts) > 0 {
		for _, h := range hosts {
			computer := ansibleHostFieldsToComputer(h)
			m.computers[computer.Host] = computer
		}
		log.Printf("[OK] Loaded %d Ansible computers from ansible_hosts", len(m.computers))
		return
	}

	// Fallback: load from legacy ansible_computers (raw_json)
	rows, legacyErr := m.store.ListAnsibleComputers()
	if legacyErr != nil || len(rows) == 0 {
		return
	}

	for host, rawJSON := range rows {
		var c AnsibleComputer
		if err := json.Unmarshal(rawJSON, &c); err != nil {
			continue
		}
		m.computers[host] = c
	}

	// Migrate legacy data to new structured table
	log.Printf("[INFO] Migrating %d computers from ansible_computers to ansible_hosts", len(m.computers))
	for _, c := range m.computers {
		if err := m.persistToAnsibleHosts(c); err != nil {
			log.Printf("[WARN] Failed to migrate computer %s to ansible_hosts: %v", c.Host, err)
		}
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
		SSHPassword:      "", // Not stored in plaintext — use secret_ref
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

	// If the computer already has a known secret_ref, use it
	if c.SSHPassword != "" && resolver != nil {
		ref, err := resolver.MigrateSSHPassword(c.Host, c.SSHPassword)
		if err != nil {
			log.Printf("[SECRET] Failed to migrate password for %s: %v", c.Host, err)
		} else {
			secretRef = ref
		}
	}

	// If no password was set, check if a secret file already exists
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

// persistToAnsibleHosts writes to the new structured table.
// SSHPassword is migrated to secret_ref before persisting.
func (m *AnsibleComputerManager) persistToAnsibleHosts(c AnsibleComputer) error {
	if m.store == nil {
		return nil
	}
	return m.store.UpsertAnsibleHost(computerToAnsibleHostFields(c, m.secretResolver))
}

// LoadComputers loads computers from SQLite.
func (m *AnsibleComputerManager) LoadComputers() error {
	if m.store != nil {
		m.loadFromSQLite()
	}
	return nil
}

// ListComputers returns all computers.
func (m *AnsibleComputerManager) ListComputers() map[string]AnsibleComputer {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]AnsibleComputer, len(m.computers))
	for k, v := range m.computers {
		result[k] = v
	}
	return result
}

// GetComputer returns a specific computer by ID.
func (m *AnsibleComputerManager) GetComputer(id string) (AnsibleComputer, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	computer, ok := m.computers[id]
	return computer, ok
}

// SaveComputer saves or updates a computer.
// SQLite (ansible_hosts) is the single source of truth.
// SSHPassword is migrated to a secret file by persistToAnsibleHosts —
// plaintext passwords are never stored in the database.
func (m *AnsibleComputerManager) SaveComputer(computer AnsibleComputer) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	computer.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	m.computers[computer.Host] = computer

	if m.store == nil {
		return nil
	}

	return m.persistToAnsibleHosts(computer)
}

// DeleteComputer deletes a computer.
func (m *AnsibleComputerManager) DeleteComputer(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.computers, id)
	if m.store == nil {
		return nil
	}

	return m.store.DeleteAnsibleHost(id)
}

// Count returns the number of computers.
func (m *AnsibleComputerManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.computers)
}

// CountEnabled returns the number of enabled computers.
func (m *AnsibleComputerManager) CountEnabled() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	count := 0
	for _, c := range m.computers {
		if c.Enabled {
			count++
		}
	}
	return count
}

// GetSecretRef returns the secret_ref for a host (for inventory generation).
// Used by AnsibleRunManager to reference secrets instead of plaintext passwords.
func (m *AnsibleComputerManager) GetSecretRef(host string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, ok := m.computers[host]; ok {
		return m.secretResolver.BuildSecretRef(host)
	}
	return ""
}

// ResolveSecret resolves a secret_ref to the actual secret value.
func (m *AnsibleComputerManager) ResolveSecret(secretRef string) (string, error) {
	return m.secretResolver.Resolve(secretRef)
}
