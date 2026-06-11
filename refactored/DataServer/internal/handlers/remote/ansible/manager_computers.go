package ansible

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
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
}

// AnsibleComputerManager manages the Ansible computers inventory.
type AnsibleComputerManager struct {
	dataDir   string
	computers map[string]AnsibleComputer
	mu        sync.RWMutex
}

// NewAnsibleComputerManager creates a new Ansible computer manager.
func NewAnsibleComputerManager(dataDir string) *AnsibleComputerManager {
	return &AnsibleComputerManager{
		dataDir:   dataDir,
		computers: make(map[string]AnsibleComputer),
	}
}

// LoadComputers loads computers from ansible_computers.json.
func (m *AnsibleComputerManager) LoadComputers() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	filePath := filepath.Join(m.dataDir, "ansible", "ansible_computers.json")
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var computers map[string]AnsibleComputer
	if err := json.Unmarshal(data, &computers); err != nil {
		return err
	}

	m.computers = computers
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
func (m *AnsibleComputerManager) SaveComputer(computer AnsibleComputer) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.computers[computer.Host] = computer
	return m.saveToFile()
}

// DeleteComputer deletes a computer.
func (m *AnsibleComputerManager) DeleteComputer(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.computers, id)
	return m.saveToFile()
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

// saveToFile saves computers to ansible_computers.json.
func (m *AnsibleComputerManager) saveToFile() error {
	filePath := filepath.Join(m.dataDir, "ansible", "ansible_computers.json")

	data, err := json.MarshalIndent(m.computers, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filePath, data, 0644)
}
