package ansible

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
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

// AnsibleRunRecord stores the execution state for a playbook or ad-hoc command.
type AnsibleRunRecord struct {
	ID              string   `json:"id"`
	Action          string   `json:"action"`
	Playbook        string   `json:"playbook,omitempty"`
	Hosts           []string `json:"hosts"`
	Commands        []string `json:"commands"`
	Status          string   `json:"status"`
	StartedAt       int64    `json:"started_at"`
	EndedAt         int64    `json:"ended_at,omitempty"`
	ReturnCode      int      `json:"returncode,omitempty"`
	Output          string   `json:"output,omitempty"`
	Preamble        string   `json:"preamble,omitempty"`
	MasterURL       string   `json:"master_url,omitempty"`
	MasterURLSource string   `json:"master_url_source,omitempty"`
}

// AnsibleRunManager manages playbook executions and run history.
type AnsibleRunManager struct {
	playbookDir string
	dataDir     string
	runsFile    string
	mu          sync.RWMutex
	runs        map[string]AnsibleRunRecord
}

// NewAnsibleRunManager creates a new Ansible run manager.
func NewAnsibleRunManager(playbookDir, dataDir string) *AnsibleRunManager {
	m := &AnsibleRunManager{
		playbookDir: playbookDir,
		dataDir:     dataDir,
		runsFile:    filepath.Join(dataDir, "ansible_runs.json"),
		runs:        make(map[string]AnsibleRunRecord),
	}
	_ = m.loadRuns()
	return m
}

func (m *AnsibleRunManager) PlaybookDir() string {
	return m.playbookDir
}

func (m *AnsibleRunManager) Ready() bool {
	if _, err := exec.LookPath("ansible-playbook"); err != nil {
		return false
	}
	if m == nil || m.playbookDir == "" {
		return false
	}
	if _, err := os.Stat(m.playbookDir); err != nil {
		return false
	}
	return true
}

func (m *AnsibleRunManager) loadRuns() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	raw, err := os.ReadFile(m.runsFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var runs map[string]AnsibleRunRecord
	if err := json.Unmarshal(raw, &runs); err != nil {
		return err
	}
	m.runs = runs
	return nil
}

func (m *AnsibleRunManager) persistRunsLocked() error {
	if err := os.MkdirAll(filepath.Dir(m.runsFile), 0755); err != nil {
		return err
	}
	tmpFile, err := os.CreateTemp(filepath.Dir(m.runsFile), "ansible_runs_*.json")
	if err != nil {
		return err
	}
	defer os.Remove(tmpFile.Name())

	enc := json.NewEncoder(tmpFile)
	enc.SetIndent("", "  ")
	if err := enc.Encode(m.runs); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}
	return os.Rename(tmpFile.Name(), m.runsFile)
}

func (m *AnsibleRunManager) saveRun(run AnsibleRunRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runs[run.ID] = run
	return m.persistRunsLocked()
}

func (m *AnsibleRunManager) updateRun(runID string, mut func(*AnsibleRunRecord)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	run, ok := m.runs[runID]
	if !ok {
		return errors.New("run not found")
	}
	mut(&run)
	m.runs[runID] = run
	return m.persistRunsLocked()
}

// ListRuns returns all runs ordered by most recent first.
func (m *AnsibleRunManager) ListRuns() []AnsibleRunRecord {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]AnsibleRunRecord, 0, len(m.runs))
	for _, run := range m.runs {
		out = append(out, run)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].StartedAt == out[j].StartedAt {
			return out[i].ID > out[j].ID
		}
		return out[i].StartedAt > out[j].StartedAt
	})
	return out
}

// GetRunStatus returns the status for a run.
func (m *AnsibleRunManager) GetRunStatus(runID string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	run, ok := m.runs[runID]
	if !ok {
		return "", errors.New("run not found")
	}
	if run.Status == "" {
		return "unknown", nil
	}
	if run.Status == "ok" {
		return "completed", nil
	}
	return run.Status, nil
}

func (m *AnsibleRunManager) getRun(runID string) (AnsibleRunRecord, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	run, ok := m.runs[runID]
	return run, ok
}

func splitRequestedHosts(hosts string) []string {
	parts := strings.FieldsFunc(hosts, func(r rune) bool {
		return r == ',' || r == '\n' || r == '\r' || r == ' ' || r == '\t'
	})
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func sanitizeInventoryAlias(v string) string {
	replacer := strings.NewReplacer(".", "_", "-", "_", ":", "_", " ", "_", "/", "_")
	return "host_" + replacer.Replace(strings.TrimSpace(v))
}

func buildExtraVars(vars map[string]interface{}) []string {
	if len(vars) == 0 {
		return nil
	}
	keys := make([]string, 0, len(vars))
	for k := range vars {
		if k == "inventory_path" || k == "inventory_file" || k == "inventory" {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]string, 0, len(keys))
	for _, key := range keys {
		switch v := vars[key].(type) {
		case string:
			out = append(out, fmt.Sprintf("%s=%s", key, v))
		case bool:
			out = append(out, fmt.Sprintf("%s=%t", key, v))
		default:
			raw, _ := json.Marshal(v)
			out = append(out, fmt.Sprintf("%s=%s", key, string(raw)))
		}
	}
	return out
}

func quoteShell(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

func (m *AnsibleRunManager) loadComputerInventory(hosts []string) (map[string]AnsibleComputer, map[string]string, error) {
	manager := NewAnsibleComputerManager(m.dataDir)
	if err := manager.LoadComputers(); err != nil {
		return nil, nil, err
	}

	allComputers := manager.ListComputers()
	aliasByTarget := make(map[string]string, len(hosts))
	selected := make(map[string]AnsibleComputer, len(hosts))

	for _, host := range hosts {
		if host == "" {
			continue
		}

		if computer, ok := allComputers[host]; ok {
			aliasByTarget[host] = sanitizeInventoryAlias(host)
			selected[host] = computer
			continue
		}

		found := false
		for id, computer := range allComputers {
			if strings.EqualFold(id, host) || strings.EqualFold(computer.Host, host) {
				aliasByTarget[host] = sanitizeInventoryAlias(id)
				selected[host] = computer
				found = true
				break
			}
		}
		if found {
			continue
		}

		aliasByTarget[host] = sanitizeInventoryAlias(host)
		selected[host] = AnsibleComputer{
			Host:         host,
			AnsibleUser:  "pierone",
			Enabled:      true,
			Availability: "UNKNOWN",
		}
	}

	return selected, aliasByTarget, nil
}

func (m *AnsibleRunManager) writeInventoryFile(hosts []string) (string, map[string]string, error) {
	selected, aliasByTarget, err := m.loadComputerInventory(hosts)
	if err != nil {
		return "", nil, err
	}

	tmpFile, err := os.CreateTemp("", fmt.Sprintf("inventory_%d_*.yml", time.Now().UnixNano()))
	if err != nil {
		return "", nil, err
	}

	lines := []string{
		"all:",
		"  children:",
		"    workers:",
		"      hosts:",
	}
	for _, host := range hosts {
		c, ok := selected[host]
		if !ok {
			continue
		}

		alias := aliasByTarget[host]
		if alias == "" {
			alias = sanitizeInventoryAlias(host)
		}

		lines = append(lines, fmt.Sprintf("        %s:", alias))
		lines = append(lines, fmt.Sprintf("          ansible_host: %s", c.Host))
		lines = append(lines, fmt.Sprintf("          ansible_user: %s", firstNonEmpty(c.AnsibleUser, "pierone")))
		lines = append(lines, "          ansible_connection: ssh")
		lines = append(lines, "          ansible_ssh_common_args: '-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null'")
		lines = append(lines, "          ansible_python_interpreter: /usr/bin/python3")
		if c.SSHPassword != "" {
			lines = append(lines, fmt.Sprintf("          ansible_password: %s", c.SSHPassword))
			lines = append(lines, fmt.Sprintf("          ansible_ssh_pass: %s", c.SSHPassword))
		}
		if c.SSHKeyPath != "" {
			lines = append(lines, fmt.Sprintf("          ansible_ssh_private_key_file: %s", c.SSHKeyPath))
		}
		if c.Enabled {
			lines = append(lines, "          ansible_become: true", "          ansible_become_method: sudo")
			if c.SSHPassword != "" {
				lines = append(lines, fmt.Sprintf("          ansible_become_password: %s", c.SSHPassword))
			}
		}
	}

	if _, err := tmpFile.WriteString(strings.Join(lines, "\n") + "\n"); err != nil {
		_ = tmpFile.Close()
		return "", nil, err
	}

	if err := tmpFile.Close(); err != nil {
		return "", nil, err
	}

	return tmpFile.Name(), aliasByTarget, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func (m *AnsibleRunManager) buildCommand(playbookPath, inventoryPath string, limitAliases []string, extraVars []string) []string {
	args := []string{
		"-i", inventoryPath,
		"--forks", "50",
		playbookPath,
	}
	if len(limitAliases) > 0 {
		args = append(args, "--limit", strings.Join(limitAliases, ","))
	}
	if len(extraVars) > 0 {
		args = append(args, "-e", strings.Join(extraVars, " "))
	}
	return args
}

func (m *AnsibleRunManager) runAsync(runID string, inventoryPath string, command []string, commandDisplay string, preamble string) {
	go func() {
		started := time.Now().Unix()
		_ = m.updateRun(runID, func(run *AnsibleRunRecord) {
			run.Status = "running"
			if run.StartedAt == 0 {
				run.StartedAt = started
			}
			run.Preamble = preamble
			run.Commands = []string{commandDisplay}
		})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cmd := exec.CommandContext(ctx, "ansible-playbook", command...)
		cmd.Env = append(os.Environ(),
			"ANSIBLE_HOST_KEY_CHECKING=False",
			"ANSIBLE_STDOUT_CALLBACK=default",
		)

		output, err := cmd.CombinedOutput()
		returnCode := 0
		if err != nil {
			returnCode = 1
			if exitErr, ok := err.(*exec.ExitError); ok {
				returnCode = exitErr.ExitCode()
			}
		}

		status := "ok"
		if returnCode != 0 {
			status = "failed"
		}

		_ = m.updateRun(runID, func(run *AnsibleRunRecord) {
			run.Status = status
			run.EndedAt = time.Now().Unix()
			run.ReturnCode = returnCode
			run.Output = string(output)
		})

		_ = os.Remove(inventoryPath)
	}()
}

// RunPlaybook executes an Ansible playbook on one or more target hosts.
func (m *AnsibleRunManager) RunPlaybook(ctx context.Context, host, playbook string, vars map[string]interface{}) (string, error) {
	if m == nil {
		return "", errors.New("ansible run manager unavailable")
	}
	if _, err := exec.LookPath("ansible-playbook"); err != nil {
		return "", err
	}

	hosts := splitRequestedHosts(host)
	if len(hosts) == 0 {
		return "", errors.New("host required")
	}

	playbookPath := playbook
	if !filepath.IsAbs(playbookPath) {
		playbookPath = filepath.Join(m.playbookDir, playbook)
	}
	if _, err := os.Stat(playbookPath); err != nil {
		return "", err
	}

	inventoryPath, aliasByTarget, err := m.writeInventoryFile(hosts)
	if err != nil {
		return "", err
	}

	runID := strings.ReplaceAll(uuid.NewString(), "-", "")[:8]
	if len(runID) == 0 {
		runID = fmt.Sprintf("%08x", rand.Uint32())
	}

	limitAliases := make([]string, 0, len(hosts))
	for _, hostName := range hosts {
		if alias := aliasByTarget[hostName]; alias != "" {
			limitAliases = append(limitAliases, alias)
		}
	}

	extraVars := buildExtraVars(vars)
	command := m.buildCommand(playbookPath, inventoryPath, limitAliases, extraVars)
	commandDisplay := fmt.Sprintf("ansible-playbook %s", strings.Join(command, " "))

	record := AnsibleRunRecord{
		ID:        runID,
		Action:    filepath.Base(playbook),
		Playbook:  filepath.Base(playbook),
		Hosts:     hosts,
		Commands:  []string{commandDisplay},
		Status:    "running",
		StartedAt: time.Now().Unix(),
		Preamble: fmt.Sprintf("ansibleDir=%s\nplaybook_path=%s\ncomando=%s\nlimit=%s\nhosts=%s\n",
			m.playbookDir,
			playbookPath,
			commandDisplay,
			strings.Join(limitAliases, ","),
			strings.Join(hosts, ","),
		),
	}
	if v, ok := vars["master_url"].(string); ok && strings.TrimSpace(v) != "" {
		record.MasterURL = v
		record.MasterURLSource = "body"
	}

	if err := m.saveRun(record); err != nil {
		return "", err
	}

	m.runAsync(runID, inventoryPath, command, commandDisplay, record.Preamble)
	return runID, nil
}
