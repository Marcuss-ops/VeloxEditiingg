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

// CreateRun stores a new run record.
func (m *AnsibleRunManager) CreateRun(run AnsibleRunRecord) error {
	return m.saveRun(run)
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

// UpdateRun mutates an existing run record.
func (m *AnsibleRunManager) UpdateRun(runID string, mut func(*AnsibleRunRecord)) error {
	return m.updateRun(runID, mut)
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

// GetRun returns a stored run by ID.
func (m *AnsibleRunManager) GetRun(runID string) (AnsibleRunRecord, bool) {
	return m.getRun(runID)
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
