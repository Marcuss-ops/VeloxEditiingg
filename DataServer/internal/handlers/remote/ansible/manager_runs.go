package ansible

import (
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/exec"
	"sort"
	"sync"

	"velox-server/internal/store"
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

// AnsibleRunStore defines the SQLite operations needed by AnsibleRunManager.
type AnsibleRunStore interface {
	UpsertAnsibleRun(runID, action, playbook, status string, startedAt, endedAt int64, returnCode int, commands, output, preamble, masterURL, masterURLSource string) error
	GetAnsibleRun(runID string) (*store.AnsibleRun, error)
	ListAnsibleRuns(limit int) ([]store.AnsibleRun, error)
	DeleteAnsibleRun(runID string) error
	AddAnsibleRunHost(runID, host string) error
	ListAnsibleRunHosts(runID string) ([]string, error)
}

// ErrExecutorRemoved is the sentinel error surfaced by every run-time
// ansible action entry-point (deploy, update, install, preflight,
// test_ssh) after PR 8 removed the in-process ansible-playbook executor
// and PR 1 deleted the RunPlaybook fake stub. Callers in handlers.go /
// runDeployWorkers return this error synchronously so the HTTP layer can
// surface an operator-action hint. A real executor lands under
// internal/ansible/executor (planned PR) — once wired, this sentinel is
// replaced by the executor's typed errors.
var ErrExecutorRemoved = errors.New(
	"ansible executor removed: no RunPlaybook path available; install a real executor under internal/ansible/executor (operator action required)",
)

// AnsibleRunManager manages playbook executions and run history.
type AnsibleRunManager struct {
	playbookDir string
	dataDir     string
	dbStore     AnsibleRunStore
	computerMgr *AnsibleComputerManager
	mu          sync.RWMutex
	runs        map[string]AnsibleRunRecord
}

// NewAnsibleRunManager creates a new Ansible run manager.
func NewAnsibleRunManager(playbookDir, dataDir string, dbStore ...AnsibleRunStore) *AnsibleRunManager {
	m := &AnsibleRunManager{
		playbookDir: playbookDir,
		dataDir:     dataDir,
		runs:        make(map[string]AnsibleRunRecord),
	}
	if len(dbStore) > 0 {
		m.dbStore = dbStore[0]
	}
	_ = m.loadRuns()
	return m
}

// SetComputerManager injects the shared computer manager for inventory lookups.
func (m *AnsibleRunManager) SetComputerManager(mgr *AnsibleComputerManager) {
	m.computerMgr = mgr
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

	if m.dbStore == nil {
		return nil
	}

	rows, err := m.dbStore.ListAnsibleRuns(500)
	if err != nil {
		return err
	}

	for _, row := range rows {
		run := ansibleRunRecordFromRow(row)
		if hosts, hErr := m.dbStore.ListAnsibleRunHosts(run.ID); hErr == nil {
			run.Hosts = hosts
		}
		m.runs[run.ID] = run
	}
	return nil
}

func ansibleRunRecordFromRow(row store.AnsibleRun) AnsibleRunRecord {
	return AnsibleRunRecord{
		ID:              row.RunID,
		Action:          row.Action,
		Playbook:        row.Playbook,
		Status:          row.Status,
		StartedAt:       row.StartedAt,
		EndedAt:         row.EndedAt,
		ReturnCode:      row.ReturnCode,
		Commands:        row.Commands,
		Output:          row.Output,
		Preamble:        row.Preamble,
		MasterURL:       row.MasterURL,
		MasterURLSource: row.MasterURLSource,
	}
}

func (m *AnsibleRunManager) persistRunToSQLite(run AnsibleRunRecord) error {
	if m.dbStore == nil {
		return nil
	}
	commandsJSON, _ := json.Marshal(run.Commands)
	if err := m.dbStore.UpsertAnsibleRun(
		run.ID, run.Action, run.Playbook, run.Status,
		run.StartedAt, run.EndedAt, run.ReturnCode,
		string(commandsJSON), run.Output, run.Preamble,
		run.MasterURL, run.MasterURLSource,
	); err != nil {
		return err
	}
	// Link hosts to run — log errors but don't fail the whole persist
	for _, host := range run.Hosts {
		if err := m.dbStore.AddAnsibleRunHost(run.ID, host); err != nil {
			log.Printf("[WARN] Failed to link host %s to run %s: %v", host, run.ID[:8], err)
		}
	}
	return nil
}

func (m *AnsibleRunManager) persistRunsLocked() error {
	// SQLite is the single source of truth for run history.
	if m.dbStore == nil {
		return nil
	}
	var firstErr error
	for _, run := range m.runs {
		if err := m.persistRunToSQLite(run); err != nil {
			log.Printf("[WARN] Failed to persist run %s: %v", run.ID[:8], err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
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

// PR 1: AnsibleRunManager.RunPlaybook was a fake-executor method that
// minted a `StatusExecutorMissing` row each time it was called without
// actually invoking ansible-playbook. The HTTP-layer callers now return
// ErrExecutorRemoved synchronously from handlers.go / runDeployWorkers so
// no fake run records are written. PR 1 deletes the method entirely;
// future PR wiring a real executor lands under
// internal/ansible/executor and registers a typed handler here.
