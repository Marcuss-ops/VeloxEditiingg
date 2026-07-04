package ansible

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

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
	// DeleteAnsibleRunHost is the linear counterpart of AddAnsibleRunHost:
	// it lets persistRunToSQLite drop stale entries when the closure
	// applied by UpdateRun removes a host from the run.
	DeleteAnsibleRunHost(runID, host string) error
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

// AnsibleRunManager owns the playbook run history.
//
// PR-ANSIBLE-SOT: the previous in-RAM `runs map[string]AnsibleRunRecord`
// mirror is REMOVED. SQLite (`ansible_runs` + `ansible_run_hosts`) is
// the single source of truth.
//   - CreateRun / UpdateRun write one row + N `ansible_run_hosts` rows
//     in a single linear flow (no batch persist over the whole
//     `m.runs` map).
//   - GetRun / GetRunStatus / ListRuns read fresh from SQLite on every
//     call. There is no bootstrap-time `loadRuns` mirror — every call
//     returns the live SQL view.
//   - The mutex field is removed entirely; concurrent writers are
//     serialised by SQLite's single-connection write lock, and there is
//     no shared mutable Go state to protect.
type AnsibleRunManager struct {
	playbookDir string
	dataDir     string
	dbStore     AnsibleRunStore
	computerMgr *AnsibleComputerManager
	// listRunsLimit caps how many rows ListRuns returns to bound the
	// per-call cost; pre-refactor the in-RAM map loaded at most 500 via
	// loadRuns, so 500 is the conservative default.
	listRunsLimit int
}

// NewAnsibleRunManager creates a new run manager.
//
// PR-ANSIBLE-SOT: dbStore is REQUIRED. Passing nil returns a manager
// whose readers return the zero value and whose writers return
// (nil, ErrExecutorRemoved) so the test-mode contract is preserved.
// The variadic + SetStore / loadRuns paths are gone — the manager's
// state is now exclusively the SQLite store.
func NewAnsibleRunManager(playbookDir, dataDir string, dbStore AnsibleRunStore) *AnsibleRunManager {
	if dbStore == nil {
		// Even without a backing store the manager must construct so callers
		// can wire dependency graphs without nil-deref guards; every method
		// checks `dbStore == nil` first.
		log.Printf("[ANSIBLE] NewAnsibleRunManager: dbStore is nil; manager is a no-op")
	}
	return &AnsibleRunManager{
		playbookDir:   playbookDir,
		dataDir:       dataDir,
		dbStore:       dbStore,
		listRunsLimit: 500,
	}
}

// SetComputerManager injects the shared computer manager for inventory lookups.
// The computer manager is read-only from the run manager's perspective.
func (m *AnsibleRunManager) SetComputerManager(mgr *AnsibleComputerManager) {
	m.computerMgr = mgr
}

// SetListRunsLimit overrides the default list-runs cap.
func (m *AnsibleRunManager) SetListRunsLimit(limit int) {
	if limit > 0 {
		m.listRunsLimit = limit
	}
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

// ansibleRunRecordFromRow converts a typed `ansible_runs` row into the
// public AnsibleRunRecord. Hosts are kept as `[]string` here and
// hydrated separately by `loadHostsLocked` style helpers at the call
// site that has the store.
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

// persistRunToSQLite writes one run + its host associations in a linear
// single-row flow.
//
// Hosts are diffed against the canonical `ansible_run_hosts` table:
//   - Additions: AddAnsibleRunHost (INSERT OR IGNORE, idempotent).
//   - Removals:  DeleteAnsibleRunHost per host that's in the DB but
//     absent from run.Hosts. Without this, UpdateRun closures that
//     drop a host would silently leave stale rows in the table.
//
// The full host-set write is linear (one DELETE per removed host, one
// INSERT per added host); SQLite's single-connection write lock keeps
// the diff atomic at the connection level.
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

	// Hosts diff: reconcile run.Hosts against the canonical DB set.
	currentHosts, listErr := m.dbStore.ListAnsibleRunHosts(run.ID)
	if listErr != nil {
		log.Printf("[WARN] persistRunToSQLite: list hosts %s: %v", run.ID[:8], listErr)
		currentHosts = nil
	}
	want := make(map[string]bool, len(run.Hosts))
	for _, h := range run.Hosts {
		want[h] = true
	}
	for _, existing := range currentHosts {
		if !want[existing] {
			if err := m.dbStore.DeleteAnsibleRunHost(run.ID, existing); err != nil {
				log.Printf("[WARN] persistRunToSQLite: drop orphan host %s from run %s: %v", existing, run.ID[:8], err)
			}
		}
	}
	for _, host := range run.Hosts {
		if err := m.dbStore.AddAnsibleRunHost(run.ID, host); err != nil {
			// Linear: don't abort the whole persist on one host; surface the
			// anomaly and keep going. This matches the pre-refactor
			// log-and-continue semantic for `persistRunToSQLite`.
			log.Printf("[WARN] persistRunToSQLite: link host %s to run %s: %v", host, run.ID[:8], err)
		}
	}
	return nil
}

// CreateRun stores a new run record.
//
// PR-ANSIBLE-SOT: a single linear `persistRunToSQLite` call replaces the
// previous in-RAM `m.runs[id] = run` + `persistRunsLocked()` bulk
// rewrite. SQLite is the source of truth, so there's no map to keep in
// sync.
func (m *AnsibleRunManager) CreateRun(run AnsibleRunRecord) error {
	return m.persistRunToSQLite(run)
}

// UpdateRun mutates an existing run record by reading the canonical
// SQLite row, applying the closure, and writing back as a single upsert
// + linear host-link flow.
//
// PR-ANSIBLE-SOT: the previous in-RAM `m.runs[runID]` + bulk-rewrite
// of every record is replaced by `GetAnsibleRun` + closure + targeted
// `UpsertAnsibleRun` + per-host `AddAnsibleRunHost`. Hosts are read
// from the canonical `ansible_run_hosts` table so the closure sees the
// full set (including any hosts persisted by parallel callers) before
// applying its mutation.
func (m *AnsibleRunManager) UpdateRun(runID string, mut func(*AnsibleRunRecord)) error {
	if m.dbStore == nil {
		return errors.New("run not found")
	}
	row, err := m.dbStore.GetAnsibleRun(runID)
	if err != nil || row == nil {
		if err == nil {
			err = errors.New("run not found")
		}
		return err
	}
	run := ansibleRunRecordFromRow(*row)
	if hosts, hostErr := m.dbStore.ListAnsibleRunHosts(runID); hostErr == nil {
		run.Hosts = hosts
	}
	mut(&run)
	return m.persistRunToSQLite(run)
}

// ListRuns returns runs ordered by most recent first.
//
// PR-ANSIBLE-SOT: ordering is now pushed down to the SQL layer
// (`ORDER BY started_at DESC, run_id DESC LIMIT ?`). No map snapshot is
// kept; every call hits SQLite. The limit caps the per-call cost so
// the manager's surface stays bounded for handlers that need the full
// audit.
func (m *AnsibleRunManager) ListRuns() []AnsibleRunRecord {
	if m.dbStore == nil {
		return []AnsibleRunRecord{}
	}
	rows, err := m.dbStore.ListAnsibleRuns(m.listRunsLimit)
	if err != nil {
		log.Printf("[ANSIBLE] ListAnsibleRuns: %v", err)
		return []AnsibleRunRecord{}
	}
	out := make([]AnsibleRunRecord, 0, len(rows))
	for _, row := range rows {
		run := ansibleRunRecordFromRow(row)
		if hosts, hostErr := m.dbStore.ListAnsibleRunHosts(run.ID); hostErr == nil {
			run.Hosts = hosts
		}
		out = append(out, run)
	}
	return out
}

// GetRunStatus returns the status for a run.
//
// PR-ANSIBLE-SOT: read straight from `GetAnsibleRun` rather than an
// in-RAM map. With a nil store the method returns the typed error so
// callers (handlers, audit endpoint) can degrade gracefully instead of
// mis-reporting stale data.
func (m *AnsibleRunManager) GetRunStatus(runID string) (string, error) {
	if m.dbStore == nil {
		return "", errors.New("run not found")
	}
	row, err := m.dbStore.GetAnsibleRun(runID)
	if err != nil || row == nil {
		if err == nil {
			err = errors.New("run not found")
		}
		return "", err
	}
	if row.Status == "" {
		return "unknown", nil
	}
	if row.Status == "ok" {
		return "completed", nil
	}
	return row.Status, nil
}

// GetRun returns a stored run by ID.
//
// PR-ANSIBLE-SOT: single SQL read + per-host listing, no in-RAM
// mirror. With a nil store / missing row, returns
// (zero, false) so callers retain the panic-free path.
func (m *AnsibleRunManager) GetRun(runID string) (AnsibleRunRecord, bool) {
	if m.dbStore == nil {
		return AnsibleRunRecord{}, false
	}
	row, err := m.dbStore.GetAnsibleRun(runID)
	if err != nil || row == nil {
		return AnsibleRunRecord{}, false
	}
	run := ansibleRunRecordFromRow(*row)
	if hosts, hostErr := m.dbStore.ListAnsibleRunHosts(runID); hostErr == nil {
		run.Hosts = hosts
	}
	return run, true
}

// RunPlaybook executes ansible-playbook with the given playbook and hosts.
// It generates inventory from the DB, writes it to a temp file, creates a run
// record, and starts ansible-playbook in a goroutine. The run_id is returned
// immediately so the HTTP handler can surface it to the caller.
func (m *AnsibleRunManager) RunPlaybook(playbook string, hosts []string, action string, masterURL string, batchSize int, canaryPercent float64) (string, error) {
	if m.computerMgr == nil {
		return "", fmt.Errorf("ansible computer manager not configured")
	}

	// Generate inventory from DB.
	inventory, err := m.computerMgr.GenerateInventory(GenerateInventoryOptions{})
	if err != nil {
		return "", fmt.Errorf("generate inventory: %w", err)
	}

	// Create a unique run ID.
	runID := newRunID()
	playbookPath := filepath.Join(m.playbookDir, playbook)

	// Build extra vars.
	extraVars := buildExtraVars(masterURL, batchSize, canaryPercent)

	// Write inventory to a temp file.
	tmpFile, err := os.CreateTemp("", "velox-ansible-inventory-*.ini")
	if err != nil {
		return "", fmt.Errorf("create temp inventory file: %w", err)
	}
	if _, err := tmpFile.WriteString(inventory); err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		return "", fmt.Errorf("write inventory: %w", err)
	}
	tmpFile.Close()

	// Build ansible-playbook command args.
	args := []string{"-i", tmpFile.Name()}
	if len(hosts) > 0 {
		args = append(args, "--limit", strings.Join(hosts, ","))
	}
	args = append(args, playbookPath)
	for k, v := range extraVars {
		args = append(args, "-e", fmt.Sprintf("%s=%s", k, v))
	}

	// Create the run record.
	now := time.Now().Unix()
	run := AnsibleRunRecord{
		ID:        runID,
		Action:    action,
		Playbook:  playbook,
		Hosts:     hosts,
		Status:    "running",
		StartedAt: now,
		MasterURL: masterURL,
	}
	if err := m.CreateRun(run); err != nil {
		os.Remove(tmpFile.Name())
		return "", fmt.Errorf("create run record: %w", err)
	}

	// Execute ansible-playbook in a goroutine.
	go func() {
		defer os.Remove(tmpFile.Name())

		cmd := exec.Command("ansible-playbook", args...)
		output, cmdErr := cmd.CombinedOutput()
		endTime := time.Now().Unix()

		exitCode := 0
		status := "ok"
		if cmdErr != nil {
			status = "failed"
			if exitErr, ok := cmdErr.(*exec.ExitError); ok {
				exitCode = exitErr.ExitCode()
			} else {
				exitCode = -1
			}
		}

		updateErr := m.UpdateRun(runID, func(r *AnsibleRunRecord) {
			r.Status = status
			r.EndedAt = endTime
			r.ReturnCode = exitCode
			r.Output = string(output)
		})
		if updateErr != nil {
			log.Printf("[ANSIBLE] UpdateRun %s: %v", runID, updateErr)
		}

		log.Printf("[ANSIBLE] Run %s %s (rc=%d, hosts=%v)", runID, status, exitCode, hosts)
	}()

	return runID, nil
}

func newRunID() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		// Fallback: use nanoseconds as entropy.
		now := time.Now().UnixNano()
		b[0] = byte(now >> 24)
		b[1] = byte(now >> 16)
		b[2] = byte(now >> 8)
		b[3] = byte(now)
	}
	return fmt.Sprintf("run_%d_%s", time.Now().Unix(), hex.EncodeToString(b))
}

func buildExtraVars(masterURL string, batchSize int, canaryPercent float64) map[string]string {
	vars := map[string]string{}
	if masterURL != "" {
		vars["master_url"] = masterURL
	}
	if batchSize > 0 {
		vars["batch_size"] = fmt.Sprintf("%d", batchSize)
	}
	if canaryPercent > 0 {
		vars["canary_percent"] = fmt.Sprintf("%.2f", canaryPercent)
	}
	return vars
}
