package store

import (
	"encoding/json"
	"time"
)

// ============================================================
// Structured ansible_hosts methods (Migration 004)
// ============================================================

// AnsibleHostFields holds the structured fields for an ansible host.
type AnsibleHostFields struct {
	Host             string
	AnsibleUser      string
	SSHKeyPath       string
	SecretRef        string // reference to external secret instead of plaintext password
	Enabled          bool
	Availability     string
	Group            string
	Subgroup         string
	Tags             []string
	Notes            string
	LinkedWorkerID   string
	WorkerID         string
	LastSeenAt       string
	LastErrorAt      string
	LastErrorMessage string
	LastLinkedAt     string
	LastRunID        string
	LastRunAction    string
	LastRunRC        int
	LastLogLevel     string
	LastLogMessage   string
	LastLogSource    string
	CreatedAt        string
	UpdatedAt        string
}

// UpsertAnsibleHost inserts or updates a structured ansible host.
// SecretRef replaces SSHPassword — passwords should not be stored in plaintext.
func (s *SQLiteStore) UpsertAnsibleHost(fields AnsibleHostFields) error {
	now := time.Now().UTC().Format(time.RFC3339)
	if fields.CreatedAt == "" {
		fields.CreatedAt = now
	}
	fields.UpdatedAt = now

	tagsJSON, _ := json.Marshal(fields.Tags)
	enabled := 0
	if fields.Enabled {
		enabled = 1
	}

	_, err := s.db.Exec(
		`INSERT INTO ansible_hosts (
			host, ansible_user, ssh_key_path, secret_ref, enabled, availability,
			host_group, subgroup, tags_json, notes,
			linked_worker_id, worker_id,
			last_seen_at, last_error_at, last_error_message,
			last_linked_at, last_run_id, last_run_action, last_run_rc,
			last_log_level, last_log_message, last_log_source,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(host) DO UPDATE SET
			ansible_user=excluded.ansible_user,
			ssh_key_path=excluded.ssh_key_path,
			secret_ref=excluded.secret_ref,
			enabled=excluded.enabled,
			availability=excluded.availability,
			host_group=excluded.host_group,
			subgroup=excluded.subgroup,
			tags_json=excluded.tags_json,
			notes=excluded.notes,
			linked_worker_id=excluded.linked_worker_id,
			worker_id=excluded.worker_id,
			last_seen_at=excluded.last_seen_at,
			last_error_at=excluded.last_error_at,
			last_error_message=excluded.last_error_message,
			last_linked_at=excluded.last_linked_at,
			last_run_id=excluded.last_run_id,
			last_run_action=excluded.last_run_action,
			last_run_rc=excluded.last_run_rc,
			last_log_level=excluded.last_log_level,
			last_log_message=excluded.last_log_message,
			last_log_source=excluded.last_log_source,
			updated_at=excluded.updated_at`,
		fields.Host, fields.AnsibleUser, fields.SSHKeyPath, fields.SecretRef, enabled,
		fields.Availability, fields.Group, fields.Subgroup, string(tagsJSON), fields.Notes,
		fields.LinkedWorkerID, fields.WorkerID,
		fields.LastSeenAt, fields.LastErrorAt, fields.LastErrorMessage,
		fields.LastLinkedAt, fields.LastRunID, fields.LastRunAction, fields.LastRunRC,
		fields.LastLogLevel, fields.LastLogMessage, fields.LastLogSource,
		fields.CreatedAt, fields.UpdatedAt,
	)
	return err
}

// DeleteAnsibleHost removes a structured ansible host.
func (s *SQLiteStore) DeleteAnsibleHost(host string) error {
	_, err := s.db.Exec(`DELETE FROM ansible_hosts WHERE host=?`, host)
	return err
}

// GetAnsibleHost returns a structured ansible host by host name.
func (s *SQLiteStore) GetAnsibleHost(host string) (*AnsibleHostFields, error) {
	row := s.db.QueryRow(`SELECT
		host, ansible_user, ssh_key_path, secret_ref, enabled, availability,
		host_group, subgroup, tags_json, notes,
		linked_worker_id, worker_id,
		last_seen_at, last_error_at, last_error_message,
		last_linked_at, last_run_id, last_run_action, last_run_rc,
		last_log_level, last_log_message, last_log_source,
		created_at, updated_at
		FROM ansible_hosts WHERE host=?`, host)

	var h AnsibleHostFields
	var enabled int
	var tagsJSON string
	err := row.Scan(
		&h.Host, &h.AnsibleUser, &h.SSHKeyPath, &h.SecretRef, &enabled, &h.Availability,
		&h.Group, &h.Subgroup, &tagsJSON, &h.Notes,
		&h.LinkedWorkerID, &h.WorkerID,
		&h.LastSeenAt, &h.LastErrorAt, &h.LastErrorMessage,
		&h.LastLinkedAt, &h.LastRunID, &h.LastRunAction, &h.LastRunRC,
		&h.LastLogLevel, &h.LastLogMessage, &h.LastLogSource,
		&h.CreatedAt, &h.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	h.Enabled = enabled == 1
	json.Unmarshal([]byte(tagsJSON), &h.Tags)
	return &h, nil
}

// ListAnsibleHosts returns all structured ansible hosts.
func (s *SQLiteStore) ListAnsibleHosts() ([]AnsibleHostFields, error) {
	rows, err := s.db.Query(`SELECT
		host, ansible_user, ssh_key_path, secret_ref, enabled, availability,
		host_group, subgroup, tags_json, notes,
		linked_worker_id, worker_id,
		last_seen_at, last_error_at, last_error_message,
		last_linked_at, last_run_id, last_run_action, last_run_rc,
		last_log_level, last_log_message, last_log_source,
		created_at, updated_at
		FROM ansible_hosts ORDER BY host`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []AnsibleHostFields
	for rows.Next() {
		var h AnsibleHostFields
		var enabled int
		var tagsJSON string
		if err := rows.Scan(
			&h.Host, &h.AnsibleUser, &h.SSHKeyPath, &h.SecretRef, &enabled, &h.Availability,
			&h.Group, &h.Subgroup, &tagsJSON, &h.Notes,
			&h.LinkedWorkerID, &h.WorkerID,
			&h.LastSeenAt, &h.LastErrorAt, &h.LastErrorMessage,
			&h.LastLinkedAt, &h.LastRunID, &h.LastRunAction, &h.LastRunRC,
			&h.LastLogLevel, &h.LastLogMessage, &h.LastLogSource,
			&h.CreatedAt, &h.UpdatedAt,
		); err != nil {
			continue
		}
		h.Enabled = enabled == 1
		json.Unmarshal([]byte(tagsJSON), &h.Tags)
		result = append(result, h)
	}
	return result, rows.Err()
}

// ============================================================
// Structured ansible_runs methods
// ============================================================

// AnsibleRun is the typed representation of an ansible_runs row.
type AnsibleRun struct {
	RunID           string
	Action          string
	Playbook        string
	Status          string
	StartedAt       int64
	EndedAt         int64
	ReturnCode      int
	Commands        []string
	Output          string
	Preamble        string
	MasterURL       string
	MasterURLSource string
}

// UpsertAnsibleRun inserts or updates a run record.
func (s *SQLiteStore) UpsertAnsibleRun(runID, action, playbook, status string, startedAt, endedAt int64, returnCode int, commands, output, preamble, masterURL, masterURLSource string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(
		`INSERT INTO ansible_runs (run_id, action, playbook, status, started_at, ended_at, return_code, commands_json, output, preamble, master_url, master_url_source, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(run_id) DO UPDATE SET
			action=excluded.action, playbook=excluded.playbook, status=excluded.status,
			started_at=excluded.started_at, ended_at=excluded.ended_at,
			return_code=excluded.return_code, commands_json=excluded.commands_json,
			output=excluded.output, preamble=excluded.preamble,
			master_url=excluded.master_url, master_url_source=excluded.master_url_source`,
		runID, action, playbook, status, startedAt, endedAt, returnCode, commands, output, preamble, masterURL, masterURLSource, now,
	)
	return err
}

// GetAnsibleRun returns a typed run record.
func (s *SQLiteStore) GetAnsibleRun(runID string) (*AnsibleRun, error) {
	row := s.db.QueryRow(`SELECT run_id, action, playbook, status, started_at, ended_at, return_code, commands_json, output, preamble, master_url, master_url_source FROM ansible_runs WHERE run_id=?`, runID)
	var r AnsibleRun
	var commandsJSON string
	if err := row.Scan(&r.RunID, &r.Action, &r.Playbook, &r.Status, &r.StartedAt, &r.EndedAt, &r.ReturnCode, &commandsJSON, &r.Output, &r.Preamble, &r.MasterURL, &r.MasterURLSource); err != nil {
		return nil, err
	}
	json.Unmarshal([]byte(commandsJSON), &r.Commands)
	return &r, nil
}

// ListAnsibleRuns returns all typed run records ordered by started_at
// descending, with run_id as the deterministic tiebreaker when two runs
// collide at the same ms. (start_at, run_id) gives a stable order that
// matches the pre-refactor Go-side sort.Slice, so callers that depended
// on the tiebreaker (Linear UpdateRun, audit endpoint) see no regression.
func (s *SQLiteStore) ListAnsibleRuns(limit int) ([]AnsibleRun, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.db.Query(`SELECT run_id, action, playbook, status, started_at, ended_at, return_code, commands_json, output, preamble, master_url, master_url_source FROM ansible_runs ORDER BY started_at DESC, run_id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []AnsibleRun
	for rows.Next() {
		var r AnsibleRun
		var commandsJSON string
		if err := rows.Scan(&r.RunID, &r.Action, &r.Playbook, &r.Status, &r.StartedAt, &r.EndedAt, &r.ReturnCode, &commandsJSON, &r.Output, &r.Preamble, &r.MasterURL, &r.MasterURLSource); err != nil {
			continue
		}
		json.Unmarshal([]byte(commandsJSON), &r.Commands)
		result = append(result, r)
	}
	return result, rows.Err()
}

// DeleteAnsibleRun removes a run and its host associations (CASCADE).
func (s *SQLiteStore) DeleteAnsibleRun(runID string) error {
	_, err := s.db.Exec(`DELETE FROM ansible_runs WHERE run_id=?`, runID)
	return err
}

// ============================================================
// ansible_run_hosts methods
// ============================================================

// AddAnsibleRunHost associates a host with a run (idempotent).
func (s *SQLiteStore) AddAnsibleRunHost(runID, host string) error {
	_, err := s.db.Exec(`INSERT OR IGNORE INTO ansible_run_hosts (run_id, host) VALUES (?, ?)`, runID, host)
	return err
}

// DeleteAnsibleRunHost removes a host association from a run.
// Linear counterpart to AddAnsibleRunHost; lets persistRunToSQLite
// (or any caller that wants a real host-set write) drop stale entries
// when the closure removes a host.
func (s *SQLiteStore) DeleteAnsibleRunHost(runID, host string) error {
	_, err := s.db.Exec(`DELETE FROM ansible_run_hosts WHERE run_id=? AND host=?`, runID, host)
	return err
}

// Compile-time assertions: *SQLiteStore satisfies both manager store
// contract surfaces for the remote/ansible package.
//
// NOTE: We redeclare the contract interfaces here instead of importing
// them from velox-server/internal/handlers/remote/ansible because the
// store package is a dependency of that handler package — importing it
// back would create a cycle. Asserting against local interfaces keeps
// the gate honest: any method drop on either side fails this build
// instead of failing at runtime in app/ansible.go.
//
// The interface bodies MUST stay in lock-step with the corresponding
// declarations in internal/handlers/remote/ansible/manager_computers.go
// (AnsibleComputerStore) and manager_runs.go (AnsibleRunStore).
type (
	ansibleComputerStoreContract interface {
		UpsertAnsibleHost(fields AnsibleHostFields) error
		DeleteAnsibleHost(host string) error
		GetAnsibleHost(host string) (*AnsibleHostFields, error)
		ListAnsibleHosts() ([]AnsibleHostFields, error)
		CountAnsibleHosts() (int, error)
		CountAnsibleHostsEnabled() (int, error)
	}
	ansibleRunStoreContract interface {
		UpsertAnsibleRun(runID, action, playbook, status string, startedAt, endedAt int64, returnCode int, commands, output, preamble, masterURL, masterURLSource string) error
		GetAnsibleRun(runID string) (*AnsibleRun, error)
		ListAnsibleRuns(limit int) ([]AnsibleRun, error)
		DeleteAnsibleRun(runID string) error
		AddAnsibleRunHost(runID, host string) error
		DeleteAnsibleRunHost(runID, host string) error
		ListAnsibleRunHosts(runID string) ([]string, error)
	}
)

var (
	_ ansibleComputerStoreContract = (*SQLiteStore)(nil)
	_ ansibleRunStoreContract      = (*SQLiteStore)(nil)
)

// ListAnsibleRunHosts returns all hosts for a run.
func (s *SQLiteStore) ListAnsibleRunHosts(runID string) ([]string, error) {
	rows, err := s.db.Query(`SELECT host FROM ansible_run_hosts WHERE run_id=? ORDER BY host`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var hosts []string
	for rows.Next() {
		var host string
		if err := rows.Scan(&host); err != nil {
			continue
		}
		hosts = append(hosts, host)
	}
	return hosts, nil
}

// CountAnsibleHosts returns the total number of structured ansible hosts.
// Linear SQL aggregate — keeps the manager's Count() O(1) at the Go layer
// instead of streaming all rows into RAM.
func (s *SQLiteStore) CountAnsibleHosts() (int, error) {
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM ansible_hosts`).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// CountAnsibleHostsEnabled returns the number of enabled hosts only.
// Linear SQL aggregate with WHERE predicate.
func (s *SQLiteStore) CountAnsibleHostsEnabled() (int, error) {
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM ansible_hosts WHERE enabled=1`).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}
