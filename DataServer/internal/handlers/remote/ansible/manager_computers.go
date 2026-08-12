package ansible

import (
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"sort"
	"strings"
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

// ErrComputerStoreNotConfigured identifies an inventory manager that cannot
// persist the canonical ansible_hosts state. A missing store must never look
// like a successful no-op mutation.
var ErrComputerStoreNotConfigured = fmt.Errorf("ansible: computer store not configured")

// AnsibleComputerManager owns the Ansible computers inventory.
//
// ── DB-as-source-of-truth inventory (P0.5) ───────────────────────
//
// The `ansible_hosts` table (migration 004) is the SINGLE source of
// truth for inventory (the legacy static deploy/ansible/inventory.*
// files have been removed — they were non-canonical and MUST NOT be
// used as a deploy input). Every read goes through the SQLite store
// via ListAnsibleHosts / GetAnsibleHost. Inventory generation at
// deploy time is driven by GenerateInventory() which
// builds the INI from DB rows and validates configured SSH auth. A host must
// have either a resolvable secret_ref or an SSHKeyPath; on missing/invalid
// auth the deploy FAILS LOUDLY — there is no silent fallback to a static file.
//
// PR-ANSIBLE-SOT: the previous in-RAM `computers map[string]AnsibleComputer`
// mirror is REMOVED. SQLite (`ansible_hosts`) is the single source of
// truth — every read (`GetComputer`, `ListComputers`, `Count`,
// `CountEnabled`, `GetSecretRef`, `GenerateInventory`) hits the store
// on every call. The bootstrap-time `loadFromSQLite` + `SetStore` are
// gone; the store is mandatory at construction. Linear DB roundtrips
// replace the O(N) in-RAM loops that the mirror allowed.
type AnsibleComputerManager struct {
	dataDir        string
	store          AnsibleComputerStore
	secretResolver *SecretResolver
}

// NewAnsibleComputerManager creates a new computer manager.
//
// The store is mandatory: inventory reads and mutations must never turn a
// missing datastore into an empty successful inventory.
func NewAnsibleComputerManager(dataDir string, store AnsibleComputerStore) (*AnsibleComputerManager, error) {
	if store == nil {
		return nil, ErrComputerStoreNotConfigured
	}
	secretsDir := filepath.Join(dataDir, "secrets", "ansible")
	return &AnsibleComputerManager{
		dataDir:        dataDir,
		store:          store,
		secretResolver: NewSecretResolver(secretsDir),
	}, nil
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

// ListComputers returns the full inventory as a fresh SQLite read. Datastore
// failures are returned; an empty map is only a valid empty inventory.
func (m *AnsibleComputerManager) ListComputers() (map[string]AnsibleComputer, error) {
	if m == nil || m.store == nil {
		return nil, ErrComputerStoreNotConfigured
	}
	hosts, err := m.store.ListAnsibleHosts()
	if err != nil {
		return nil, fmt.Errorf("ansible: list computers: %w", err)
	}
	result := make(map[string]AnsibleComputer, len(hosts))
	for _, h := range hosts {
		result[h.Host] = ansibleHostFieldsToComputer(h)
	}
	return result, nil
}

// GetComputer returns a specific computer by host name from SQLite.
// The boolean is false only when the host does not exist; datastore failures
// are returned separately.
func (m *AnsibleComputerManager) GetComputer(id string) (AnsibleComputer, bool, error) {
	if m == nil || m.store == nil {
		return AnsibleComputer{}, false, ErrComputerStoreNotConfigured
	}
	h, err := m.store.GetAnsibleHost(id)
	if errors.Is(err, store.ErrAnsibleHostNotFound) || (h == nil && err == nil) {
		return AnsibleComputer{}, false, nil
	}
	if err != nil {
		return AnsibleComputer{}, false, fmt.Errorf("ansible: get computer %q: %w", id, err)
	}
	return ansibleHostFieldsToComputer(*h), true, nil
}

// SaveComputer upserts a computer in SQLite.
//
// PR-ANSIBLE-SOT: the in-RAM `m.computers[host] = computer` assignment
// is replaced by a single `UpsertAnsibleHost` call. SSHPassword is
// migrated to a secret file by `persistToAnsibleHosts` so plaintext
// passwords never reach the database.
func (m *AnsibleComputerManager) SaveComputer(computer AnsibleComputer) error {
	if m == nil || m.store == nil {
		return ErrComputerStoreNotConfigured
	}
	computer.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	return m.store.UpsertAnsibleHost(computerToAnsibleHostFields(computer, m.secretResolver))
}

// DeleteComputer removes a computer from SQLite.
//
// PR-ANSIBLE-SOT: the in-RAM `delete(m.computers, id)` is replaced by a
// single `DeleteAnsibleHost` call.
func (m *AnsibleComputerManager) DeleteComputer(id string) error {
	if m == nil || m.store == nil {
		return ErrComputerStoreNotConfigured
	}
	return m.store.DeleteAnsibleHost(id)
}

// Count returns the total number of computers via a SQL `COUNT(*)` query.
func (m *AnsibleComputerManager) Count() (int, error) {
	if m == nil || m.store == nil {
		return 0, ErrComputerStoreNotConfigured
	}
	n, err := m.store.CountAnsibleHosts()
	if err != nil {
		return 0, fmt.Errorf("ansible: count computers: %w", err)
	}
	return n, nil
}

// CountEnabled returns the number of enabled computers via a SQL
// `COUNT(*) WHERE enabled=1` query.
//
// PR-ANSIBLE-SOT: replaces the O(N) in-RAM `if c.Enabled { count++ }` with
// a constant-cost aggregate. With a nil store the manager reports 0.
func (m *AnsibleComputerManager) CountEnabled() (int, error) {
	if m == nil || m.store == nil {
		return 0, ErrComputerStoreNotConfigured
	}
	n, err := m.store.CountAnsibleHostsEnabled()
	if err != nil {
		return 0, fmt.Errorf("ansible: count enabled computers: %w", err)
	}
	return n, nil
}

// GetSecretRef returns the secret_ref for a host (for inventory generation).
// Used by AnsibleRunManager to reference secrets instead of plaintext passwords.
//
// PR-ANSIBLE-SOT: the in-RAM `m.computers[host]` existence check is
// replaced by a single `GetAnsibleHost` query — host existence is
// validated against SQLite before the secret_ref is constructed.
func (m *AnsibleComputerManager) GetSecretRef(host string) (string, error) {
	if m == nil || m.store == nil {
		return "", ErrComputerStoreNotConfigured
	}
	if _, err := m.store.GetAnsibleHost(host); err != nil {
		if errors.Is(err, store.ErrAnsibleHostNotFound) {
			return "", nil
		}
		return "", fmt.Errorf("ansible: get computer %q: %w", host, err)
	}
	return m.secretResolver.BuildSecretRef(host), nil
}

// ResolveSecret resolves a secret_ref to the actual secret value.
// Pure helper; doesn't hit the store.
func (m *AnsibleComputerManager) ResolveSecret(secretRef string) (string, error) {
	return m.secretResolver.Resolve(secretRef)
}

// GenerateInventoryOptions configures inventory generation.
type GenerateInventoryOptions struct {
	// IncludeDisabled forces inclusion of disabled rows. Default false:
	// disabled hosts are skipped (no log line, no INI entry). Useful
	// for audit-only flows that want to see the full DB state.
	IncludeDisabled bool
}

// GenerateInventory builds an Ansible INI inventory string from the
// `ansible_hosts` DB rows (the single source of truth — the legacy
// static deploy/ansible/inventory.* files have been removed). This
// method is the only sanctioned way to produce an inventory at deploy
// time.
//
// Per-host contract (enforced in this order, fail-fast on the first
// violation):
//  1. Enabled == false (and !opts.IncludeDisabled) → SKIP silently
//  2. SecretRef == ""                                → fail with
//     `host=<host>: missing secret_ref (DB column secret_ref is
//     NULL/empty); add via /api/v1/ansible/computers PUT`
//  3. secretResolver.Resolve(SecretRef) returns error → fail with
//     `host=<host>: invalid secret_ref=<ref>: <error>`. The error
//     message from the resolver is preserved (e.g., "read secret file
//     /var/lib/velox/secrets/ansible/ssh_host_x: no such file or
//     directory" or "environment variable FOO not set") but the
//     RESOLVED SECRET VALUE itself is NEVER included in the error
//     or in the structured log line.
//
// Per-host structured log (printed BEFORE the INI is emitted, so the
// operator sees the full per-host audit even on partial-failure):
//
//	[ANSIBLE_INV] host=<host> user=<user> unit=<unit> source=db secret_status=ok|missing
//
// `unit` is the canonical systemd unit name derived from the host's
// group: `velox-worker-<host>.service` for groups containing "worker"
// (default for the empty/unknown case), `velox-server.service`
// otherwise. The secret VALUE never appears in any log line; the
// `secret_ref` SCHEME (e.g., "file:ssh_host_x") may.
//
// Returns the INI string with one section per unique host_group. The
// INI is suitable for `ansible-playbook -i <(echo "$INI") ...` or
// for writing to a temp file consumed by the playbook runner.
func (m *AnsibleComputerManager) GenerateInventory(opts GenerateInventoryOptions) (string, error) {
	if m.store == nil {
		return "", ErrComputerStoreNotConfigured
	}
	hosts, err := m.store.ListAnsibleHosts()
	if err != nil {
		return "", fmt.Errorf("list ansible_hosts: %w", err)
	}

	// Per-group section buffers. A stable order (sorted group names,
	// then sorted host names within a group) keeps the INI diff-friendly.
	sections := map[string][]string{}
	groupOrder := []string{}

	for _, h := range hosts {
		if !h.Enabled && !opts.IncludeDisabled {
			continue
		}

		// Compute the effective group FIRST so the unit name and the
		// INI section header agree. An empty DB group falls back to
		// "velox_workers" for both — otherwise the audit log would
		// say "velox-server.service" while the INI emits
		// "[velox_workers]" for the same host, which is confusing.
		group := h.Group
		if group == "" {
			group = "velox_workers"
		}
		unit := canonicalUnitName(h.Host, group)
		secretStatus := "ok"

		// (1) At least one supported SSH auth mechanism must be configured.
		// Key-based auth is the normal production path; secret_ref is only
		// required for password-backed inventory entries.
		if strings.TrimSpace(h.SecretRef) == "" && strings.TrimSpace(h.SSHKeyPath) == "" {
			secretStatus = "missing"
			log.Printf("[ANSIBLE_INV] host=%s user=%s unit=%s source=db secret_ref=%s secret_status=%s",
				h.Host, h.AnsibleUser, unit, h.SecretRef, secretStatus)
			return "", fmt.Errorf("host=%s: missing SSH auth (secret_ref or ssh_key_path)", h.Host)
		}

		// (2) When present, SecretRef must resolve. The resolved password is passed
		// to hostINI as ansible_ssh_pass fallback (it appears ONLY in
		// the temp inventory file, never in any log line).
		sshPass := ""
		if strings.TrimSpace(h.SecretRef) != "" {
			var err error
			sshPass, err = m.secretResolver.Resolve(h.SecretRef)
			if err != nil {
				secretStatus = "missing"
				log.Printf("[ANSIBLE_INV] host=%s user=%s unit=%s source=db secret_ref=%s secret_status=%s",
					h.Host, h.AnsibleUser, unit, h.SecretRef, secretStatus)
				return "", fmt.Errorf("host=%s: invalid secret_ref=%q: %v", h.Host, h.SecretRef, err)
			}
		} else {
			secretStatus = "ssh_key"
		}

		// Success log line. The secret_ref SCHEME appears in the log
		// (e.g., "file:ssh_host_x") for operator audit — that's a
		// reference, not the resolved value, and never reveals the
		// credential itself.
		log.Printf("[ANSIBLE_INV] host=%s user=%s unit=%s source=db secret_ref=%s secret_status=%s",
			h.Host, h.AnsibleUser, unit, h.SecretRef, secretStatus)

		if _, ok := sections[group]; !ok {
			groupOrder = append(groupOrder, group)
		}
		sections[group] = append(sections[group], hostINI(h, sshPass))
	}

	// Stable group order (alphabetical) so the INI is diff-friendly.
	sort.Strings(groupOrder)

	var b strings.Builder
	for _, g := range groupOrder {
		fmt.Fprintf(&b, "[%s]\n", g)
		// Stable host order within a group.
		hostLines := sections[g]
		sort.Strings(hostLines)
		for _, line := range hostLines {
			b.WriteString(line)
			b.WriteString("\n")
		}
		b.WriteString("\n")
	}
	return b.String(), nil
}

// canonicalUnitName maps a host + group to the canonical systemd unit
// the deploy will touch. Worker hosts use `velox-worker-<host>.service`
// (per canonical_worker_runtime.yml line 13). Master / non-worker
// hosts use `velox-server.service`. Group detection is a substring
// match on "worker" so a custom group like "velox_workers_canary"
// still resolves to the worker unit.
func canonicalUnitName(host, group string) string {
	if strings.Contains(strings.ToLower(group), "worker") {
		return "velox-worker-" + host + ".service"
	}
	return "velox-server.service"
}

// hostINI renders one INI host line for the canonical Ansible vars.
// sshPass is resolved from the secret_ref; it is intentionally NOT
// injected as ansible_ssh_pass because sshpass overrides key-based
// auth and breaks passwordless sudo. The temp inventory relies on
// SSH keys (configured on the master) as the primary auth method.
func hostINI(h store.AnsibleHostFields, sshPass string) string {
	_ = sshPass // reserved for future password-only fallback mode
	workerID := h.WorkerID
	if workerID == "" {
		workerID = h.Host
	}
	keyArg := ""
	if strings.TrimSpace(h.SSHKeyPath) != "" {
		keyArg = fmt.Sprintf(" ansible_ssh_private_key_file=%s", h.SSHKeyPath)
	}
	return fmt.Sprintf(
		"%s ansible_host=%s ansible_user=%s ansible_python_interpreter=/usr/bin/python3 worker_id=%s secret_ref=%s%s",
		h.Host, h.Host, h.AnsibleUser, workerID, h.SecretRef, keyArg,
	)
}
