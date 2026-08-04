// Package fleet — worker_registry.go
//
// Unified WorkerRegistry: single source of truth for worker connectivity
// consumed by Fleet Controller, Ansible, fleetctl, smoke runner, update
// executor, and restart executor. Replaces the ad-hoc private map of
// SSHWorkerTarget previously hardcoded in bootstrap_composition.go.
//
// Each entry carries:
//   - worker_id    — canonical id (matches gRPC registration + ansible)
//   - host         — IP or hostname
//   - ssh_user     — SSH username
//   - ssh_port     — SSH port (default 22)
//   - health_port  — HTTP health endpoint port (default 8081)
//   - work_dir     — worker data directory (default /var/lib/velox-worker)
//
// Security invariants:
//   - StrictHostKeyChecking=yes with UserKnownHostsFile
//   - Shell metacharacter validation on worker_id + run_id
//   - Fixed remote script with JSON stdin (never fmt.Sprintf commands)
//
// References:
//   - docs/SECURITY_RUNBOOK.md §SSH hardening
//   - AGENTS.md §1 (typed ports, no silent fallback)
package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
)

// ---------------------------------------------------------------------------
// WorkerRegistryEntry — one worker's connectivity metadata
// ---------------------------------------------------------------------------

// WorkerRegistryEntry holds the connectivity details for a single worker.
type WorkerRegistryEntry struct {
	WorkerID           string // canonical id (e.g. "velox-worker-523925eb")
	Host               string // IP or hostname
	SSHUser            string // SSH username (e.g. debian, ubuntu)
	SSHPort            int    // SSH port (0 means default 22)
	HostKeyFingerprint string // expected host key fingerprint (SHA256:...) — for known_hosts population
	HealthPort         int    // HTTP health endpoint port (0 means default 8081)
	WorkDir            string // worker data directory (empty means /var/lib/velox-worker)
}

// ---------------------------------------------------------------------------
// WorkerRegistry — unified worker inventory
// ---------------------------------------------------------------------------

// WorkerRegistry is the single source of truth for worker connectivity.
// All consumers (Fleet Controller, Ansible, fleetctl, smoke, update, restart)
// read from this registry. Mutations happen at composition time only; the
// registry is read-only at runtime.
type WorkerRegistry struct {
	mu      sync.RWMutex
	entries map[string]WorkerRegistryEntry
}

// NewWorkerRegistry creates an empty registry.
func NewWorkerRegistry() *WorkerRegistry {
	return &WorkerRegistry{
		entries: make(map[string]WorkerRegistryEntry),
	}
}

// AddWorker registers a worker. Returns error on duplicate workerID
// or invalid fields.
func (r *WorkerRegistry) AddWorker(e WorkerRegistryEntry) error {
	if err := validateWorkerID(e.WorkerID); err != nil {
		return fmt.Errorf("worker registry: %w", err)
	}
	if strings.TrimSpace(e.Host) == "" {
		return fmt.Errorf("worker registry: empty host for worker %s", e.WorkerID)
	}
	if strings.TrimSpace(e.SSHUser) == "" {
		return fmt.Errorf("worker registry: empty ssh_user for worker %s", e.WorkerID)
	}
	if e.SSHPort == 0 {
		e.SSHPort = 22
	}
	if e.HealthPort == 0 {
		e.HealthPort = 8081
	}
	if e.WorkDir == "" {
		e.WorkDir = "/var/lib/velox-worker"
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.entries[e.WorkerID]; exists {
		return fmt.Errorf("worker registry: duplicate worker %s", e.WorkerID)
	}
	r.entries[e.WorkerID] = e
	return nil
}

// GetWorker returns the entry for workerID, or nil if not found.
func (r *WorkerRegistry) GetWorker(workerID string) *WorkerRegistryEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.entries[workerID]
	if !ok {
		return nil
	}
	return &e
}

// ListWorkers returns all registered workers.
func (r *WorkerRegistry) ListWorkers() []WorkerRegistryEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]WorkerRegistryEntry, 0, len(r.entries))
	for _, e := range r.entries {
		out = append(out, e)
	}
	return out
}

// Len returns the number of registered workers.
func (r *WorkerRegistry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.entries)
}

// SSHHost returns the user@host string for SSH commands.
func (e WorkerRegistryEntry) SSHHost() string {
	return fmt.Sprintf("%s@%s", e.SSHUser, e.Host)
}

// SSHPortFlag returns the SSH port flag or empty for default.
func (e WorkerRegistryEntry) SSHPortFlag() string {
	if e.SSHPort == 22 || e.SSHPort == 0 {
		return ""
	}
	return fmt.Sprintf("-p %d", e.SSHPort)
}

// ---------------------------------------------------------------------------
// SecureSSHClient — hardened SSH client
// ---------------------------------------------------------------------------

// DefaultSSHKeyPath is the filesystem path to the SSH private key.
const DefaultSSHKeyPath = "/etc/velox/ssh/id_ed25519_velox"

// DefaultKnownHostsPath is the filesystem path to the known_hosts file.
const DefaultKnownHostsPath = "/etc/velox/ssh/known_hosts"

// SecureSSHClient implements BackendSSHClient by shelling out to the
// system ssh binary with StrictHostKeyChecking=yes and a known_hosts file.
type SecureSSHClient struct {
	registry   *WorkerRegistry
	keyPath    string
	knownHosts string
	connectTO  int
}

// NewSecureSSHClient creates a hardened SSH client backed by the
// unified WorkerRegistry.
func NewSecureSSHClient(reg *WorkerRegistry, keyPath, knownHosts string) *SecureSSHClient {
	if keyPath == "" {
		keyPath = os.ExpandEnv("$HOME/.ssh/id_ed25519_velox")
		if keyPath == "" || strings.HasPrefix(keyPath, "$") {
			keyPath = DefaultSSHKeyPath
		}
	}
	if knownHosts == "" {
		knownHosts = DefaultKnownHostsPath
	}
	return &SecureSSHClient{
		registry:   reg,
		keyPath:    keyPath,
		knownHosts: knownHosts,
		connectTO:  10,
	}
}

// Run executes a command on the worker via SSH. The command is validated
// for shell metacharacters; RunJSON is preferred for parameterized commands.
func (c *SecureSSHClient) Run(ctx context.Context, workerID string, command string) (string, error) {
	if err := validateWorkerID(workerID); err != nil {
		return "", fmt.Errorf("ssh: %w", err)
	}
	if err := validateShellCommand(command); err != nil {
		return "", fmt.Errorf("ssh: %w", err)
	}

	e := c.registry.GetWorker(workerID)
	if e == nil {
		return "", fmt.Errorf("ssh: worker %s not in registry", workerID)
	}

	args := c.baseSSHArgs(e)
	args = append(args, command)
	cmd := exec.CommandContext(ctx, "ssh", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("ssh %s@%s: %w (output: %s)", e.SSHUser, e.Host, err, string(out))
	}
	return string(out), nil
}

// RunJSON executes a fixed remote script with JSON-encoded input on stdin.
// Parameters travel as JSON on stdin, eliminating shell injection risk.
// The remote script MUST exist at scriptPath on the worker.
func (c *SecureSSHClient) RunJSON(ctx context.Context, workerID string, scriptPath string, input interface{}) (string, error) {
	if err := validateWorkerID(workerID); err != nil {
		return "", fmt.Errorf("ssh: %w", err)
	}
	if err := validateScriptPath(scriptPath); err != nil {
		return "", fmt.Errorf("ssh: %w", err)
	}

	e := c.registry.GetWorker(workerID)
	if e == nil {
		return "", fmt.Errorf("ssh: worker %s not in registry", workerID)
	}

	args := c.baseSSHArgs(e)
	args = append(args, scriptPath)

	cmd := exec.CommandContext(ctx, "ssh", args...)
	if input != nil {
		jsonBytes, err := json.Marshal(input)
		if err != nil {
			return "", fmt.Errorf("ssh: json marshal input: %w", err)
		}
		cmd.Stdin = strings.NewReader(string(jsonBytes))
	}

	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("ssh %s@%s script %s: %w (output: %s)",
			e.SSHUser, e.Host, scriptPath, err, string(out))
	}
	return string(out), nil
}

// baseSSHArgs returns the hardened SSH arguments shared by Run and RunJSON.
func (c *SecureSSHClient) baseSSHArgs(e *WorkerRegistryEntry) []string {
	args := []string{
		"-i", c.keyPath,
		"-o", "StrictHostKeyChecking=yes",
		"-o", "UserKnownHostsFile=" + c.knownHosts,
		"-o", fmt.Sprintf("ConnectTimeout=%d", c.connectTO),
		"-o", "BatchMode=yes",
		"-o", "LogLevel=ERROR",
	}
	if e.SSHPort != 0 && e.SSHPort != 22 {
		args = append(args, "-p", fmt.Sprintf("%d", e.SSHPort))
	}
	args = append(args, e.SSHHost())
	return args
}

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

var safeWorkerIDRegex = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$`)
var safeRunIDRegex = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,255}$`)
var safeScriptPathRegex = regexp.MustCompile(`^/(var/lib/velox-worker|usr/local/bin|etc/velox)/[a-zA-Z0-9_/.-]+$`)

// ErrInvalidWorkerID is returned when a worker_id contains shell metacharacters.
var ErrInvalidWorkerID = errors.New("invalid worker_id: contains shell metacharacters")

// ErrInvalidRunID is returned when a run_id contains shell metacharacters.
var ErrInvalidRunID = errors.New("invalid run_id: contains shell metacharacters")

// ErrInvalidShellCommand is returned when a command contains unsafe patterns.
var ErrInvalidShellCommand = errors.New("invalid shell command: contains metacharacters or unsafe patterns")

// ErrInvalidScriptPath is returned when a script path is outside allowed roots.
var ErrInvalidScriptPath = errors.New("invalid script path: must be under /var/lib/velox-worker, /usr/local/bin, or /etc/velox")

func validateWorkerID(id string) error {
	if id == "" {
		return fmt.Errorf("%w: empty", ErrInvalidWorkerID)
	}
	if !safeWorkerIDRegex.MatchString(id) {
		return fmt.Errorf("%w: %q", ErrInvalidWorkerID, id)
	}
	return nil
}

// ValidateRunID rejects run IDs containing shell metacharacters.
func ValidateRunID(id string) error {
	if id == "" {
		return fmt.Errorf("%w: empty", ErrInvalidRunID)
	}
	if !safeRunIDRegex.MatchString(id) {
		return fmt.Errorf("%w: %q", ErrInvalidRunID, id)
	}
	return nil
}

func validateShellCommand(cmd string) error {
	if cmd == "" {
		return fmt.Errorf("%w: empty", ErrInvalidShellCommand)
	}
	dangerous := []string{";", "&&", "||", "`", "$(", "${", "|", ">", "<", "&"}
	for _, d := range dangerous {
		if strings.Contains(cmd, d) {
			return fmt.Errorf("%w: contains %q", ErrInvalidShellCommand, d)
		}
	}
	return nil
}

func validateScriptPath(path string) error {
	if path == "" {
		return fmt.Errorf("%w: empty", ErrInvalidScriptPath)
	}
	if !safeScriptPathRegex.MatchString(path) {
		return fmt.Errorf("%w: %q", ErrInvalidScriptPath, path)
	}
	return nil
}
