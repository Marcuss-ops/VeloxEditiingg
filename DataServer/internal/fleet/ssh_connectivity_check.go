// Package fleet — SSHConnectivityCheck: drive-by operator diagnostics
// for every worker in the canonical WorkerRegistry.
//
// This is the `fleetctl ssh-check` surface. Its purpose is to answer
// exactly three questions per worker with ZERO per-host config:
//
//	ssh     — is the worker SSH daemon reachable AND does our key
//	          authenticate (SecureSSHClient flags already enforce
//	          BatchMode + StrictHostKeyChecking=yes)?
//	hostkey — is the worker's host key present in the centralized
//	          /etc/velox/ssh/known_hosts file (so a future
//	          StrictHostKeyChecking=yes run will not be TOFU)?
//	sudo    — can the canonical SSH user run `sudo -n true`
//	          WITHOUT a password prompt (the master-driven update /
//	          smoke executors require passwordless sudo)?
//
// The check deliberately reuses the same hardened `ssh` invocation the
// production executors use (see SecureSSHClient.baseSSHArgs): the same
// key, the same known_hosts, the same BatchMode/StrictHostKeyChecking.
// There is intentional NO `&&` and no `||` in the remote command — the
// `sudo -n true` check is a distinct probe so a locked sudo policy is
// reported separately from a broken network/auth path.
//
// This is a READ-only diagnostic; it never mutates the worker. It is
// driven by the WorkerRegistry (persistent ansible_hosts view) so there
// is no second inventory file to keep in sync.
package fleet

import (
	"bytes"
	"context"
	"os/exec"
	"strconv"
	"strings"
)

// WorkerSSHStatus is the per-worker result of SSHConnectivityCheck.
// WorkerName is the optional operator-facing display name (e.g.
// "velox-worker-01"), resolved from the persistent workers registry when
// the caller supplies a name resolver; it is always distinct from the
// immutable WorkerID (the mTLS-bound security principal).
type WorkerSSHStatus struct {
	WorkerID   string `json:"worker_id"`
	WorkerName string `json:"worker_name,omitempty"`
	Host       string `json:"host"`
	User       string `json:"user"`
	Port       int    `json:"port"`
	SSH        string `json:"ssh"`
	HostKey    string `json:"hostkey"`
	Sudo       string `json:"sudo"`
	Detail     string `json:"detail"`
}

// Ready reports whether the worker is fully operational from the
// master-control-plane's point of view: ssh + hostkey + sudo all PASS.
func (s WorkerSSHStatus) Ready() bool {
	return s.SSH == "PASS" && s.HostKey == "PASS" && s.Sudo == "PASS"
}

// Result renders the per-worker consolidated verdict used by the CLI.
func (s WorkerSSHStatus) Result() string {
	if s.Ready() {
		return "READY"
	}
	return "NOT-READY"
}

// WorkerNameResolver maps an immutable worker_id to its operator-facing
// worker_name. It is optional: when nil, per-worker rows carry no
// worker_name and output degrades to the ID-only view.
type WorkerNameResolver func(workerID string) string

// SSHConnectivityCheck runs the three probes (ssh / hostkey / sudo)
// across every enabled worker in the registry. KnownHosts must point at
// the centralized known_hosts file; keyPath at the canonical private
// key. A nil registry yields an empty slice (nil-tolerant, mirroring
// the rest of the fleet surface). nameResolver, when non-nil, labels each
// row with the operator-facing worker_name for the immutable worker ID.
func SSHConnectivityCheck(ctx context.Context, reg *WorkerRegistry, keyPath, knownHosts string, nameResolver WorkerNameResolver) []WorkerSSHStatus {
	if reg == nil {
		return nil
	}
	if keyPath == "" {
		keyPath = DefaultSSHKeyPath
	}
	if knownHosts == "" {
		knownHosts = DefaultKnownHostsPath
	}
	entries := reg.ListWorkers()
	out := make([]WorkerSSHStatus, 0, len(entries))
	for _, e := range entries {
		out = append(out, checkOneWorker(ctx, e, keyPath, knownHosts, nameResolver))
	}
	return out
}

func checkOneWorker(_ context.Context, e WorkerRegistryEntry, keyPath, knownHosts string, resolveName WorkerNameResolver) WorkerSSHStatus {
	status := WorkerSSHStatus{
		WorkerID: e.WorkerID.String(),
		Host:     e.Host,
		User:     e.SSHUser,
		Port:     e.SSHPort,
	}
	if resolveName != nil {
		if name := resolveName(e.WorkerID.String()); name != "" {
			status.WorkerName = name
		}
	}

	hostKeyFail := hostKeyNotFound(keyPath, knownHosts, e)
	if hostKeyFail != "" {
		status.HostKey = "FAIL"
		status.Detail = "host key absent from known_hosts: " + hostKeyFail
		return status
	}
	status.HostKey = "PASS"

	// Probe 1 (proves reachability + auth + host key acceptance in one
	// round trip). A failure here short-circuits the sudo probe — there
	// is no point asserting a passwordless sudo policy over a dead auth
	// path — but the hostkey verdict is already PASS (it was in the file);
	// the failure is a network/auth/known_hosts-drift issue.
	if _, err := runSSH(keyPath, knownHosts, e, "true"); err != nil {
		status.SSH = "FAIL"
		status.Sudo = "SKIP"
		status.Detail = "ssh: " + err.Error()
		return status
	}
	status.SSH = "PASS"

	// Probe 2: passwordless sudo, reported independently so a locked
	// sudoers entry is diagnosable without confusing it for auth failure.
	if _, err := runSSH(keyPath, knownHosts, e, "sudo -n true"); err != nil {
		status.Sudo = "FAIL"
		status.Detail = "sudo -n rejected (sudo policy does not allow passwordless): " + err.Error()
		return status
	}
	status.Sudo = "PASS"
	return status
}

// runSSH shells out to the ssh binary with the same hardened flags the
// production executors use. It returns the combined output and an error
// if ssh exits non-zero (or the host key is unknown).
func runSSH(keyPath, knownHosts string, e WorkerRegistryEntry, command string) (string, error) {
	args := []string{
		"-i", keyPath,
		"-o", "StrictHostKeyChecking=yes",
		"-o", "UserKnownHostsFile=" + knownHosts,
		"-o", "ConnectTimeout=10",
		"-o", "BatchMode=yes",
		"-o", "LogLevel=ERROR",
	}
	if e.SSHPort != 0 && e.SSHPort != 22 {
		args = append(args, "-p", strconv.Itoa(e.SSHPort))
	}
	args = append(args, e.SSHHost(), command)

	var stdout, stderr bytes.Buffer
	cmd := exec.Command("ssh", args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return stdout.String() + stderr.String(), err
	}
	return stdout.String(), nil
}

// hostKeyNotFound returns "" when `ssh-keygen -F <host> -f known_hosts`
// finds the host key, else an error detail. This is a local, offline
// probe (it reads known_hosts, it does not dial the host), so it cleanly
// separates "host key missing from inventory" (a config problem) from
// "host unreachable" (a network problem).
func hostKeyNotFound(_ string, knownHosts string, e WorkerRegistryEntry) string {
	keyArg := e.Host
	if e.SSHPort != 0 && e.SSHPort != 22 {
		keyArg = "[" + e.Host + "]:" + strconv.Itoa(e.SSHPort)
	}
	out, err := exec.Command("ssh-keygen", "-F", keyArg, "-f", knownHosts).CombinedOutput()
	if err != nil {
		return "ssh-keygen error for " + keyArg + ": " + err.Error()
	}
	if strings.TrimSpace(string(out)) == "" {
		return "no ssh-keygen -F entry for " + keyArg + " in " + knownHosts
	}
	return ""
}
