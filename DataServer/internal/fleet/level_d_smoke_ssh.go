// Package fleet — SSHWorkerTarget + sshClient + SSHWorkerExec
// (production BackendSSHClient + BackendWorkerExec adapters).
//
// Split out of level_d_smoke_deps.go. See the parent file for the
// full Level D smoke dependency-surface contract.
package fleet

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// ── SSHWorkerTarget + sshClient (BackendSSHClient) ──────────────────
//
// Production adapters for executing commands on remote workers via SSH.
// The sshClient maps workerID → SSH connection details; SSHWorkerExec
// wraps it to implement the BackendWorkerExec surface for smoke phases.

// SSHWorkerTarget holds the connection details for a single worker.
type SSHWorkerTarget struct {
	Host    string // IP or hostname
	User    string // SSH user (e.g. debian, ubuntu, velox-deploy)
	KeyPath string // path to SSH private key
}

// sshClient implements BackendSSHClient by shelling out to the
// system ssh binary. Tests wire the stubs from update_executor_test.go
// which implement the same interface via canned responses.
type sshClient struct {
	targets map[string]SSHWorkerTarget
}

// NewSSHClient returns a BackendSSHClient backed by the system ssh
// binary. targets maps workerID → SSH connection details; workers
// not in the map will receive an error on Run.
func NewSSHClient(targets map[string]SSHWorkerTarget) BackendSSHClient {
	return &sshClient{targets: targets}
}

// NewSSHClientFromRegistry returns a BackendSSHClient whose target map
// is derived from the canonical WorkerRegistry (itself populated from the
// persistent `ansible_hosts` worker-node view). This is the Phase 9
// inventory unification seam: the ONLY sanctioned way to build the SSH
// client in production is from the WorkerRegistry — never from a
// hardcoded map. Rows without a host or user are skipped so a partially
// populated inventory fails per-target at Run time (with a clear error)
// instead of producing a malformed ssh invocation at build time.
func NewSSHClientFromRegistry(reg *WorkerRegistry) BackendSSHClient {
	if reg == nil {
		return &sshClient{targets: map[string]SSHWorkerTarget{}}
	}
	targets := make(map[string]SSHWorkerTarget)
	for _, e := range reg.ListWorkers() {
		if strings.TrimSpace(e.Host) == "" || strings.TrimSpace(e.SSHUser) == "" {
			continue
		}
		targets[e.WorkerID.String()] = SSHWorkerTarget{
			Host: e.Host,
			User: e.SSHUser,
			// Canonical key path — NOT "" (sshClient.Run would then fall
			// back to $HOME/.ssh/id_ed25519_velox, which breaks production
			// where the key lives at /etc/velox/ssh/id_ed25519_velox).
			KeyPath: DefaultSSHKeyPath,
		}
	}
	return &sshClient{targets: targets}
}

// Run executes command on the worker via ssh. Returns the combined
// stdout+stderr on success, or an error wrapping the ssh exit code.
//
// Security: StrictHostKeyChecking=yes with UserKnownHostsFile ensures
// the worker's host key is verified against a known_hosts file managed
// by the operator. The known_hosts file is populated at deploy time
// via ansible (ssh-keyscan) and must exist at the configured path.
func (c *sshClient) Run(ctx context.Context, workerID string, command string) (string, error) {
	t, ok := c.targets[workerID]
	if !ok {
		return "", fmt.Errorf("ssh: no target configured for worker %s", workerID)
	}
	if t.Host == "" || t.User == "" {
		return "", fmt.Errorf("ssh: incomplete target for worker %s (host=%q user=%q)", workerID, t.Host, t.User)
	}
	keyPath := t.KeyPath
	if keyPath == "" {
		keyPath = DefaultSSHKeyPath
	}
	cmd := exec.CommandContext(ctx, "ssh",
		"-i", keyPath,
		"-o", "StrictHostKeyChecking=yes",
		"-o", "UserKnownHostsFile=/etc/velox/ssh/known_hosts",
		"-o", "ConnectTimeout=10",
		"-o", "BatchMode=yes",
		"-o", "LogLevel=ERROR",
		t.User+"@"+t.Host,
		command,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("ssh %s@%s: %w (output: %s)", t.User, t.Host, err, string(out))
	}
	return string(out), nil
}

// ── SSHWorkerExec (BackendWorkerExec via SSH) ───────────────────────
//
// Executes smoke phases on the remote worker via SSH. Each method
// constructs a shell command and delegates to BackendSSHClient.Run.
// Production wires this when SSH keys and worker connectivity are
// configured; before that, LocalShellWorker serves as the dev adapter.

// SSHWorkerExec implements BackendWorkerExec by running commands on
// the remote worker via SSH.
type SSHWorkerExec struct {
	ssh BackendSSHClient
}

// NewSSHWorkerExec returns a BackendWorkerExec that runs smoke phases
// on remote workers via the provided SSH client.
func NewSSHWorkerExec(ssh BackendSSHClient) BackendWorkerExec {
	return &SSHWorkerExec{ssh: ssh}
}

// DownloadAsset downloads the asset on the remote worker.
// In production, uses curl from the resolved pickupURL.
// A pickupURL starting with "asset://" or empty URL triggers the
// dev-mode fallback: a synthetic ffmpeg-generated clip.
func (e *SSHWorkerExec) DownloadAsset(ctx context.Context, runID, workerID, pickupURL, destPath string) error {
	// Production path: download the real asset from the pickup URL.
	// asset:// URLs are synthetic (StubAssetResolver) — skip curl.
	if pickupURL != "" && !strings.HasPrefix(pickupURL, "asset://") {
		cmd := fmt.Sprintf(
			"mkdir -p %s && curl -sSL -o %s '%s'",
			filepath.Dir(destPath), destPath, pickupURL,
		)
		_, err := e.ssh.Run(ctx, workerID, cmd)
		if err != nil {
			return fmt.Errorf("%w: ssh download asset from %s: %v", ErrAssetDownloadFail, pickupURL, err)
		}
		return nil
	}

	// Dev-mode fallback: generate a synthetic test clip via ffmpeg lavfi.
	cmd := fmt.Sprintf(
		"mkdir -p %s && ffmpeg -f lavfi -i color=c=blue:size=320x240:d=1 -c:v libx264 -f mp4 -t 1 -y %s",
		filepath.Dir(destPath), destPath,
	)
	_, err := e.ssh.Run(ctx, workerID, cmd)
	if err != nil {
		return fmt.Errorf("%w: ssh download asset: %v", ErrAssetDownloadFail, err)
	}
	return nil
}

// RunFFmpegRender executes the render on the remote worker, then SCPs the
// artifact back to a local temp path so LocalFileDriveUploader can read it.
// Returns the LOCAL artifact path + size in bytes.
func (e *SSHWorkerExec) RunFFmpegRender(ctx context.Context, runID, workerID, renderPlan, outputPath string) (string, int64, error) {
	// 1. Render on remote worker + stat for byte count.
	var cmd string
	if renderPlan != "" {
		inputPath := filepath.Join(filepath.Dir(outputPath), runID+".in")
		cmd = fmt.Sprintf(
			"mkdir -p %s && ffmpeg -y -i %s -c:v libx264 -t 2 %s 2>/dev/null && stat -c%%s %s",
			filepath.Dir(outputPath), inputPath, outputPath, outputPath,
		)
	} else {
		cmd = fmt.Sprintf(
			"mkdir -p %s && ffmpeg -y -f lavfi -i color=c=red:size=320x240:d=2 -c:v libx264 -t 2 %s 2>/dev/null && stat -c%%s %s",
			filepath.Dir(outputPath), outputPath, outputPath,
		)
	}
	out, err := e.ssh.Run(ctx, workerID, cmd)
	if err != nil {
		return "", 0, fmt.Errorf("%w: ssh ffmpeg render: %v", ErrFFmpegRenderFail, err)
	}
	// Last line is the stat byte count.
	out = strings.TrimSpace(out)
	lines := strings.Split(out, "\n")
	lastLine := strings.TrimSpace(lines[len(lines)-1])
	artifactBytes, parseErr := strconv.ParseInt(lastLine, 10, 64)
	if parseErr != nil {
		return "", 0, fmt.Errorf("%w: parse stat output %q: %v", ErrArtifactMissing, lastLine, parseErr)
	}
	if artifactBytes == 0 {
		return "", 0, fmt.Errorf("%w: artifact is empty (stat returned 0)", ErrArtifactMissing)
	}

	// 2. Fetch the artifact via base64 (sshClient.Run returns string, so
	//    we can't pass raw binary — base64 is ASCII-safe and smoke videos
	//    are tiny).
	fetchCmd := fmt.Sprintf("base64 -w0 %s 2>/dev/null", outputPath)
	b64, err := e.ssh.Run(ctx, workerID, fetchCmd)
	if err != nil {
		return "", 0, fmt.Errorf("%w: ssh fetch artifact: %v", ErrArtifactMissing, err)
	}
	raw, decErr := base64.StdEncoding.DecodeString(strings.TrimSpace(b64))
	if decErr != nil {
		return "", 0, fmt.Errorf("%w: base64 decode artifact: %v", ErrArtifactMissing, decErr)
	}

	// 3. Write to local temp path so LocalFileDriveUploader can read it.
	localPath := filepath.Join(SmokeTempRoot, runID, runID+".mp4")
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return "", 0, fmt.Errorf("%w: mkdir for local artifact: %v", ErrArtifactMissing, err)
	}
	if err := os.WriteFile(localPath, raw, 0644); err != nil {
		return "", 0, fmt.Errorf("%w: write local artifact: %v", ErrArtifactMissing, err)
	}
	return localPath, artifactBytes, nil
}

// CleanupWorkerTemp removes smoke temp files from the remote worker AND
// the local temp directory created by RunFFmpegRender.
// Best-effort: always returns nil (errors are logged by the executor).
func (e *SSHWorkerExec) CleanupWorkerTemp(ctx context.Context, runID, workerID string) error {
	cmd := fmt.Sprintf(
		"rm -f /var/lib/velox-worker/smoke/%s.* /tmp/velox-smoke/%s/* 2>/dev/null; true",
		runID, runID,
	)
	// Best-effort: ignore errors (worker may be unreachable or files already gone).
	_, _ = e.ssh.Run(ctx, workerID, cmd)
	// Also clean local temp files written by RunFFmpegRender.
	_ = os.RemoveAll(filepath.Join(SmokeTempRoot, runID))
	return nil
}
