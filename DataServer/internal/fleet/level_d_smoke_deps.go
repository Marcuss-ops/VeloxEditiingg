// Package fleet — Step 12/15 LevelDSmokeExecutor dependency surface.
//
// This file mirrors update_executor_deps.go (Step 9/15): defines
// the narrow consumer-side interfaces and dependent types that
// the Level D smoke orchestrator depends on. Each interface
// is intentionally tiny — Go convention: consumer interfaces
// are smaller than producer interfaces. Production wires live
// implementations:
//   - lease store    → WorkerInfo.Drain transient (Step 6/15 owner)
//   - worker exec    → BackendSSHClient (Step 9/15 owner, real SSH
//                     wiring lands in Step 11+)
//   - drive uploader → integrations/drive.Service.UploadFile
//                     (production wiring lands in a follow-up step)
//   - asset resolver → minimal stub today; bundle lookup wiring
//                     lands when the canonical asset picker lands
//   - smoke runs     → SQLiteStore persisted rows
//
// Tests wire in-process stubs that drive every phase + failure
// mode without standing up real infra.
//
// Per-step sentinels below are the operator-dashboard grep keys;
// the audit row's error_message prefixes each failure with the
// sentinel string so the dashboard surfaces a clean category
// per failure rather than a stringified Go error.
package fleet

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

// ── Sentinel errors ──────────────────────────────────────────────────
//
// Each maps to a grep-friendly error_message prefix in the
// audit ledger so operator dashboards can route on it.
var (
	// ErrSmokeRunnerNotWired is returned when the executor
	// backend is missing one of the production deps (i.e. SSH
	// or Drive uploader not wired). Surfaces Audit-Only mode at
	// the Level D smoke endpoint without a confusing 500 — the
	// executor's caller (FleetController tick) maps the row to
	// FAILED with error_message = "smoke_runner_not_wired".
	ErrSmokeRunnerNotWired = errors.New("smoke_runner_not_wired")

	// ErrSmokeLeaseUnavailable is returned when the smoke
	// lease cannot be acquired (worker is DRAINING for another
	// operation, or transient registry contention).
	ErrSmokeLeaseUnavailable = errors.New("smoke_lease_unavailable")

	// ErrAssetDownloadFail surfaces a backend Worker.DownloadAsset
	// failure (SSH unreachable, asset URL stale, disk full).
	ErrAssetDownloadFail = errors.New("asset_download_failed")

	// ErrFFmpegRenderFail surfaces a backend Worker.RunFFmpegRender
	// failure (ffmpeg returns non-zero, render timeout).
	ErrFFmpegRenderFail = errors.New("ffmpeg_render_failed")

	// ErrArtifactMissing surfaces when the ffmpeg exit was clean
	// but the expected artifact file is absent on the worker
	// (write-to-network issue, disk pressure, fs corruption).
	ErrArtifactMissing = errors.New("artifact_missing")

	// ErrDriveUploadFail surfaces a backend Drive.UploadArtifact
	// failure (Drive API 5xx, auth expired, quota exceeded).
	ErrDriveUploadFail = errors.New("drive_upload_failed")

	// ErrSmokePipelineFailed is the marker wrap for "forward
	// pipeline failed; cleanup attempted". The audit dashboard's
	// error_message reads "<phase>: <err>" + ErrSmokePipelineFailed
	// stringified in the message.
	ErrSmokePipelineFailed = errors.New("smoke_pipeline_failed")

	// ErrSmokeCleanupFailed is the marker wrap for "clean-up
	// cascade also failed". The audit row's error_message
	// surfaces both the original pipeline failure + the
	// cleanup failure (concatenated).
	ErrSmokeCleanupFailed = errors.New("smoke_cleanup_failed")
)

// ── Status enum (mirrors DeployStatus* in store_deployment_records.go) ──

const (
	// SmokeStatusPending is the initial state at insert. The
	// row exists but no phase has completed yet.
	SmokeStatusPending = "PENDING"

	// SmokeStatusSucceeded marks a smoke where every phase
	// completed: lease acquired → asset downloaded → ffmpeg
	// rendered → artifact uploaded to Drive. The artifact_drive_id
	// columns holds the canonical Drive URL.
	SmokeStatusSucceeded = "SUCCEEDED"

	// SmokeStatusFailed marks a smoke where any phase failed
	// AND cleanup could not recover. The error_message column
	// stores the operator-readable failure diagnosis.
	SmokeStatusFailed = "FAILED"
)

// ── Payload schema ──────────────────────────────────────────────────

// SmokePayload is the Operation.Payload schema for the `smoke`
// kind. Decoded by LevelDSmokeExecutor.parsePayload at the start
// of Execute.
//
// All fields are optional except AssetID which is required for
// the asset-download phase. RenderPlan is a JSON blob the
// executor passes to the worker-side ffmpeg invocator. TimeoutSec
// is a per-run override that falls back to TotalBudget when
// unspecified or zero.
type SmokePayload struct {
	AssetID    string `json:"asset_id"`
	RenderPlan string `json:"render_plan,omitempty"`
	TimeoutSec int    `json:"timeout_sec,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

// ── Run-record shape ────────────────────────────────────────────────
//
// Executor uses store.SmokeRun directly (no local mirror).
// Defined in store/store_smoke_runs.go; this file references
// it via the store import above. Tests use store.SmokeRun
// values directly via the BackendSmokeRuns interface; a
// separate SmokeRun local type would just create a second
// field-set the executor has to maintain.

// ── Consumer interfaces ─────────────────────────────────────────────

// BackendLeaseStore is the typed surface for "acquire and release
// a smoke lease on a worker". Production wires a WorkerInfo.Drain
// transient implementation (SetWorkerDrain(true) on acquire;
// SetWorkerDrain(false) on release) which excludes the worker
// from real-job placement for the smoke duration.
//
// AcquireSmokeLease MUST return ErrSmokeLeaseUnavailable when
// the worker is already drained for a different operation; the
// caller treats this as a hard 409-equivalent at the audit row.
type BackendLeaseStore interface {
	AcquireSmokeLease(ctx context.Context, runID, workerID string) error
	ReleaseSmokeLease(ctx context.Context, runID string) error
}

// BackendWorkerExec owns the worker's per-phase action surface.
// Each method corresponds to one phase of the forward pipeline:
//
//   DownloadAsset     — curl/SSH the asset bundle from a pickup URL
//   RunFFmpegRender  — invoke the engine's ffmpeg-wrapped render
//                       plan; returns the output path + size in bytes
//   CleanupWorkerTemp — best-effort rm of /var/lib/velox-worker/smoke/<run_id>
//
// Production reuses Step 9/15's BackendSSHClient for the
// underlying shell-out; per-method semantics are implemented by
// the smoke-specific SSH exec helper that wraps the SSHClient
// with phase-specific command strings. Tests wire a stub
// dispatching by command-string prefix.
//
// DownloadAsset receives the pickupURL resolved by Phase 1 (asset
// resolver) so the worker can download the real asset bundle
// instead of generating a synthetic test clip. In dev mode
// (VELOX_SMOKE_MODE=development) the URL may be empty and the
// implementation falls back to a synthetic ffmpeg-generated clip.
type BackendWorkerExec interface {
	DownloadAsset(ctx context.Context, runID, workerID, pickupURL, destPath string) error
	RunFFmpegRender(ctx context.Context, runID, workerID, renderPlan, outputPath string) (artifactPath string, artifactBytes int64, err error)
	CleanupWorkerTemp(ctx context.Context, runID, workerID string) error
}

// BackendDriveUploader uploads the rendered artifact to Google
// Drive via the auth-token managed by integrations/drive.Service.
// Production wires integrations/drive.Service.UploadFile; tests
// wire a stub that returns a canned DriveFileID.
//
// expectedBytes is the size enforced on the upload response —
// Drive returns a 200 with size mismatch when the upload silently
// truncated the file (network glitch in the middle); the
// verifier catches the divergence.
type BackendDriveUploader interface {
	UploadArtifact(ctx context.Context, runID, srcPath string, expectedBytes int64) (driveFileID string, err error)
}

// BackendAssetResolver maps an AssetID to its pickup URL +
// expected byte size. Production wires the canonical asset
// picker (referenced via mission-control/asset bundle lookup);
// today the orchestrator ships with a minimal stub returning a
// canned URL — a TODO comment marks the production wiring path.
// The resolver MUST set expectedBytes to the bundle's actual
// downloaded size (NOT a fixed constant) so Drive's size-mismatch
// check catches upload truncation.
type BackendAssetResolver interface {
	ResolveAsset(ctx context.Context, assetID string) (pickupURL string, expectedBytes int64, err error)
}

// BackendSmokeRuns is the typed surface for the smoke_runs table.
// The executor's contract:
//
//   - InsertSmokeRun is called with Status=PENDING — partial
//     failure mid-cascade can be reasoned about row-by-row.
//   - MarkSmokeSucceeded is the terminal SUCCEEDED transition,
//     also stamping finished_at + duration_ms +
//     artifact_drive_id.
//   - MarkSmokeFailed is the terminal FAILED transition, also
//     stamping finished_at + duration_ms + error_message.
//
// Both terminal transitions are atomic (single UPDATE) so the
// dashboard never observes a torn (status=SUCCEEDED, finished_at=NULL)
// row.
type BackendSmokeRuns interface {
	InsertSmokeRun(ctx context.Context, rec store.SmokeRun) error
	MarkSmokeSucceeded(ctx context.Context, runID string, finishedAt time.Time, durationMs int64, artifactDriveID string) error
	MarkSmokeFailed(ctx context.Context, runID string, finishedAt time.Time, durationMs int64, errMsg string) error
	GetLatestSmokeForWorker(ctx context.Context, workerID string) (*store.SmokeRun, error)
	ListRecentSmokesForWorker(ctx context.Context, workerID string, limit int) ([]store.SmokeRun, error)
}

// ── Backend bundle ─────────────────────────────────────────────────

// LevelDSmokeBackend bundles the 5 dep surfaces into a single
// construction-time argument. Mirrors fleet.UpdateBackend
// (Step 9/15) for visual+test symmetry — production wires the
// bundle at buildFleet; tests construct it stub-by-stub.
//
// nil-tolerant per field: each per-step helper surfaces
// errPhaseNotWired (or the phase analogue) when its specific
// dep is nil. Empty-backend construction is the production
// wiring's starting position because BackendWorkerExec and
// BackendDriveUploader don't yet have real implementations
// pending Step 11+ and a follow-up Drive-wiring step.
//
// Compile-time shape contract: Executor uses store.SmokeRun
// throughout (rather than a local mirror struct) so Go's
// import-graph type-check enforces the field set across the
// store + fleet packages. Renaming a field on store.SmokeRun
// breaks both the executor compile AND the store tests; both
// are caught by the AGENTS.md §1 gate's `go build ./...`.
type LevelDSmokeBackend struct {
	Worker    BackendWorkerExec
	Drive     BackendDriveUploader
	Asset     BackendAssetResolver
	Lease     BackendLeaseStore
	SmokeRuns BackendSmokeRuns
	Now       NowFunc
}

// ── Production adapters (composition-root side) ─────────────────────
//
// These concrete adapter types live next to the consumer
// interfaces so the bootstrap_composition.go wiring has a single
// canonical import. They are NOT interfaces — concrete
// implementations of the surfaces above.

// RegistryDrainLease adapts the in-process workersreg.Registry
// to the BackendLeaseStore surface: AcquireSmokeLease flips
// WorkerInfo.Drain=true (excluding the worker from real-job
// placement for the smoke duration); ReleaseSmokeLease restores
// Drain=false. Symmetric with Step 6/15's mutations handler
// which calls SetWorkerDrain directly.
//
// Audit-only invariant: when WorkerInfo.Drain is set transiently
// by smoke, the Worker's Health derivation (Step 3/15) reflects
// DRAINING on the next poll. The deferred ReleaseSmokeLease
// in LevelDSmokeExecutor's Phase 3 cleanup ensures the worker
// recovers to HEALTHY even if a panic / cleanup-skipped state
// interrupts the pipeline.
//
// The runID convention is "smoke-<workerID>-<nanos>" — see
// LevelDSmokeExecutor's `runID := fmt.Sprintf(...)` call —
// so ReleaseSmokeLease splits the runID to recover workerID.
// Future steps may swap this for a parallel
// smoke_lease_owner column in workers.raw_json, but for atomic
// Step 12+ we accept the URL-encoding constraint.
type RegistryDrainLease struct {
	Reg *workersreg.Registry
}

// NewRegistryDrainLease returns the lease store wrapping the
// given registry. Production calls this in bootstrap with
// m.Workers.Registry() as the registry. Returns nil if reg is
// nil so the bootstrap's nil-tolerance flow-through survives.
func NewRegistryDrainLease(reg *workersreg.Registry) BackendLeaseStore {
	if reg == nil {
		return nil
	}
	return &RegistryDrainLease{Reg: reg}
}

// AcquireSmokeLease calls reg.SetWorkerDrain(workerID, true) so
// costmodel.Score excludes the worker. Returns
// ErrSmokeLeaseUnavailable on any underlying error so the
// audit-row grep is stable.
func (r *RegistryDrainLease) AcquireSmokeLease(_ context.Context, runID, workerID string) error {
	if r == nil || r.Reg == nil {
		return ErrSmokeLeaseUnavailable
	}
	if err := r.Reg.SetWorkerDrain(context.Background(), workerID, true); err != nil {
		return fmt.Errorf("%w: worker drain or registry error: %v", ErrSmokeLeaseUnavailable, err)
	}
	return nil
}

// ReleaseSmokeLease calls reg.SetWorkerDrain(workerID, false)
// idempotently. Parses the workerID from the runID: the executor
// formats runID as "smoke-<workerID>-<nanos>". Since workerID
// may itself contain dashes (e.g. "velox-worker-523925eb"), we
// strip the "smoke-" prefix then split on the LAST dash to
// separate workerID from nanos.
func (r *RegistryDrainLease) ReleaseSmokeLease(_ context.Context, runID string) error {
	if r == nil || r.Reg == nil {
		return nil
	}
	const prefix = "smoke-"
	if !strings.HasPrefix(runID, prefix) {
		return nil
	}
	withoutPrefix := runID[len(prefix):]
	lastDash := strings.LastIndex(withoutPrefix, "-")
	if lastDash <= 0 {
		return nil
	}
	workerID := withoutPrefix[:lastDash]
	if err := r.Reg.SetWorkerDrain(context.Background(), workerID, false); err != nil {
		return fmt.Errorf("smoke: release drain: %w", err)
	}
	return nil
}

// StubAssetResolver is the minimal BackendAssetResolver used
// for development smoke runs (VELOX_SMOKE_MODE=development) until
// the canonical asset picker (the stage_preflight / engine_planner
// asset lookup) lands. Both pickup URL and expected bytes are
// caller-provided so production wiring can swap it with a real
// resolver via the same seam.
//
// In production mode this resolver MUST NOT be used — the asset
// resolver should be nil, causing the executor's pre-flight nil
// check to surface ErrSmokeRunnerNotWired until the real asset
// resolver is wired.
type StubAssetResolver struct {
	PickupURL     string
	ExpectedBytes int64
}

// NewStubAssetResolver returns a StubAssetResolver for dev-mode smoke runs.
func NewStubAssetResolver(pickupURL string, expectedBytes int64) *StubAssetResolver {
	return &StubAssetResolver{PickupURL: pickupURL, ExpectedBytes: expectedBytes}
}

// ResolveAsset returns the canned pickup URL + expected bytes.
func (s *StubAssetResolver) ResolveAsset(_ context.Context, _ string) (string, int64, error) {
	if s.PickupURL == "" {
		return "", 0, errors.New("smoke: stub asset resolver has empty pickup_url (production wiring lands in a follow-up step)")
	}
	return s.PickupURL, s.ExpectedBytes, nil
}

// ── LocalShellWorker (BackendWorkerExec via os/exec) ────────────────
//
// Runs smoke phases (download, ffmpeg, cleanup) as local shell
// commands on the Master host. This is a development/staging
// adapter — production swaps in a real SSH-backed
// BackendWorkerExec once SSH keys and worker connectivity are
// configured. The adapter creates a per-run temp directory under
// SmokeTempRoot and cleans it up on CleanupWorkerTemp.

// SmokeTempRoot is the parent directory for smoke run temp files.
const SmokeTempRoot = "/tmp/velox-smoke"

// LocalShellWorker implements BackendWorkerExec by shelling out
// to local commands. Each method writes a small log file under
// SmokeTempRoot/<runID>/ for post-mortem inspection.
type LocalShellWorker struct {
	// FFmpegBin is the path to the ffmpeg binary (default: "ffmpeg").
	FFmpegBin string
}

// NewLocalShellWorker returns a LocalShellWorker with sensible defaults.
func NewLocalShellWorker() *LocalShellWorker {
	return &LocalShellWorker{FFmpegBin: "ffmpeg"}
}

func (w *LocalShellWorker) runDir(runID string) string {
	return filepath.Join(SmokeTempRoot, runID)
}

// DownloadAsset downloads the asset from pickupURL to destPath.
// In production, uses curl/wget from the resolved pickup URL.
// In dev mode (empty pickupURL), falls back to a synthetic ffmpeg-generated clip.
func (w *LocalShellWorker) DownloadAsset(_ context.Context, runID, _, pickupURL, destPath string) error {
	if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
		return fmt.Errorf("smoke: mkdir for download: %w", err)
	}
	_ = os.MkdirAll(w.runDir(runID), 0755)

	// Production path: download the real asset from the pickup URL.
	if pickupURL != "" {
		cmd := exec.Command("curl", "-sSL", "-o", destPath, pickupURL)
		out, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("smoke: curl download asset from %s: %w (output: %s)", pickupURL, err, string(out))
		}
		return nil
	}

	// Dev-mode fallback: generate a small test video via ffmpeg lavfi.
	ffmpeg := w.FFmpegBin
	if ffmpeg == "" {
		ffmpeg = "ffmpeg"
	}
	cmd := exec.Command(ffmpeg,
		"-f", "lavfi", "-i", "color=c=blue:size=320x240:d=1",
		"-c:v", "libx264", "-f", "mp4", "-t", "1", "-y", destPath,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("smoke: ffmpeg generate test asset: %w (output: %s)", err, string(out))
	}
	return nil
}

// RunFFmpegRender executes the render plan as a local ffmpeg command.
// Returns the output path and artifact size in bytes.
func (w *LocalShellWorker) RunFFmpegRender(_ context.Context, runID, _, renderPlan, outputPath string) (string, int64, error) {
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return "", 0, fmt.Errorf("smoke: mkdir for render: %w", err)
	}
	ffmpeg := w.FFmpegBin
	if ffmpeg == "" {
		ffmpeg = "ffmpeg"
	}
	args := []string{"-y"}
	if renderPlan != "" {
		// Derive input path from outputPath: the executor places
		// destPath and outputPath in the same directory (e.g.
		// /var/lib/velox-worker/smoke/<runID>.in / .mp4).
		inputPath := filepath.Join(filepath.Dir(outputPath), runID+".in")
		args = append(args, "-i", inputPath,
			"-c:v", "libx264", "-t", "2", outputPath)
	} else {
		args = append(args, "-f", "lavfi", "-i", "color=c=red:size=320x240:d=2",
			"-c:v", "libx264", "-t", "2", outputPath)
	}
	cmd := exec.Command(ffmpeg, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", 0, fmt.Errorf("%w: %v (output: %s)", ErrFFmpegRenderFail, err, string(out))
	}
	fi, err := os.Stat(outputPath)
	if err != nil {
		return "", 0, fmt.Errorf("%w: stat artifact: %v", ErrArtifactMissing, err)
	}
	return outputPath, fi.Size(), nil
}

// CleanupWorkerTemp removes the smoke temp directory and the
// executor's convention-path files for this run.
func (w *LocalShellWorker) CleanupWorkerTemp(_ context.Context, runID, _ string) error {
	// 1. Remove the local smoke temp dir (log / cache files).
	localDir := w.runDir(runID)
	if err := os.RemoveAll(localDir); err != nil {
		return fmt.Errorf("smoke: cleanup local dir %s: %w", localDir, err)
	}
	// 2. Remove executor convention-path files
	//    (/var/lib/velox-worker/smoke/<runID>.in, .mp4).
	smokeDir := "/var/lib/velox-worker/smoke"
	matches, _ := filepath.Glob(filepath.Join(smokeDir, runID+".*"))
	for _, f := range matches {
		_ = os.Remove(f)
	}
	return nil
}

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

// Run executes command on the worker via ssh. Returns the combined
// stdout+stderr on success, or an error wrapping the ssh exit code.
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
		keyPath = os.ExpandEnv("$HOME/.ssh/id_ed25519_velox")
	}
	cmd := exec.CommandContext(ctx, "ssh",
		"-i", keyPath,
		"-o", "StrictHostKeyChecking=no",
		"-o", "ConnectTimeout=10",
		"-o", "BatchMode=yes",
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
// In dev mode (empty pickupURL), falls back to a synthetic ffmpeg-generated clip.
func (e *SSHWorkerExec) DownloadAsset(ctx context.Context, runID, workerID, pickupURL, destPath string) error {
	// Production path: download the real asset from the pickup URL.
	if pickupURL != "" {
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

// ── LocalFileDriveUploader (BackendDriveUploader via local fs) ──────
//
// Writes the artifact to a local directory instead of uploading
// to Google Drive. Returns a fake driveFileID so the smoke
// pipeline can complete end-to-end. Production swaps in a real
// Drive-backed adapter once credentials are configured.

// LocalDriveRoot is the parent directory for smoke artifact "uploads".
const LocalDriveRoot = "/tmp/velox-smoke-drive"

// LocalFileDriveUploader implements BackendDriveUploader by copying
// the artifact to a local directory.
type LocalFileDriveUploader struct{}

// NewLocalFileDriveUploader returns a LocalFileDriveUploader.
func NewLocalFileDriveUploader() *LocalFileDriveUploader {
	return &LocalFileDriveUploader{}
}

// UploadArtifact copies srcPath to LocalDriveRoot/<runID>.mp4 and
// returns a fake Drive file ID.
func (d *LocalFileDriveUploader) UploadArtifact(_ context.Context, runID, srcPath string, _ int64) (string, error) {
	if err := os.MkdirAll(LocalDriveRoot, 0755); err != nil {
		return "", fmt.Errorf("smoke: mkdir drive root: %w", err)
	}
	dst := filepath.Join(LocalDriveRoot, runID+".mp4")
	src, err := os.ReadFile(srcPath)
	if err != nil {
		return "", fmt.Errorf("%w: read artifact: %v", ErrDriveUploadFail, err)
	}
	if err := os.WriteFile(dst, src, 0644); err != nil {
		return "", fmt.Errorf("%w: write local drive: %v", ErrDriveUploadFail, err)
	}
	fakeID := "local-drive-" + runID
	return fakeID, nil
}
