// Package fleet — Step 12/15 LevelDSmokeExecutor dependency surface.
//
// This file mirrors update_executor_deps.go (Step 9/15): defines
// the narrow consumer-side interfaces and dependent types that
// the Level D smoke orchestrator depends on. Each interface
// is intentionally tiny — Go convention: consumer interfaces
// are smaller than producer interfaces. Production wires live
// implementations:
//   - lease store    → Worker.Drain transient (Step 6/15 owner)//   - worker exec   → BackendSSHClient (the production SSH adapter)
//   - drive uploader → integrations/drive.Service.UploadFile
//   - asset resolver → the production asset-bundle lookup; no stub is
//     permitted in the production composition

//   - smoke runs     → SQLiteStore persisted rows
//
// Tests wire in-process stubs that drive every phase + failure
// mode without standing up real infra.
//
// Per-step sentinels below are the operator-dashboard grep keys;
// the audit row's error_message prefixes each failure with the
// sentinel string so the dashboard surfaces a clean category
// per failure rather than a stringified Go error.
//
// File split by responsibility:
//   - level_d_smoke_deps.go    → errors, status, payload, interfaces, backend bundle
//   - level_d_smoke_lease.go   → RegistryDrainLease (BackendLeaseStore adapter)
//   - level_d_smoke_local.go   → LocalShellWorker + LocalFileDriveUploader (dev adapters)
//   - level_d_smoke_ssh.go     → SSHWorkerTarget + sshClient + SSHWorkerExec (prod adapters)
package fleet

import (
	"context"
	"errors"
	"time"

	"velox-server/internal/store"
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

// ── Status enum ────────────────────────────────────────────────────
//
// The smoke_runs status vocabulary is NOT defined here. The single
// canonical source is smokerunstore.SmokeStatus* (re-exported by
// store as store.SmokeStatus*); the executor and the smoke-health
// runner reference it via the store import above. Defining a local
// copy would fork the vocabulary and let the two drift.

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
// a smoke lease on a worker". Production wires a Worker.Drain
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
//	DownloadAsset     — curl/SSH the asset bundle from a pickup URL
//	RunFFmpegRender  — invoke the engine's ffmpeg-wrapped render
//	                    plan; returns the output path + size in bytes
//	CleanupWorkerTemp — best-effort rm of /var/lib/velox-worker/smoke/<run_id>
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
// expectedBytes and expectedSHA256 are the content-addressed metadata
// verified before an adapter accepts the upload. Concrete adapters must
// validate the exact bytes they hand to the remote/local sink; this keeps
// an upload from succeeding with a truncated or substituted file.
type BackendDriveUploader interface {
	UploadArtifact(ctx context.Context, runID, srcPath string, expectedBytes int64, expectedSHA256 string) (driveFileID string, err error)
}

// BackendAssetResolver maps an AssetID to its pickup URL +
// expected byte size. Production must wire the canonical asset
// picker (referenced via mission-control/asset bundle lookup).
// The development-only StubAssetResolver is never accepted by
// ConfigureLevelDSmokeCapability when composing production.
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
// ErrSmokeRunnerNotWired when its specific dependency is nil. Empty
// construction is useful for tests and partial composition only; production
// validation rejects an incomplete backend before the supervisor starts.
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
	Verifier  BackendArtifactVerifier
	Now       NowFunc
}

// StubAssetResolver is a development-only BackendAssetResolver used
// exclusively when VELOX_SMOKE_MODE=development. It must never be
// passed to a production composition; ConfigureLevelDSmokeCapability
// validates that production has a real resolver and fails closed when
// the resolver is absent.
//
// In production mode this resolver MUST NOT be used — the asset
// resolver should be nil, causing the executor's pre-flight nil
// check to surface ErrSmokeRunnerNotWired until the real asset
// resolver is wired.
type StubAssetResolver struct {
	PickupURL     string
	ExpectedBytes int64
}

// NewStubAssetResolver returns a StubAssetResolver for explicit dev-mode
// smoke runs. Production bootstrap must not call this constructor.
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
