// Package fleet — Step 12/15 LevelDSmokeExecutor implementation.
//
// The user-spec awk flow is a 6-phase pipeline:
//
//	Phase 1 — Resolve asset       (BackendAssetResolver)
//	Phase 2 — Insert PENDING row  (BackendSmokeRuns)
//	Phase 3 — Acquire smoke lease (BackendLeaseStore; sets
//	                                   Worker.Drain=true so
//	                                   placement excludes the worker
//	                                   during the test)
//	Phase 4 — Download asset      (BackendWorkerExec; SSH +
//	                                   curl-or-equivalent pickup)
//	Phase 5 — ffmpeg render        (BackendWorkerExec; produces
//	                                   artifact on worker filesystem)
//	Phase 6 — Upload to Drive     (BackendDriveUploader; produces
//	                                   driveFileID and stores in
//	                                   smoke_runs.artifact_drive_id)
//	Phase 7 — Mark SUCCEEDED       (BackendSmokeRuns; stamps
//	                                   duration_ms for analytics
//	                                   baseline)
//
// On any forward failure (Phase 1-6):
//   - Mark FAILED on the smoke_runs row (with duration_ms + err)
//   - RunCleanup cascade (best-effort worker temp cleanup).
//     Lease release is MANDATORY (deferred at function entry).
//   - Return ErrSmokePipelineFailed wrap so the audit row's
//     error_message surface reads "<phase>: <err> (duration_ms=N)".
//
// Distinct from prepare-host.sh's Level B selftest (which only
// checks container-level health), the Level D smoke is the
// canonical end-to-end render path used for analytics baseline.
// Production requires real SSH, Drive, asset, lease, smoke-run and artifact
// verification dependencies; missing wiring fails the operation and boot
// validation rather than silently becoming a successful no-op.
//
// Each Phase uses context.WithTimeout so a runaway step fails
// fast rather than pinning the FleetController opTimeout for
// the whole smoke.
package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"velox-server/internal/store"
)

// Per-step timeouts. Single point of tuning — production-ready
// defaults that match the user's spec's "5-15s render" envelope
// with 4x headroom.
const (
	timeoutAssetResolve  = 30 * time.Second
	timeoutSmokeInsert   = 5 * time.Second
	timeoutLeaseAcquire  = 5 * time.Second
	timeoutAssetDownload = 30 * time.Second
	timeoutFFmpegRender  = 5 * time.Minute
	timeoutDriveUpload   = 60 * time.Second
	timeoutSmokeFinal    = 5 * time.Second
	timeoutCleanupWorker = 30 * time.Second
)

// LevelDSmokeExecutor is the Step 12/15 OperationExecutor binding
// for fleet.OperationKindSmoke. It must be explicitly registered
// in the production ExecutorRegistry.
//
// Construction takes a single LevelDSmokeBackend bundle (mirrors
// Step 9/15's UpdateBackend). Nil `now` defaults to time.Now;
// nil per-dep fails Execute loudly via the ErrSmokeRunnerNotWired
// sentinel (rather than panicking on first call).
type LevelDSmokeExecutor struct {
	backend LevelDSmokeBackend
}

// NewLevelDSmokeExecutor returns a LevelDSmokeExecutor ready for
// ExecutorRegistry.Register(fleet.OperationKindSmoke, exec).
func NewLevelDSmokeExecutor(b LevelDSmokeBackend) *LevelDSmokeExecutor {
	if b.Now == nil {
		b.Now = func() time.Time { return time.Now().UTC() }
	}
	return &LevelDSmokeExecutor{backend: b}
}

// ValidateProductionBackends keeps a registered smoke executor from being
// mistaken for a ready capability. The fleet bootstrap must wire every
// backend before this executor can be considered production-ready.
func (e *LevelDSmokeExecutor) ValidateProductionBackends() error {
	if e == nil {
		return errors.New("smoke: nil executor")
	}
	missing := make([]string, 0, 6)
	if isNilSmokeDependency(e.backend.Worker) {
		missing = append(missing, "worker")
	}
	if isNilSmokeDependency(e.backend.Drive) {
		missing = append(missing, "drive")
	}
	if isNilSmokeDependency(e.backend.Asset) {
		missing = append(missing, "asset")
	}
	if isNilSmokeDependency(e.backend.Lease) {
		missing = append(missing, "lease")
	}
	if isNilSmokeDependency(e.backend.SmokeRuns) {
		missing = append(missing, "smoke_runs")
	}
	if isNilSmokeDependency(e.backend.Verifier) {
		missing = append(missing, "verifier")
	}
	if len(missing) > 0 {
		return fmt.Errorf("%w: missing dependencies: %s", ErrSmokeRunnerNotWired, strings.Join(missing, ", "))
	}
	return nil
}

// Execute runs the 6-phase forward pipeline + cleanup cascade.
// Returns nil only on a clean forward success; any other terminal
// state returns a wrapped error the FleetController maps to a
// FAILED Operation row in fleet_operations, with the underlying
// smoke_runs row also marked FAILED for analytics.
func (e *LevelDSmokeExecutor) Execute(ctx context.Context, op *store.Operation) error {
	if op == nil {
		return errors.New("smoke: nil operation")
	}
	if op.WorkerID == "" {
		return errors.New("smoke: worker_id empty")
	}
	// ── Pre-flight: nil-tolerance guard ──────────────────────────
	// Surface the missing dep in one wrapped error rather than
	// discovering it mid-pipeline. The 5 required backends each
	// resolve a specific phase; if any one is nil the pipeline
	// cannot run.
	if isNilSmokeDependency(e.backend.Worker) || isNilSmokeDependency(e.backend.Drive) ||
		isNilSmokeDependency(e.backend.Lease) || isNilSmokeDependency(e.backend.SmokeRuns) ||
		isNilSmokeDependency(e.backend.Asset) || isNilSmokeDependency(e.backend.Verifier) {
		return fmt.Errorf("%w: required backend missing (worker/drive/lease/asset/smokeruns)", ErrSmokeRunnerNotWired)
	}
	// ── Phase 0: parse payload ───────────────────────────────────
	payload, err := e.parsePayload(op)
	if err != nil {
		return fmt.Errorf("smoke: parse payload: %w", err)
	}
	// ── Phase 1: resolve asset ───────────────────────────────────
	resolveCtx, cancel := context.WithTimeout(ctx, timeoutAssetResolve)
	pickupURL, expectedBytes, err := e.backend.Asset.ResolveAsset(resolveCtx, payload.AssetID)
	cancel()
	if err != nil {
		return fmt.Errorf("smoke: asset resolve: %w", err)
	}
	if pickupURL == "" {
		return errors.New("smoke: asset resolver returned empty pickup_url")
	}
	// ── Phase 2: insert PENDING smoke_runs row ───────────────────
	runStart := e.backend.Now()
	runID := fmt.Sprintf("smoke-%s-%d", op.WorkerID, runStart.UnixNano())
	insertCtx, cancel := context.WithTimeout(ctx, timeoutSmokeInsert)
	err = e.backend.SmokeRuns.InsertSmokeRun(insertCtx, store.SmokeRun{
		RunID:       runID,
		WorkerID:    op.WorkerID,
		StartedAt:   runStart,
		FinishedAt:  runStart, // overwritten on terminal transition
		AssetID:     payload.AssetID,
		Status:      store.SmokeStatusPending,
		RequestedBy: op.RequestedBy,
	})
	cancel()
	if err != nil {
		return fmt.Errorf("smoke: insert PENDING: %w", err)
	}
	log.Printf("[SMOKE] worker=%s run=%s asset_id=%s pickup=%s QUEUED", op.WorkerID, runID, payload.AssetID, pickupURL)
	// ── Phase 3: acquire smoke lease ─────────────────────────────
	// Symmetric with Step 6/15's drain — Worker.Drain=true
	// excludes the worker from real-job placement for the smoke
	// duration. Deferred release fires on every return path so a
	// panic / cleanup-skipped state still recovers the worker.
	leaseCtx, cancel := context.WithTimeout(ctx, timeoutLeaseAcquire)
	err = e.backend.Lease.AcquireSmokeLease(leaseCtx, runID, op.WorkerID)
	cancel()
	if err != nil {
		finishedAt := e.backend.Now()
		durationMs := finishedAt.Sub(runStart).Milliseconds()
		_ = e.markFailed(ctx, runID, finishedAt, durationMs, fmt.Sprintf("%s: %v", ErrSmokeLeaseUnavailable.Error(), err))
		return fmt.Errorf("%w: %v", ErrSmokeLeaseUnavailable, err)
	}
	// Cleanup on every return path: lease release (mandatory)
	// + worker temp cleanup (best-effort, in runCleanupAndFail).
	defer func() {
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer releaseCancel()
		if err := e.backend.Lease.ReleaseSmokeLease(releaseCtx, runID); err != nil {
			log.Printf("[SMOKE] cleanup: lease release %s failed: %v", runID, err)
		}
	}()
	// ── Phase 4: download asset via worker exec ──────────────────
	// Pass the pickupURL resolved in Phase 1 so the worker downloads
	// the real asset bundle (not a synthetic ffmpeg-generated clip).
	destPath := fmt.Sprintf("/var/lib/velox-worker/smoke/%s.in", runID)
	dlCtx, cancel := context.WithTimeout(ctx, timeoutAssetDownload)
	err = e.backend.Worker.DownloadAsset(dlCtx, runID, op.WorkerID, pickupURL, destPath)
	cancel()
	if err != nil {
		return e.runCleanupAndFail(ctx, runID, op.WorkerID, runStart,
			fmt.Sprintf("%s: %v (after %s)", ErrAssetDownloadFail.Error(), err, "lease_acquired"))
	}
	// The resolver's expectedBytes describes the downloaded input asset;
	// the rendered output has independent size semantics. The output's
	// authoritative byte count is verified by the artifact verifier and
	// uploader at the upload boundary.
	_ = expectedBytes
	// ── Phase 5: ffmpeg render via worker exec ───────────────────
	outputPath := fmt.Sprintf("/var/lib/velox-worker/smoke/%s.mp4", runID)
	renderCtx, cancel := context.WithTimeout(ctx, timeoutFFmpegRender)
	outputPathReturned, artifactBytes, err := e.backend.Worker.RunFFmpegRender(renderCtx, runID, op.WorkerID, payload.RenderPlan, outputPath)
	if outputPathReturned != "" && outputPathReturned != outputPath {
		log.Printf("[SMOKE] worker=%s run=%s render path mismatch: requested=%s actual=%s (using actual)", op.WorkerID, runID, outputPath, outputPathReturned)
	}
	cancel()
	if err != nil {
		return e.runCleanupAndFail(ctx, runID, op.WorkerID, runStart,
			fmt.Sprintf("%s: %v (after %s)", ErrFFmpegRenderFail.Error(), err, "asset_downloaded"))
	}
	if artifactBytes == 0 {
		return e.runCleanupAndFail(ctx, runID, op.WorkerID, runStart,
			fmt.Sprintf("%s: ffmpeg exited 0 but artifact is empty (after %s)", ErrArtifactMissing.Error(), "render"))
	}
	// ── Phase 5b: verify artifact bytes/container/hash -------------
	verifyCtx, cancel := context.WithTimeout(ctx, timeoutFFmpegRender)
	artifactSHA256, err := e.backend.Verifier.VerifyArtifact(verifyCtx, artifactPathOr(outputPathReturned, outputPath), artifactBytes)
	cancel()
	if err != nil {
		return e.runCleanupAndFail(ctx, runID, op.WorkerID, runStart,
			fmt.Sprintf("%s: %v (after %s)", ErrArtifactMissing.Error(), err, "artifact_verify"))
	}
	log.Printf("[SMOKE] worker=%s run=%s artifact_bytes=%d sha256=%s VERIFIED",
		op.WorkerID, runID, artifactBytes, artifactSHA256)
	// ── Phase 6: upload artifact to Drive ────────────────────────
	// Use the path returned by RunFFmpegRender (may differ from
	// outputPath when the adapter fetches the artifact locally, e.g.
	// SSHWorkerExec downloads via base64 to a local temp path).
	artifactPath := outputPathReturned
	if artifactPath == "" {
		artifactPath = outputPath
	}
	upCtx, cancel := context.WithTimeout(ctx, timeoutDriveUpload)
	driveFileID, err := e.backend.Drive.UploadArtifact(upCtx, runID, artifactPath, artifactBytes, artifactSHA256)
	cancel()
	if err != nil {
		return e.runCleanupAndFail(ctx, runID, op.WorkerID, runStart,
			fmt.Sprintf("%s: %v (after %s)", ErrDriveUploadFail.Error(), err, "ffmpeg_render"))
	}
	if driveFileID == "" {
		return e.runCleanupAndFail(ctx, runID, op.WorkerID, runStart,
			fmt.Sprintf("%s: drive uploader returned empty file_id (sha256=%s)", ErrDriveUploadFail.Error(), artifactSHA256))
	}
	// ── Phase 7: mark SUCCEEDED on smoke_runs row ─────────────────
	finishedAt := e.backend.Now()
	durationMs := finishedAt.Sub(runStart).Milliseconds()
	finalCtx, cancel := context.WithTimeout(ctx, timeoutSmokeFinal)
	err = e.backend.SmokeRuns.MarkSmokeSucceeded(finalCtx, runID, finishedAt, durationMs, driveFileID)
	cancel()
	if err != nil {
		return fmt.Errorf("smoke: mark SUCCEEDED for %s: %w", runID, err)
	}
	// ── Forward-cleanup: best-effort worker temp removal ─────────
	// Done AFTER mark SUCCEEDED because the artifact_id column
	// is now in the smoke_runs row; even if cleanup fails the
	// operator can find the artifact via Drive.
	cleanupCtx, cancel := context.WithTimeout(context.Background(), timeoutCleanupWorker)
	if err := e.backend.Worker.CleanupWorkerTemp(cleanupCtx, runID, op.WorkerID); err != nil {
		log.Printf("[SMOKE] cleanup: worker temp cleanup %s failed: %v (non-fatal)", runID, err)
	}
	cancel()
	log.Printf("[SMOKE] worker=%s run=%s duration_ms=%d artifact_drive_id=%s SUCCEEDED",
		op.WorkerID, runID, durationMs, driveFileID)
	return nil
}

func isNilSmokeDependency(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func artifactPathOr(returned, fallback string) string {
	if returned != "" {
		return returned
	}
	return fallback
}

// parsePayload unwraps op.Payload into the SmokePayload schema.
// Returns (payload, error). Empty / "{}" payload fails the
// "asset_id required" check; malformed JSON returns a parse error.
func (e *LevelDSmokeExecutor) parsePayload(op *store.Operation) (SmokePayload, error) {
	if len(op.Payload) == 0 || string(op.Payload) == "{}" {
		return SmokePayload{}, errors.New("smoke: payload empty (asset_id required)")
	}
	var p SmokePayload
	if err := json.Unmarshal(op.Payload, &p); err != nil {
		return p, fmt.Errorf("smoke: payload parse: %w", err)
	}
	if p.AssetID == "" {
		return p, errors.New("smoke: asset_id missing")
	}
	p.AssetID = strings.TrimSpace(p.AssetID)
	p.RenderPlan = strings.TrimSpace(p.RenderPlan)
	return p, nil
}

// runCleanupAndFail is the cleanup-cascade entrypoint used by
// Phase 4-6 forward failures. (Phase 1-3 failures short-circuit
// before lease acquisition so no cleanup is needed beyond
// markFailed.)
//
// Sequence:
//  1. Mark smoke_runs row FAILED with duration_ms + phaseErr.
//  2. Best-effort worker temp cleanup (warning-only on failure).
//
// Lease release is handled by the deferred func at Phase 3 —
// not duplicated here.
//
// Returns ErrSmokePipelineFailed wrap so the audit dashboard's
// error_message surface reads "<phase>: <err> (duration_ms=N)".
func (e *LevelDSmokeExecutor) runCleanupAndFail(ctx context.Context, runID, workerID string, runStart time.Time, phaseErr string) error {
	finishedAt := e.backend.Now()
	durationMs := finishedAt.Sub(runStart).Milliseconds()
	if err := e.markFailed(ctx, runID, finishedAt, durationMs, phaseErr); err != nil {
		log.Printf("[SMOKE] mark FAILED for %s: %v", runID, err)
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), timeoutCleanupWorker)
	defer cancel()
	if err := e.backend.Worker.CleanupWorkerTemp(cleanupCtx, runID, workerID); err != nil {
		log.Printf("[SMOKE] cleanup: worker temp cleanup %s failed: %v (non-fatal post-fail)", runID, err)
	}
	return fmt.Errorf("%w: %s (duration_ms=%d)", ErrSmokePipelineFailed, phaseErr, durationMs)
}

// markFailed is the small helper that bridges "forward-failed
// at Phase <X>" to the smoke_runs FAILED terminal UPDATE. Used
// both from Phase 3 (lease unavailable, before lease acquired)
// and from runCleanupAndFail (Phase 4-6, after lease acquired).
func (e *LevelDSmokeExecutor) markFailed(ctx context.Context, runID string, finishedAt time.Time, durationMs int64, phaseErr string) error {
	finalCtx, cancel := context.WithTimeout(ctx, timeoutSmokeFinal)
	defer cancel()
	return e.backend.SmokeRuns.MarkSmokeFailed(finalCtx, runID, finishedAt, durationMs, phaseErr)
}
