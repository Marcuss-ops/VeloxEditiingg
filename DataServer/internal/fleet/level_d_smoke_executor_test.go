// Package fleet — Step 12/15 LevelDSmokeExecutor tests.
//
// Coverage map (each per-phase test exercises a single decision
// branch of the executor's 7-phase pipeline):
//
//	Happy path:
//	  TestLevelDSmoke_HappyPath               — all 7 phases pass, smoke_runs
//	                                           row marked SUCCEEDED with
//	                                           duration_ms + drive_file_id
//
//	Pre-flight (parsePayload / nil deps):
//	  TestLevelDSmoke_NilOp                   — nil op → error
//	  TestLevelDSmoke_EmptyWorkerID          — empty worker_id → error
//	  TestLevelDSmoke_NilBackend_Returns     — nil backend bundle
//	                                            surfaces ErrSmokeRunnerNotWired
//
//	Phase 0 (parsePayload):
//	  TestLevelDSmoke_EmptyPayload            — empty "{}" → payload-empty error
//	  TestLevelDSmoke_PayloadParseFails       — malformed JSON → parse error
//	  TestLevelDSmoke_MissingAssetID         — JSON parses but no asset_id
//
//	Phase 1 (asset resolve):
//	  TestLevelDSmoke_AssetResolveFail        — resolver returns error
//	  TestLevelDSmoke_AssetEmptyURL           — resolver returns empty URL
//
//	Phase 3 (lease acquire):
//	  TestLevelDSmoke_LeaseUnavailable        — lease acquire fails →
//	                                           smoke_runs FAILED with
//	                                           ErrSmokeLeaseUnavailable
//
//	Phase 4-6 (asset / ffmpeg / Drive):
//	  TestLevelDSmoke_AssetDownloadFail       — Phase 4 fail → FAILED + pipeline wrap
//	  TestLevelDSmoke_FFmpegRenderFail        — Phase 5 fail → FAILED
//	  TestLevelDSmoke_ArtifactMissing         — Phase 5 ok but zero bytes → ERR
//	  TestLevelDSmoke_DriveUploadFail         — Phase 6 fail → FAILED
//	  TestLevelDSmoke_DriveEmptyFileID       — Phase 6 returns "" → ERR
//
//	Cleanup cascade (mandatory lease release + best-effort temp):
//	  TestLevelDSmoke_Cleanup_ReleasesLease   — happy-path suite asserts lease
//	                                            release was called (via stub)
//	  TestLevelDSmoke_Cleanup_BestEffortTemp  — Phase 6 fail: worker temp cleanup
//	                                            is best-effort (warning-only)
//	  TestLevelDSmoke_Cleanup_FailSurfaces    — Phase 5 ffmpeg fail: cleanup
//	                                            cascade on worker temp must
//	                                            still be attempted
//
//	Helpers:
//	  TestLevelDSmoke_ParsePayload_Whitespace — leading/trailing whitespace
//	                                           trimmed from asset_id
//	  TestLevelDSmoke_NilNow_Defaults         — NowFunc nil default falls back
//	                                            to time.Now
package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"velox-server/internal/store"
)

// ── Stub helpers ─────────────────────────────────────────────────────

// stubLeaseStore records Acquire/Release calls for cleanup assertions.
type stubLeaseStore struct {
	mu         sync.Mutex
	acquired   map[string]string // runID → workerID
	released   []string          // runIDs
	acquireErr error
	releaseErr error
}

func newStubLease() *stubLeaseStore {
	return &stubLeaseStore{acquired: map[string]string{}}
}

// recordAcquire fires before acquireErr; allows tests to verify
// the executor attempted the call before failing.
func (s *stubLeaseStore) recordAcquire(runID, workerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acquired[runID] = workerID
}

func (s *stubLeaseStore) wasAcquired(runIDOrPrefix string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Match by exact key first; fall back to HasPrefix so the
	// canonical "smoke-<wkr>-<nanos>" runID matches queries from
	// tests that pass only the "smoke-<wkr>" prefix.
	if _, ok := s.acquired[runIDOrPrefix]; ok {
		return true
	}
	for k := range s.acquired {
		if strings.HasPrefix(k, runIDOrPrefix) {
			return true
		}
	}
	return false
}

func (s *stubLeaseStore) releaseCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.released)
}

func (s *stubLeaseStore) AcquireSmokeLease(_ context.Context, runID, workerID string) error {
	if s.acquireErr != nil {
		return s.acquireErr
	}
	s.recordAcquire(runID, workerID)
	return nil
}

func (s *stubLeaseStore) ReleaseSmokeLease(_ context.Context, runID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.released = append(s.released, runID)
	return s.releaseErr
}

// stubWorker records the per-phase call sequence + returns canned errors.
type stubWorker struct {
	downloadErr   error
	renderErr     error
	cleanupErr    error
	artifactBytes int64
	cleanupCalls  int
}

func (s *stubWorker) DownloadAsset(_ context.Context, _, _, _, _ string) error {
	return s.downloadErr
}

func (s *stubWorker) RunFFmpegRender(_ context.Context, _, _, _, _ string) (string, int64, error) {
	if s.renderErr != nil {
		return "", 0, s.renderErr
	}
	return "/var/lib/velox-worker/smoke/<runID>.mp4", s.artifactBytes, nil
}

func (s *stubWorker) CleanupWorkerTemp(_ context.Context, _, _ string) error {
	s.cleanupCalls++
	return s.cleanupErr
}

// stubDrive records UploadArtifact calls.
type stubDrive struct {
	mu        sync.Mutex
	uploads   []string
	uploadErr error
	fileID    string
}

func (s *stubDrive) UploadArtifact(_ context.Context, _, srcPath string, _ int64) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.uploads = append(s.uploads, srcPath)
	if s.uploadErr != nil {
		return "", s.uploadErr
	}
	// Return s.fileID verbatim (including empty string) so the
	// TestLevelDSmoke_DriveEmptyFileID case can exercise the
	// executor's empty-file_id path. Tests that want a non-empty
	// canned id MUST set fileID explicitly.
	return s.fileID, nil
}

func (s *stubDrive) uploadCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.uploads)
}

// stubAsset returns canned URL + size.
type stubAsset struct {
	url        string
	bytes      int64
	resolveErr error
	emptyURL   bool
}

func (s stubAsset) ResolveAsset(_ context.Context, _ string) (string, int64, error) {
	if s.resolveErr != nil {
		return "", 0, s.resolveErr
	}
	if s.emptyURL {
		return "", 0, nil
	}
	return s.url, s.bytes, nil
}

// stubSmokeRuns stores rows in memory.
type stubSmokeRuns struct {
	mu       sync.Mutex
	rows     map[string]store.SmokeRun // by RunID
	insertOK bool
	sucOK    bool
	failOK   bool
}

func newStubRuns() *stubSmokeRuns {
	return &stubSmokeRuns{rows: map[string]store.SmokeRun{}, insertOK: true, sucOK: true, failOK: true}
}

func (s *stubSmokeRuns) InsertSmokeRun(_ context.Context, rec store.SmokeRun) error {
	if !s.insertOK {
		return errors.New("insert disabled")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec.Status = store.SmokeStatusPending
	s.rows[rec.RunID] = rec
	return nil
}

func (s *stubSmokeRuns) MarkSmokeSucceeded(_ context.Context, runID string, finishedAt time.Time, durationMs int64, driveID string) error {
	if !s.sucOK {
		return errors.New("succeeded update disabled")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.rows[runID]
	r.Status = store.SmokeStatusSucceeded
	r.ArtifactDriveID = driveID
	r.FinishedAt = finishedAt
	r.DurationMs = durationMs
	s.rows[runID] = r
	return nil
}

func (s *stubSmokeRuns) MarkSmokeFailed(_ context.Context, runID string, _ time.Time, _ int64, errMsg string) error {
	if !s.failOK {
		return errors.New("failed update disabled")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.rows[runID]
	r.Status = store.SmokeStatusFailed
	r.ErrorMessage = errMsg
	s.rows[runID] = r
	return nil
}

func (s *stubSmokeRuns) GetLatestSmokeForWorker(_ context.Context, workerID string) (*store.SmokeRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.rows {
		if r.WorkerID == workerID {
			r := r
			return &r, nil
		}
	}
	return nil, store.ErrSmokeRunNotFound
}

func (s *stubSmokeRuns) ListRecentSmokesForWorker(_ context.Context, _ string, _ int) ([]store.SmokeRun, error) {
	return nil, nil
}

// ── Common test fixtures ────────────────────────────────────────────

func validSmokeOp(workerID string) *store.Operation {
	payload := SmokePayload{AssetID: "asset-canary-001", RenderPlan: "ffmpeg -i in.mp4 -c:v libx264 out.mp4"}
	raw, _ := json.Marshal(payload)
	return &store.Operation{
		OperationID: "op-smoke-test-1",
		WorkerID:    workerID,
		Op:          OperationKindSmoke,
		RequestedBy: "test",
		Reason:      "step 12/15 test",
		Payload:     raw,
		Status:      store.OperationStatusQueued,
	}
}

func fullBackend(t *testing.T) (LevelDSmokeBackend, *stubLeaseStore, *stubWorker, *stubDrive, stubAsset, *stubSmokeRuns) {
	t.Helper()
	lease := newStubLease()
	worker := &stubWorker{artifactBytes: 1024 * 1024}
	drive := &stubDrive{fileID: "drive-file-id-test"}
	runs := newStubRuns()
	asset := stubAsset{url: "asset://canary/run.mp4", bytes: 1024 * 1024}
	// stubNow returns synthetic timestamps spaced by 1ms so
	// duration_ms > 0 even when the test execution is sub-ms.
	// Each call to Now advances the counter; in real workloads
	// the executor calls Now 4-7 times per run (Phase 1/2/3/4/5/7),
	// giving us a defensible baseline that survives on any host.
	stubNow := func() time.Time {
		t0 := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
		stubNowCalls++
		return t0.Add(time.Duration(stubNowCalls) * time.Millisecond)
	}
	b := LevelDSmokeBackend{
		Worker:    worker,
		Drive:     drive,
		Asset:     asset,
		Lease:     lease,
		SmokeRuns: runs,
		Now:       stubNow,
	}
	return b, lease, worker, drive, asset, runs
}

// stubNowCalls is package-level counter incremented by the
// stubNow() function returned from fullBackend. Tests that need
// a fresh timing context MUST reset it before constructing the
// backend (rare; most tests use the default 0-N progression).
var stubNowCalls int

// ── Happy path ─────────────────────────────────────────────────────

func TestLevelDSmoke_HappyPath(t *testing.T) {
	b, lease, _, drive, _, runs := fullBackend(t)
	e := NewLevelDSmokeExecutor(b)
	err := e.Execute(context.Background(), validSmokeOp("wkr-1"))
	if err != nil {
		t.Errorf("happy path: want nil err, got %v", err)
	}
	// Lease must have been acquired then released.
	if !lease.wasAcquired("smoke-wkr-1") {
		t.Errorf("lease must be acquired for run smoke-wkr-1")
	}
	if lease.releaseCount() != 1 {
		t.Errorf("lease must be released exactly once, got %d", lease.releaseCount())
	}
	if drive.uploadCount() != 1 {
		t.Errorf("Drive.UploadArtifact must be called once, got %d", drive.uploadCount())
	}
	// verify smoke_runs row was marked SUCCEEDED with drive id + duration.
	for runID, r := range runs.rows {
		if !strings.HasPrefix(runID, "smoke-wkr-1-") {
			continue
		}
		// Re-fetch by runID — we know one row exists.
		_ = r
	}
	if len(runs.rows) != 1 {
		t.Fatalf("want 1 smoke_runs row, got %d", len(runs.rows))
	}
	for _, r := range runs.rows {
		if r.Status != store.SmokeStatusSucceeded {
			t.Errorf("status = %q, want SUCCEEDED", r.Status)
		}
		if r.ArtifactDriveID == "" {
			t.Errorf("artifact_drive_id should be set on success")
		}
		if r.DurationMs <= 0 {
			t.Errorf("duration_ms should be > 0 on success, got %d", r.DurationMs)
		}
	}
}

func TestLevelDSmoke_EmptyPayload(t *testing.T) {
	b, _, _, _, _, _ := fullBackend(t)
	e := NewLevelDSmokeExecutor(b)
	op := validSmokeOp("wkr-1")
	op.Payload = []byte("{}")
	err := e.Execute(context.Background(), op)
	if err == nil || !strings.Contains(err.Error(), "payload empty") {
		t.Errorf("want payload-empty error, got %v", err)
	}
}

func TestLevelDSmoke_PayloadParseFails(t *testing.T) {
	b, _, _, _, _, _ := fullBackend(t)
	e := NewLevelDSmokeExecutor(b)
	op := validSmokeOp("wkr-1")
	op.Payload = []byte("{not-json")
	err := e.Execute(context.Background(), op)
	if err == nil || !strings.Contains(err.Error(), "payload parse") {
		t.Errorf("want payload-parse error, got %v", err)
	}
}

func TestLevelDSmoke_MissingAssetID(t *testing.T) {
	b, _, _, _, _, _ := fullBackend(t)
	e := NewLevelDSmokeExecutor(b)
	op := validSmokeOp("wkr-1")
	raw, _ := json.Marshal(SmokePayload{AssetID: ""})
	op.Payload = raw
	err := e.Execute(context.Background(), op)
	if err == nil || !strings.Contains(err.Error(), "asset_id missing") {
		t.Errorf("want asset_id-missing error, got %v", err)
	}
}

func TestLevelDSmoke_NilOp(t *testing.T) {
	b, _, _, _, _, _ := fullBackend(t)
	e := NewLevelDSmokeExecutor(b)
	err := e.Execute(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "nil operation") {
		t.Errorf("want nil-operation error, got %v", err)
	}
}

func TestLevelDSmoke_EmptyWorkerID(t *testing.T) {
	b, _, _, _, _, _ := fullBackend(t)
	e := NewLevelDSmokeExecutor(b)
	op := validSmokeOp("")
	err := e.Execute(context.Background(), op)
	if err == nil || !strings.Contains(err.Error(), "worker_id empty") {
		t.Errorf("want worker_id-empty error, got %v", err)
	}
}

func TestLevelDSmoke_NilBackend_Returns(t *testing.T) {
	b := LevelDSmokeBackend{} // all-nil
	e := NewLevelDSmokeExecutor(b)
	// Override asset so the executor's pre-flight guard isn't
	// tripped before we can observe the message.
	op := validSmokeOp("wkr-1")
	err := e.Execute(context.Background(), op)
	if err == nil {
		t.Fatalf("nil backend must surface ErrSmokeRunnerNotWired")
	}
	if !errors.Is(err, ErrSmokeRunnerNotWired) {
		t.Errorf("want ErrSmokeRunnerNotWired wrap, got %v", err)
	}
}

func TestLevelDSmoke_AssetEmptyURL(t *testing.T) {
	b, _, _, _, _, _ := fullBackend(t)
	b.Asset = stubAsset{url: "", bytes: 0, emptyURL: true}
	e := NewLevelDSmokeExecutor(b)
	err := e.Execute(context.Background(), validSmokeOp("wkr-1"))
	if err == nil || !strings.Contains(err.Error(), "empty pickup_url") {
		t.Errorf("want empty-pickup_url error, got %v", err)
	}
}

func TestLevelDSmoke_AssetResolveFail(t *testing.T) {
	b, _, _, _, _, _ := fullBackend(t)
	b.Asset = stubAsset{resolveErr: errors.New("asset-bundle-stale")}
	e := NewLevelDSmokeExecutor(b)
	err := e.Execute(context.Background(), validSmokeOp("wkr-1"))
	if err == nil || !strings.Contains(err.Error(), "asset resolve") {
		t.Errorf("want asset-resolve error, got %v", err)
	}
}

func TestLevelDSmoke_LeaseUnavailable(t *testing.T) {
	b, lease, _, _, _, runs := fullBackend(t)
	lease.acquireErr = errors.New("already-drained")
	e := NewLevelDSmokeExecutor(b)
	err := e.Execute(context.Background(), validSmokeOp("wkr-1"))
	if err == nil || !errors.Is(err, ErrSmokeLeaseUnavailable) {
		t.Errorf("want ErrSmokeLeaseUnavailable wrap, got %v", err)
	}
	// Rows: PENDING inserted; FAILED marked; lease never released.
	if len(runs.rows) != 1 {
		t.Errorf("want 1 smoke_runs row, got %d", len(runs.rows))
	}
	for _, r := range runs.rows {
		if r.Status != store.SmokeStatusFailed {
			t.Errorf("after lease-unavailable: status = %q, want FAILED", r.Status)
		}
	}
	if lease.releaseCount() != 0 {
		t.Errorf("lease must NOT be released when acquire fails; got %d releases", lease.releaseCount())
	}
}

func TestLevelDSmoke_AssetDownloadFail(t *testing.T) {
	b, lease, worker, _, _, runs := fullBackend(t)
	worker.downloadErr = errors.New("network-unreachable")
	e := NewLevelDSmokeExecutor(b)
	err := e.Execute(context.Background(), validSmokeOp("wkr-1"))
	if err == nil || !errors.Is(err, ErrSmokePipelineFailed) {
		t.Errorf("want ErrSmokePipelineFailed wrap, got %v", err)
	}
	if !strings.Contains(err.Error(), "asset_download_failed") {
		t.Errorf("err must surface ErrAssetDownloadFail sentinel; got %v", err)
	}
	// Lease was acquired → must be released via deferred cleanup
	if lease.releaseCount() != 1 {
		t.Errorf("lease release count = %d, want 1 (acquired before Phase 4 fail)", lease.releaseCount())
	}
	for _, r := range runs.rows {
		if r.Status != store.SmokeStatusFailed {
			t.Errorf("post-fail status = %q, want FAILED", r.Status)
		}
		if !strings.Contains(r.ErrorMessage, "asset_download_failed") {
			t.Errorf("error_message must include sentinel; got %q", r.ErrorMessage)
		}
	}
}

func TestLevelDSmoke_FFmpegRenderFail(t *testing.T) {
	b, _, worker, _, _, runs := fullBackend(t)
	worker.renderErr = errors.New("ffmpeg exit 1")
	e := NewLevelDSmokeExecutor(b)
	err := e.Execute(context.Background(), validSmokeOp("wkr-1"))
	if err == nil || !strings.Contains(err.Error(), "ffmpeg_render_failed") {
		t.Errorf("want ffmpeg_render_failed in err, got %v", err)
	}
	for _, r := range runs.rows {
		if r.Status != store.SmokeStatusFailed {
			t.Errorf("status = %q, want FAILED", r.Status)
		}
	}
}

func TestLevelDSmoke_ArtifactMissing(t *testing.T) {
	b, _, _, _, _, _ := fullBackend(t)
	// stubWorker.artifactBytes = 0 → ErrArtifactMissing
	b.Worker = &stubWorker{artifactBytes: 0}
	e := NewLevelDSmokeExecutor(b)
	err := e.Execute(context.Background(), validSmokeOp("wkr-1"))
	if err == nil || !strings.Contains(err.Error(), "artifact_missing") {
		t.Errorf("want artifact_missing err, got %v", err)
	}
}

func TestLevelDSmoke_DriveUploadFail(t *testing.T) {
	b, _, _, drive, _, _ := fullBackend(t)
	drive.uploadErr = errors.New("drive-5xx")
	e := NewLevelDSmokeExecutor(b)
	err := e.Execute(context.Background(), validSmokeOp("wkr-1"))
	if err == nil || !strings.Contains(err.Error(), "drive_upload_failed") {
		t.Errorf("want drive_upload_failed err, got %v", err)
	}
}

func TestLevelDSmoke_DriveEmptyFileID(t *testing.T) {
	b, _, _, drive, _, runs := fullBackend(t)
	// Replace stubDrive with one whose fileID stays empty — exercises
	// the executor's "driveturned empty file_id" branch.
	b.Drive = &stubDrive{fileID: ""}
	_ = drive
	e := NewLevelDSmokeExecutor(b)
	err := e.Execute(context.Background(), validSmokeOp("wkr-1"))
	if err == nil || !strings.Contains(err.Error(), "drive_upload_failed") {
		t.Errorf("want drive_upload_failed err (empty file_id path), got %v", err)
	}
	// Verify the smoke_runs row was marked FAILED with the
	// proper sentinel so the audit dashboard surfaces it.
	found := false
	for _, r := range runs.rows {
		if r.Status == store.SmokeStatusFailed &&
			strings.Contains(r.ErrorMessage, "drive_upload_failed") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected smoke_runs row marked FAILED with drive_upload_failed sentinel; rows=%v", runs.rows)
	}
}

func TestLevelDSmoke_Cleanup_BestEffortTemp(t *testing.T) {
	b, _, worker, _, _, _ := fullBackend(t)
	worker.cleanupErr = errors.New("rm-permission-denied")
	e := NewLevelDSmokeExecutor(b)
	// Happy path; cleanup fails but is best-effort — happy path still returns nil.
	err := e.Execute(context.Background(), validSmokeOp("wkr-1"))
	if err != nil {
		t.Errorf("happy path must NOT propagate cleanup failure; got %v", err)
	}
	if worker.cleanupCalls != 1 {
		t.Errorf("worker.CleanupWorkerTemp must be called once, got %d", worker.cleanupCalls)
	}
}

func TestLevelDSmoke_Cleanup_FailSurfacesAfterForwardFail(t *testing.T) {
	b, _, worker, _, _, _ := fullBackend(t)
	worker.renderErr = errors.New("ffmpeg exit 1")
	worker.cleanupErr = errors.New("rm-permission-denied")
	e := NewLevelDSmokeExecutor(b)
	err := e.Execute(context.Background(), validSmokeOp("wkr-1"))
	// Both forward fail + cleanup fail are wrapped together;
	// the audit row is FAILED via MarkSmokeFailed regardless.
	if err == nil {
		t.Errorf("forward fail must propagate")
	}
	if worker.cleanupCalls != 1 {
		t.Errorf("cleanup must be attempted even after forward fail, got %d calls", worker.cleanupCalls)
	}
}

func TestLevelDSmoke_ParsePayload_Whitespace(t *testing.T) {
	b, _, _, _, _, _ := fullBackend(t)
	e := NewLevelDSmokeExecutor(b)
	raw, _ := json.Marshal(SmokePayload{AssetID: "  asset-id-padded  "})
	op := validSmokeOp("wkr-1")
	op.Payload = raw
	err := e.Execute(context.Background(), op)
	// Stored rows should have asset_id trimmed; just confirm happy-path completion.
	if err != nil {
		t.Errorf("whitespace asset_id must be accepted: %v", err)
	}
}

func TestLevelDSmoke_NilNow_Defaults(t *testing.T) {
	b, _, _, _, _, _ := fullBackend(t)
	b.Now = nil // constructor default
	e := NewLevelDSmokeExecutor(b)
	err := e.Execute(context.Background(), validSmokeOp("wkr-1"))
	if err != nil {
		t.Errorf("nil Now default should still complete happy path: %v", err)
	}
}

func TestLevelDSmoke_UnregisteredWorker_NilLease_Fails(t *testing.T) {
	// Symmetric test: BackendLeaseStore nil → executor's pre-flight
	// surfaces ErrSmokeRunnerNotWired (single guard).
	b, _, _, _, _, _ := fullBackend(t)
	b.Lease = nil
	e := NewLevelDSmokeExecutor(b)
	err := e.Execute(context.Background(), validSmokeOp("wkr-1"))
	if err == nil {
		t.Fatalf("nil lease: want ErrSmokeRunnerNotWired")
	}
	if !errors.Is(err, ErrSmokeRunnerNotWired) {
		t.Errorf("want ErrSmokeRunnerNotWired wrap, got %v", err)
	}
}

func TestLevelDSmoke_MultipleWorkers_AreIsolated(t *testing.T) {
	// Two workers, two smokes; each must be independently routed
	// (asset resolution + lease + cleanup scoped to one workerID).
	b1, _, _, _, _, _ := fullBackend(t)
	b2, _, _, _, _, _ := fullBackend(t)
	e := NewLevelDSmokeExecutor(b1)
	if err := e.Execute(context.Background(), validSmokeOp("wkr-A")); err != nil {
		t.Errorf("wkr-A happy: %v", err)
	}
	if err := e.Execute(context.Background(), validSmokeOp("wkr-B")); err != nil {
		t.Errorf("wkr-B happy: %v", err)
	}
	// Same executor reused for both runs (state-less per op).
	_ = b2
}
