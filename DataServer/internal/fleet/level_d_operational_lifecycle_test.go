package fleet

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"velox-server/internal/artifacts"
	"velox-server/internal/store"
)

func TestLevelDSmoke_ControllerLifecyclePersistsVerifiedArtifactEvidence(t *testing.T) {
	requireMediaTools(t)
	db, err := store.NewSQLiteStore(t.TempDir() + "/smoke-lifecycle.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.CreateFleetOperationsTableIfNotExists(); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateSmokeRunsTableIfNotExists(); err != nil {
		t.Fatal(err)
	}

	fixture := filepath.Join(t.TempDir(), "fixture.mp4")
	if output, err := exec.Command("ffmpeg", "-v", "error", "-y", "-f", "lavfi", "-i", "color=c=blue:size=160x120:d=1", "-c:v", "libx264", "-pix_fmt", "yuv420p", fixture).CombinedOutput(); err != nil {
		t.Fatalf("create fixture: %v: %s", err, output)
	}
	durableBlob := filepath.Join(t.TempDir(), "durable", "smoke.mp4")
	backend, lease, _, _, _, _ := fullBackend(t)
	worker := &realArtifactWorker{root: t.TempDir(), fixture: fixture}
	backend.Worker = worker
	backend.Asset = stubAsset{url: "file://" + fixture, bytes: 0}
	backend.Verifier = NewFFprobeArtifactVerifier()
	drive := &hashCheckingDrive{durablePath: durableBlob}
	backend.Drive = drive
	backend.SmokeRuns = db
	backend.Now = func() time.Time { return time.Now().UTC() }

	registry := NewExecutorRegistry()
	if err := registry.Register(OperationKindSmoke, NewLevelDSmokeExecutor(backend)); err != nil {
		t.Fatal(err)
	}
	controller := NewFleetController(db, registry, 10*time.Millisecond, time.Minute)
	payload, _ := json.Marshal(SmokePayload{AssetID: "asset-canary-001", RenderPlan: "real-level-d"})
	op := &store.Operation{
		OperationID: "op-level-d-real",
		WorkerID:    "worker-level-d-real",
		Op:          OperationKindSmoke,
		RequestedBy: "test",
		Reason:      "real Level D lifecycle",
		Payload:     payload,
		QueuedAt:    time.Now().UTC(),
		Status:      store.OperationStatusQueued,
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	if err := controller.Start(runCtx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		controller.Stop()
		<-controller.Done()
	})
	if err := controller.PublishOperation(context.Background(), op); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	var storedOp *store.Operation
	for time.Now().Before(deadline) {
		storedOp, err = db.GetOperation(context.Background(), op.OperationID)
		if err == nil && storedOp.Status == store.OperationStatusSucceeded {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if storedOp == nil || storedOp.Status != store.OperationStatusSucceeded || storedOp.StartedAt == nil || storedOp.FinishedAt == nil {
		t.Fatalf("operation lifecycle=%+v, want async SUCCEEDED with timestamps", storedOp)
	}
	run, err := db.GetLatestSmokeForWorker(context.Background(), op.WorkerID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != SmokeStatusSucceeded || run.ArtifactDriveID == "" {
		t.Fatalf("smoke evidence=%+v, want SUCCEEDED with artifact id", run)
	}
	if drive.bytes <= 0 || len(drive.hash) != 64 {
		t.Fatalf("uploaded artifact evidence bytes=%d hash=%q", drive.bytes, drive.hash)
	}
	if drive.uploadedPath != worker.artifact {
		t.Fatalf("upload path=%q, want rendered artifact path=%q", drive.uploadedPath, worker.artifact)
	}
	durableInfo, err := os.Stat(durableBlob)
	if err != nil || durableInfo.Size() != drive.bytes {
		t.Fatalf("durable uploaded blob stat=%v info=%v, want size=%d", err, durableInfo, drive.bytes)
	}
	durableBytes, err := os.ReadFile(durableBlob)
	if err != nil {
		t.Fatal(err)
	}
	durableSum := sha256.Sum256(durableBytes)
	if got := hex.EncodeToString(durableSum[:]); got != drive.hash {
		t.Fatalf("durable blob hash=%q, want uploaded hash=%q", got, drive.hash)
	}
	if _, err := os.Stat(worker.artifact); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary artifact survived cleanup: path=%s err=%v", worker.artifact, err)
	}
	if lease.releaseCount() != 1 {
		t.Fatalf("lease releases=%d want 1", lease.releaseCount())
	}

	// Bind canonical finalization to the exact smoke run and durable
	// bytes uploaded above; this is not a second synthetic artifact.
	artifactID := "artifact-" + run.RunID
	uploadID := "upload-" + run.RunID
	jobID := "job-" + run.RunID
	now := time.Now().UTC().Format(time.RFC3339)
	_, err = db.DB().Exec(`
CREATE TABLE IF NOT EXISTS jobs (job_id TEXT PRIMARY KEY, status TEXT, revision INTEGER, completed_at TEXT, updated_at TEXT, migrated_at TEXT, request_json TEXT NOT NULL DEFAULT '{}');
CREATE TABLE IF NOT EXISTS tasks (task_id TEXT PRIMARY KEY, job_id TEXT, status TEXT, completed_at TEXT, updated_at TEXT, winning_attempt_id TEXT, winning_attempt_committed_at TEXT, winning_attempt_terminal_pending INTEGER DEFAULT 0, revision INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS artifacts (id TEXT PRIMARY KEY, job_id TEXT, attempt_id INTEGER, type TEXT, storage_provider TEXT, storage_key TEXT, storage_url TEXT, local_path TEXT, sha256 TEXT, size_bytes INTEGER, duration_seconds REAL, duration_ms INTEGER, mime_type TEXT, status TEXT, verified_at TEXT, created_at TEXT);
CREATE TABLE IF NOT EXISTS artifact_uploads (upload_id TEXT PRIMARY KEY, artifact_id TEXT, job_id TEXT, attempt_number INTEGER, worker_id TEXT, lease_id TEXT, status TEXT, temporary_storage_key TEXT, expected_size_bytes INTEGER, expected_sha256 TEXT, expected_revision INTEGER, received_size_bytes INTEGER, received_sha256 TEXT, created_at TEXT, expires_at TEXT, completed_at TEXT);`)
	if err != nil {
		t.Fatal(err)
	}
	requestJSON, _ := json.Marshal(map[string]bool{"render_only": true})
	if _, err := db.DB().Exec(`INSERT INTO jobs (job_id,status,revision,updated_at,migrated_at,request_json) VALUES (?, 'AWAITING_ARTIFACT', 0, ?, ?, ?)`, jobID, now, now, string(requestJSON)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.DB().Exec(`INSERT INTO artifacts (id,job_id,type,status,created_at) VALUES (?, ?, 'render', 'STAGING', ?)`, artifactID, jobID, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.DB().Exec(`INSERT INTO artifact_uploads (upload_id,artifact_id,job_id,attempt_number,worker_id,lease_id,status,expected_size_bytes,expected_sha256,expected_revision,temporary_storage_key,created_at,expires_at) VALUES (?, ?, ?, 1, ?, 'lease-level-d', 'FINALIZING', ?, ?, 0, ?, ?, ?)`, uploadID, artifactID, jobID, op.WorkerID, drive.bytes, drive.hash, "smoke-level-d-real", now, now); err != nil {
		t.Fatal(err)
	}
	writer := artifacts.NewSQLiteFinalizeWriter(store.NewSQLiteArtifactFinalizer(db.DB(), nil))
	if _, err := writer.FinalizeVerified(context.Background(), artifacts.FinalizeVerifiedCommand{
		UploadID: uploadID, ArtifactID: artifactID, JobID: jobID,
		WorkerID: op.WorkerID, LeaseID: "lease-level-d", AttemptNumber: 1,
		StorageProvider: "local", StorageKey: durableBlob,
		SHA256: drive.hash, SizeBytes: drive.bytes, MIMEType: "video/mp4", VerifiedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	var status, gotHash string
	var gotSize int64
	if err := db.DB().QueryRow(`SELECT status, sha256, size_bytes FROM artifacts WHERE id = ?`, artifactID).Scan(&status, &gotHash, &gotSize); err != nil {
		t.Fatal(err)
	}
	if status != "READY" || gotHash != drive.hash || gotSize != drive.bytes {
		t.Fatalf("canonical artifact status=%q hash=%q size=%d; want READY/%s/%d", status, gotHash, gotSize, drive.hash, drive.bytes)
	}
	var jobStatus string
	if err := db.DB().QueryRow(`SELECT status FROM jobs WHERE job_id = ?`, jobID).Scan(&jobStatus); err != nil {
		t.Fatal(err)
	}
	if jobStatus != "SUCCEEDED" {
		t.Fatalf("canonical job status=%q, want SUCCEEDED after verified finalization", jobStatus)
	}
	var uploadStatus string
	if err := db.DB().QueryRow(`SELECT status FROM artifact_uploads WHERE upload_id = ?`, uploadID).Scan(&uploadStatus); err != nil {
		t.Fatal(err)
	}
	if uploadStatus != "COMPLETED" {
		t.Fatalf("canonical upload status=%q, want COMPLETED", uploadStatus)
	}
}
