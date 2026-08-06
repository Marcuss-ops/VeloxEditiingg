package fleet

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

type realArtifactWorker struct {
	root       string
	artifact   string
	cleanupErr error
}

func (w *realArtifactWorker) DownloadAsset(_ context.Context, runID, _, _, _ string) error {
	return os.MkdirAll(filepath.Join(w.root, runID), 0o755)
}

func (w *realArtifactWorker) RunFFmpegRender(ctx context.Context, runID, _, _, _ string) (string, int64, error) {
	if err := os.MkdirAll(filepath.Join(w.root, runID), 0o755); err != nil {
		return "", 0, err
	}
	path := filepath.Join(w.root, runID, "render.mp4")
	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-v", "error", "-y",
		"-f", "lavfi", "-i", "color=c=blue:size=160x120:d=1",
		"-c:v", "libx264", "-pix_fmt", "yuv420p", path,
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		return "", 0, errors.New(string(output))
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", 0, err
	}
	w.artifact = path
	return path, info.Size(), nil
}

func (w *realArtifactWorker) CleanupWorkerTemp(_ context.Context, runID, _ string) error {
	if w.cleanupErr != nil {
		return w.cleanupErr
	}
	return os.RemoveAll(filepath.Join(w.root, runID))
}

type hashCheckingDrive struct {
	uploadedPath string
	hash         string
	bytes        int64
}

func (d *hashCheckingDrive) UploadArtifact(_ context.Context, _, path string, expectedBytes int64, expectedSHA256 string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	if int64(len(data)) != expectedBytes {
		return "", errors.New("upload size mismatch")
	}
	sum := sha256.Sum256(data)
	d.uploadedPath = path
	d.hash = hex.EncodeToString(sum[:])
	if expectedSHA256 != "" && d.hash != expectedSHA256 {
		return "", errors.New("upload sha256 mismatch")
	}
	d.bytes = int64(len(data))
	return "verified-artifact-" + d.hash[:12], nil
}

func requireMediaTools(t *testing.T) {
	t.Helper()
	for _, tool := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s unavailable: %v", tool, err)
		}
	}
}

func TestFFprobeArtifactVerifier_RealMP4HashAndSize(t *testing.T) {
	requireMediaTools(t)
	root := t.TempDir()
	path := filepath.Join(root, "fixture.mp4")
	cmd := exec.Command("ffmpeg", "-v", "error", "-y", "-f", "lavfi", "-i", "color=c=red:size=160x120:d=1", "-c:v", "libx264", "-pix_fmt", "yuv420p", path)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("create fixture: %v: %s", err, output)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	verifier := NewFFprobeArtifactVerifier()
	got, err := verifier.VerifyArtifact(context.Background(), path, info.Size())
	if err != nil {
		t.Fatalf("VerifyArtifact: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if got != hex.EncodeToString(sum[:]) {
		t.Fatalf("hash=%s want=%s", got, hex.EncodeToString(sum[:]))
	}
	if len(got) != 64 || strings.ToLower(got) != got {
		t.Fatalf("hash is not canonical lowercase sha256: %q", got)
	}
}

func TestLevelDSmoke_RealArtifactVerifiedBeforeSuccessAndCleaned(t *testing.T) {
	requireMediaTools(t)
	backend, lease, _, _, _, runs := fullBackend(t)
	worker := &realArtifactWorker{root: t.TempDir()}
	drive := &hashCheckingDrive{}
	backend.Worker = worker
	backend.Asset = stubAsset{url: "asset://canary/run.mp4", bytes: 0}
	backend.Drive = drive
	backend.Verifier = NewFFprobeArtifactVerifier()

	exec := NewLevelDSmokeExecutor(backend)
	if err := exec.Execute(context.Background(), validSmokeOp("wkr-real-artifact")); err != nil {
		t.Fatalf("Level D execute: %v", err)
	}
	if lease.releaseCount() != 1 {
		t.Fatalf("lease releases=%d want 1", lease.releaseCount())
	}
	if drive.bytes <= 0 || drive.hash == "" {
		t.Fatalf("upload did not receive verified artifact: %+v", drive)
	}
	if len(runs.rows) != 1 {
		t.Fatalf("smoke rows=%d want 1", len(runs.rows))
	}
	for _, run := range runs.rows {
		if run.Status != SmokeStatusSucceeded || run.ArtifactDriveID == "" {
			t.Fatalf("smoke row is not successful artifact evidence: %+v", run)
		}
	}
	if _, err := os.Stat(worker.artifact); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary artifact still exists after cleanup: path=%s err=%v", worker.artifact, err)
	}
}

func TestLevelDSmoke_RealVerifierRejectsSizeMismatch(t *testing.T) {
	requireMediaTools(t)
	root := t.TempDir()
	path := filepath.Join(root, "fixture.mp4")
	cmd := exec.Command("ffmpeg", "-v", "error", "-y", "-f", "lavfi", "-i", "color=c=green:size=160x120:d=1", "-c:v", "libx264", "-pix_fmt", "yuv420p", path)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("create fixture: %v: %s", err, output)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewFFprobeArtifactVerifier().VerifyArtifact(context.Background(), path, info.Size()+1)
	if err == nil || !strings.Contains(err.Error(), "size=") {
		t.Fatalf("size mismatch error=%v, want size diagnostic", err)
	}
}
