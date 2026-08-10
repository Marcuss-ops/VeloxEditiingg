// Package fleet — LocalShellWorker + LocalFileDriveUploader
// (dev/staging BackendWorkerExec + BackendDriveUploader adapters).
//
// Split out of level_d_smoke_deps.go. See the parent file for the
// full Level D smoke dependency-surface contract.
package fleet

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

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
// A pickupURL starting with "asset://" or empty URL triggers the
// dev-mode fallback: a synthetic ffmpeg-generated clip.
func (w *LocalShellWorker) DownloadAsset(_ context.Context, runID, _, pickupURL, destPath string) error {
	if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
		return fmt.Errorf("smoke: mkdir for download: %w", err)
	}
	_ = os.MkdirAll(w.runDir(runID), 0755)

	// Production path: download the real asset from the pickup URL.
	// asset:// URLs are synthetic and are accepted only by the explicit
	// development smoke backend; production resolves a real pickup URL.
	if pickupURL != "" && !strings.HasPrefix(pickupURL, "asset://") {
		cmd := exec.Command("curl", "-sSL", "-o", destPath, pickupURL)
		out, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("smoke: curl download asset from %s: %w (output: %s)", pickupURL, err, string(out))
		}
		return nil
	}

	// Dev-mode fallback: generate a small test video via ffmpeg lavfi.
	// The synthetic asset carries one AAC audio track so the canonical
	// Level D media gate (audio_track_count >= 1, audio_codec=aac) is
	// exercised identically in dev and production.
	ffmpeg := w.FFmpegBin
	if ffmpeg == "" {
		ffmpeg = "ffmpeg"
	}
	cmd := exec.Command(ffmpeg,
		"-f", "lavfi", "-i", "color=c=blue:size=320x240:d=1",
		"-f", "lavfi", "-i", "anullsrc=r=48000:cl=stereo",
		"-c:v", "libx264", "-c:a", "aac", "-f", "mp4", "-t", "1", "-shortest", "-y", destPath,
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
			"-c:v", "libx264", "-c:a", "aac", "-t", "2", outputPath)
	} else {
		args = append(args,
			"-f", "lavfi", "-i", "color=c=red:size=320x240:d=2",
			"-f", "lavfi", "-i", "anullsrc=r=48000:cl=stereo",
			"-c:v", "libx264", "-c:a", "aac", "-t", "2", "-shortest", outputPath)
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
func (d *LocalFileDriveUploader) UploadArtifact(_ context.Context, runID, srcPath string, expectedBytes int64, expectedSHA256 string) (string, error) {
	if err := os.MkdirAll(LocalDriveRoot, 0755); err != nil {
		return "", fmt.Errorf("smoke: mkdir drive root: %w", err)
	}
	src, err := os.ReadFile(srcPath)
	if err != nil {
		return "", fmt.Errorf("%w: read artifact: %v", ErrDriveUploadFail, err)
	}
	if expectedBytes > 0 && int64(len(src)) != expectedBytes {
		return "", fmt.Errorf("%w: size=%d want=%d", ErrDriveUploadFail, len(src), expectedBytes)
	}
	sum := sha256.Sum256(src)
	gotSHA256 := hex.EncodeToString(sum[:])
	if expectedSHA256 != "" && gotSHA256 != expectedSHA256 {
		return "", fmt.Errorf("%w: sha256=%s want=%s", ErrDriveUploadFail, gotSHA256, expectedSHA256)
	}
	dst := filepath.Join(LocalDriveRoot, runID+".mp4")
	if err := os.WriteFile(dst, src, 0644); err != nil {
		return "", fmt.Errorf("%w: write local drive: %v", ErrDriveUploadFail, err)
	}
	fakeID := "local-drive-" + runID
	return fakeID, nil
}
