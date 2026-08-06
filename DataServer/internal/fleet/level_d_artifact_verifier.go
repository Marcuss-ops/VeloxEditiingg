package fleet

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// BackendArtifactVerifier validates the rendered artifact before the smoke
// pipeline records success or uploads it. The verifier runs against the path
// returned by the worker adapter, so both local and SSH-backed smoke paths
// share the same media/hash gate.
type BackendArtifactVerifier interface {
	VerifyArtifact(ctx context.Context, path string, expectedBytes int64) (sha256Hex string, err error)
}

// FFprobeArtifactVerifier is the real Level D verifier used by staging and
// production. It rejects missing/empty/truncated files, computes the master
// SHA-256, and requires ffprobe to parse a positive-duration media container.
type FFprobeArtifactVerifier struct {
	FFprobeBin string
}

// NewFFprobeArtifactVerifier returns the default verifier using ffprobe from
// PATH. The binary is resolved at execution time so bootstrap remains cheap.
func NewFFprobeArtifactVerifier() *FFprobeArtifactVerifier {
	return &FFprobeArtifactVerifier{FFprobeBin: "ffprobe"}
}

// VerifyLocalArtifactDigest validates the exact bytes an uploader is about
// to send. It is shared by the production Drive adapter and local test
// uploader so the post-verifier upload boundary has one implementation.
func VerifyLocalArtifactDigest(path string, expectedBytes int64, expectedSHA256 string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("artifact upload verification: stat %s: %w", path, err)
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 {
		return fmt.Errorf("artifact upload verification: %s is not a non-empty regular file", path)
	}
	if expectedBytes > 0 && info.Size() != expectedBytes {
		return fmt.Errorf("artifact upload verification: size=%d want=%d", info.Size(), expectedBytes)
	}
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("artifact upload verification: open %s: %w", path, err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return fmt.Errorf("artifact upload verification: hash %s: %w", path, err)
	}
	got := hex.EncodeToString(h.Sum(nil))
	if expectedSHA256 != "" && got != expectedSHA256 {
		return fmt.Errorf("artifact upload verification: sha256=%s want=%s", got, expectedSHA256)
	}
	return nil
}

func (v *FFprobeArtifactVerifier) VerifyArtifact(ctx context.Context, path string, expectedBytes int64) (string, error) {
	if v == nil {
		return "", fmt.Errorf("artifact verification: verifier is nil")
	}
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("artifact verification: path is empty")
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("artifact verification: stat %s: %w", path, err)
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 {
		return "", fmt.Errorf("artifact verification: %s is not a non-empty regular file", path)
	}
	if expectedBytes > 0 && info.Size() != expectedBytes {
		return "", fmt.Errorf("artifact verification: size=%d want=%d", info.Size(), expectedBytes)
	}

	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("artifact verification: open %s: %w", path, err)
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		_ = f.Close()
		return "", fmt.Errorf("artifact verification: hash %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("artifact verification: close %s: %w", path, err)
	}

	ffprobe := v.FFprobeBin
	if ffprobe == "" {
		ffprobe = "ffprobe"
	}
	cmd := exec.CommandContext(ctx, ffprobe,
		"-v", "error",
		"-show_entries", "format=duration",
		"-of", "default=noprint_wrappers=1:nokey=1",
		path,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("artifact verification: ffprobe %s: %w (output: %s)", path, err, strings.TrimSpace(string(output)))
	}
	duration, err := strconv.ParseFloat(strings.TrimSpace(string(output)), 64)
	if err != nil || duration <= 0 {
		return "", fmt.Errorf("artifact verification: ffprobe duration=%q", strings.TrimSpace(string(output)))
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Keep the hash implementation visible at this boundary: callers receive a
// canonical lowercase SHA-256 digest suitable for artifact metadata and
// deterministic assertions.
var _ = sha256.Size
