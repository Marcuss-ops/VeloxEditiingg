package executors

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	osexec "os/exec"
	"path/filepath"
	"strings"
	"testing"

	"velox-shared/contract"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/runtimeassets"
	"velox-worker-agent/pkg/video/ffmpegrunner"
)

// TestVideoPrepareWindow_AnamorphicStockWindowMatchesCanonicalAVCC is the PR-2
// golden test: a real stock-style anamorphic source (PAL 64:45 SAR) goes
// through the exact worker projection and must certify against the canonical
// avcC. Before the setsar=1 normalization, libx264 copied the non-square SAR
// into the SPS VUI and the worker rejected its own valid fragment with
// WINDOW_EXTRADATA_IDENTITY_MISMATCH.
func TestVideoPrepareWindow_AnamorphicStockWindowMatchesCanonicalAVCC(t *testing.T) {
	if _, err := osexec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg is not installed")
	}
	if _, err := osexec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe is not installed")
	}
	root := t.TempDir()
	sourcePath := filepath.Join(root, "anamorphic-stock.mp4")
	sourceCmd := osexec.Command("ffmpeg",
		"-hide_banner", "-loglevel", "error", "-y",
		"-f", "lavfi", "-i", "testsrc2=size=720x576:rate=24:duration=3",
		"-vf", "setsar=64/45", "-an", "-c:v", "libx264", "-preset", "ultrafast",
		"-pix_fmt", "yuv420p", "-g", "24", sourcePath,
	)
	if output, err := sourceCmd.CombinedOutput(); err != nil {
		t.Skipf("local ffmpeg lacks the anamorphic fixture encoder: %v: %s", err, output)
	}
	source, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(source)
	profile := contract.CanonicalVideoProfileV1Default
	quality := contract.CanonicalPreparationQualityPolicyDefault
	revision, err := contract.VideoWindowPreparationRevision(profile, quality)
	if err != nil {
		t.Fatal(err)
	}
	request := contract.VideoPrepareWindowV1{
		Version:               contract.VideoPrepareWindowVersion,
		Source:                contract.AssetRefV2{AssetID: "anamorphic-stock", SHA256: hex.EncodeToString(sum[:]), SizeBytes: int64(len(source)), Kind: "video"},
		SourceStartFrame:      0,
		SourceFrameCount:      24,
		SourceFPSNum:          24,
		SourceFPSDen:          1,
		OutputFrameCount:      24,
		TimelineRevision:      1,
		TimelineSHA256:        strings.Repeat("e", 64),
		OutputProfileID:       profile.ProfileID,
		OutputProfileRevision: revision,
	}
	exec := &videoPrepareWindowExecutor{
		outputRoot: filepath.Join(root, "prepared"),
		runner:     ffmpegrunner.NewProcessRunner(),
		certifier:  ffprobeWindowOutputCertifier{},
		profile:    profile,
		quality:    quality,
	}
	spec := executor.TaskSpec{
		JobID:      "job-anamorphic-golden",
		ExecutorID: VideoPrepareWindowID,
		Payload:    map[string]interface{}{contract.VideoPrepareWindowPayloadKey: request},
	}
	ctx := runtimeassets.WithBindings(context.Background(), runtimeassets.Bindings{
		"anamorphic-stock": {
			AssetID: "anamorphic-stock", Path: sourcePath, SHA256: hex.EncodeToString(sum[:]),
			Size: int64(len(source)), Verified: true,
		},
	})
	result, err := exec.Execute(ctx, nil, spec)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Status != "succeeded" {
		t.Fatalf("anamorphic stock window did not certify: %+v", result)
	}
	var certificate windowPreparationCertificate
	if err := json.Unmarshal([]byte(result.Segments[0].MetadataJSON), &certificate); err != nil {
		t.Fatal(err)
	}
	if certificate.OutputEvidence.ExtradataSHA256 != contract.CanonicalVideoExtradataSHA256 {
		t.Fatalf("anamorphic window avcC = %s, want canonical %s",
			certificate.OutputEvidence.ExtradataSHA256, contract.CanonicalVideoExtradataSHA256)
	}
	if certificate.OutputEvidence.Width != profile.Width || certificate.OutputEvidence.Height != profile.Height {
		t.Fatalf("anamorphic window geometry = %dx%d, want %dx%d",
			certificate.OutputEvidence.Width, certificate.OutputEvidence.Height, profile.Width, profile.Height)
	}
}
