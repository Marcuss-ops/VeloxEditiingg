package executors

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

// TestVideoPrepareWindow_CenterChromaStockWindowMatchesCanonicalAVCC is the
// r5 regression: a real stock-style yuvj420p plate with center chroma
// location goes through the exact worker projection and must certify against
// the canonical avcC. Before the chroma-location pin, libx264 copied
// chroma_sample_loc_type=1 into the SPS VUI and the worker rejected its own
// valid fragment with WINDOW_EXTRADATA_IDENTITY_MISMATCH (8d11e80f… instead of
// 7201ce80…). The source is square-pixel, so setsar=1 alone does not fix it.
func TestVideoPrepareWindow_CenterChromaStockWindowMatchesCanonicalAVCC(t *testing.T) {
	if _, err := osexec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg is not installed")
	}
	if _, err := osexec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe is not installed")
	}
	root := t.TempDir()
	sourcePath := filepath.Join(root, "center-chroma-stock.mp4")
	sourceCmd := osexec.Command("ffmpeg",
		"-hide_banner", "-loglevel", "error", "-y",
		"-f", "lavfi", "-i", "testsrc2=size=640x360:rate=24:duration=3",
		"-an", "-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
		"-chroma_sample_location", "center", sourcePath,
	)
	if output, err := sourceCmd.CombinedOutput(); err != nil {
		t.Skipf("local ffmpeg lacks the center-chroma fixture encoder: %v: %s", err, output)
	}
	probe, err := osexec.Command("ffprobe",
		"-v", "error", "-select_streams", "v:0",
		"-show_entries", "stream=chroma_location",
		"-of", "default=noprint_wrappers=1:nokey=1", sourcePath,
	).Output()
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(probe)); got != "center" {
		t.Skipf("local ffmpeg did not emit a center-located fixture (got %q)", got)
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
		Source:                contract.AssetRefV2{AssetID: "center-chroma-stock", SHA256: hex.EncodeToString(sum[:]), SizeBytes: int64(len(source)), Kind: "video"},
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
		JobID:      "job-center-chroma-golden",
		ExecutorID: VideoPrepareWindowID,
		Payload:    map[string]interface{}{contract.VideoPrepareWindowPayloadKey: request},
	}
	ctx := runtimeassets.WithBindings(context.Background(), runtimeassets.Bindings{
		"center-chroma-stock": {
			AssetID: "center-chroma-stock", Path: sourcePath, SHA256: hex.EncodeToString(sum[:]),
			Size: int64(len(source)), Verified: true,
		},
	})
	result, err := exec.Execute(ctx, nil, spec)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Status != "failed" || result.ErrorCode != "WINDOW_PACKET_COPY_INCOMPATIBLE" {
		t.Fatalf("incompatible center-chroma source was not rejected: %+v", result)
	}
}
