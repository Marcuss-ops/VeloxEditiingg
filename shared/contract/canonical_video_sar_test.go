package contract

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCanonicalVideoGeometrySetsSquarePixelAspectRatio(t *testing.T) {
	profile := CanonicalVideoProfileV1Default
	filter := CanonicalVideoScaleGeometryFilter(profile)
	if !strings.HasSuffix(filter, "setsar=1") {
		t.Fatalf("canonical geometry filter must set square pixels, got %q", filter)
	}

	// SAR is a container/display geometry correction, not an encoder argument
	// or cache identity change. Keep the established bitstream identity intact.
	if CanonicalVideoExtradataSHA256 != "7201ce80a2b2b32a0c1a89501851099f267c1767d59ff799db744f72032aae5a" {
		t.Fatalf("canonical avcC identity changed unexpectedly: %s", CanonicalVideoExtradataSHA256)
	}
	revision, err := VideoWindowPreparationRevision(profile, CanonicalPreparationQualityPolicyDefault)
	if err != nil {
		t.Fatal(err)
	}
	const wantRevision = "video-window-preparer.v1:3eeb93e952e54a8e525603b20804dae86dca0977619af1a3c3f2b2ef1b44d672"
	if revision != wantRevision {
		t.Fatalf("SAR-only filter change must preserve prepared-window revision: got %q want %q", revision, wantRevision)
	}
	if !strings.Contains(strings.Join(CanonicalVideoEncodeArgs(profile, CanonicalPreparationQualityPolicyDefault), " "), "-c:v libx264") {
		t.Fatal("canonical encoder argument projection unexpectedly changed")
	}
}

// TestCanonicalVideoProjectionsForceSquarePixels pins the SAR normalization in
// every projection that can publish or prepare an assembly input. Without it a
// non-square source SAR survives scale/pad into the SPS VUI and produces an
// avcC that differs from the canonical identity while every nominal fact
// (codec, profile, level, geometry, fps) still agrees. That is a false
// WINDOW_EXTRADATA_IDENTITY_MISMATCH on a fragment the worker itself encoded.
func TestCanonicalVideoProjectionsForceSquarePixels(t *testing.T) {
	profile := CanonicalVideoProfileV1Default
	quality := CanonicalPreparationQualityPolicyDefault
	projections := map[string][]string{
		"scale filter":     {"-vf", CanonicalVideoScaleFilter(profile)},
		"normalize source": CanonicalVideoNormalizeSourceArgs(profile, quality, "in.mp4", "out.mp4"),
		"frame window":     CanonicalVideoWindowArgs(profile, quality, "in.mp4", "out.mp4", 0, 48, 24, 1, 48),
		"timestamp window": CanonicalVideoWindowTimestampArgs(profile, quality, "in.mp4", "out.mp4", 0, 2_000_000, 48),
	}
	for name, args := range projections {
		if joined := strings.Join(args, " "); !strings.Contains(joined, "setsar=1") {
			t.Errorf("%s does not force square pixels: %q", name, joined)
		}
	}
}

// TestCanonicalWindowArgsNormalizeAnamorphicSources proves the fix on real
// bytes: a PAL 64:45 source goes through the exact worker projection and must
// still land on the canonical avcC. The same source without the normalization
// emits a different SPS (measured 4ade6a…), which is what the worker rejected.
func TestCanonicalWindowArgsNormalizeAnamorphicSources(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg is not installed")
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe is not installed")
	}
	root := t.TempDir()
	sourcePath := filepath.Join(root, "anamorphic.mp4")
	sourceArgs := []string{
		"-hide_banner", "-loglevel", "error", "-y",
		"-f", "lavfi", "-i", "testsrc2=size=720x576:rate=24:duration=2",
		"-vf", "setsar=64/45",
		"-an", "-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
		sourcePath,
	}
	if output, err := exec.Command("ffmpeg", sourceArgs...).CombinedOutput(); err != nil {
		t.Skipf("local ffmpeg lacks the fixture encoder: %v: %s", err, output)
	}

	profile := CanonicalVideoProfileV1Default
	quality := CanonicalPreparationQualityPolicyDefault
	outputPath := filepath.Join(root, "window.mp4")
	args := CanonicalVideoWindowArgs(profile, quality, sourcePath, outputPath, 0, 24, 24, 1, 24)
	if output, err := exec.Command("ffmpeg", args...).CombinedOutput(); err != nil {
		t.Fatalf("anamorphic window encode: %v: %s", err, output)
	}

	got, err := probeAVCC(t, outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if got != CanonicalVideoExtradataSHA256 {
		t.Fatalf("anamorphic window avcC = %s, want the canonical square-pixel identity %s", got, CanonicalVideoExtradataSHA256)
	}
}
