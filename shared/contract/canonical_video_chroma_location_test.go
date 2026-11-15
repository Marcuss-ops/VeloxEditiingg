package contract

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestCanonicalVideoChromaLocationIsPinned pins the SPS VUI chroma location.
// libx264 inherits the frame chroma location from the source; without an
// explicit encoder pin a center-located plate produces a different avcC than
// the canonical reference even though every other stream fact agrees.
func TestCanonicalVideoChromaLocationIsPinned(t *testing.T) {
	joined := strings.Join(CanonicalVideoEncodeArgs(CanonicalVideoProfileV1Default, CanonicalPreparationQualityPolicyDefault), " ")
	if !strings.Contains(joined, "-chroma_sample_location unspecified") {
		t.Fatalf("canonical encoder args must pin the chroma sample location: %q", joined)
	}
	if CanonicalVideoExtradataSHA256 != "7201ce80a2b2b32a0c1a89501851099f267c1767d59ff799db744f72032aae5a" {
		t.Fatalf("canonical avcC identity changed unexpectedly: %s", CanonicalVideoExtradataSHA256)
	}
}

// TestCanonicalWindowArgsNormalizeCenterChromaSources proves the fix on real
// bytes: a center-located source goes through the exact worker window
// projection and must land on the canonical avcC. The control encode without
// the chroma pin must miss it, otherwise the fixture carries no center chroma
// and the assertion would be vacuous.
func TestCanonicalWindowArgsNormalizeCenterChromaSources(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg is not installed")
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe is not installed")
	}
	root := t.TempDir()
	sourcePath := filepath.Join(root, "center-chroma.mp4")
	sourceArgs := []string{
		"-hide_banner", "-loglevel", "error", "-y",
		"-f", "lavfi", "-i", "testsrc2=size=640x360:rate=24:duration=2",
		"-an", "-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
		"-chroma_sample_location", "center", sourcePath,
	}
	if output, err := exec.Command("ffmpeg", sourceArgs...).CombinedOutput(); err != nil {
		t.Skipf("local ffmpeg lacks the center-chroma fixture encoder: %v: %s", err, output)
	}
	chroma, err := probeChromaLocation(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if chroma != "center" {
		t.Skipf("local ffmpeg did not emit a center-located fixture (got %q)", chroma)
	}

	profile := CanonicalVideoProfileV1Default
	quality := CanonicalPreparationQualityPolicyDefault
	canonicalPath := filepath.Join(root, "canonical.mp4")
	args := CanonicalVideoWindowArgs(profile, quality, sourcePath, canonicalPath, 0, 24, 24, 1, 24)
	if output, err := exec.Command("ffmpeg", args...).CombinedOutput(); err != nil {
		t.Fatalf("center-chroma window encode: %v: %s", err, output)
	}
	got, err := probeAVCC(t, canonicalPath)
	if err != nil {
		t.Fatal(err)
	}
	if got != CanonicalVideoExtradataSHA256 {
		t.Fatalf("center-chroma window avcC = %s, want the canonical identity %s", got, CanonicalVideoExtradataSHA256)
	}

	legacyPath := filepath.Join(root, "legacy.mp4")
	legacyArgs := CanonicalVideoWindowArgs(profile, quality, sourcePath, legacyPath, 0, 24, 24, 1, 24)
	legacyArgs = withoutChromaSampleLocation(legacyArgs)
	if output, err := exec.Command("ffmpeg", legacyArgs...).CombinedOutput(); err != nil {
		t.Fatalf("control window encode: %v: %s", err, output)
	}
	legacy, err := probeAVCC(t, legacyPath)
	if err != nil {
		t.Fatal(err)
	}
	if legacy == CanonicalVideoExtradataSHA256 {
		t.Fatal("control encode without the chroma pin unexpectedly matched the canonical avcC")
	}
}

func probeChromaLocation(path string) (string, error) {
	args := []string{"-v", "error", "-select_streams", "v:0", "-show_entries", "stream=chroma_location", "-of", "default=noprint_wrappers=1:nokey=1", path}
	data, err := exec.Command("ffprobe", args...).Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

// withoutChromaSampleLocation removes the canonical pin from an argument list
// so a test can prove what happens without it.
func withoutChromaSampleLocation(args []string) []string {
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		if args[i] == "-chroma_sample_location" {
			i++
			continue
		}
		out = append(out, args[i])
	}
	return out
}
