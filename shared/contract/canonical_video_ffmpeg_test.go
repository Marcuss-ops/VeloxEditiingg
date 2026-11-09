package contract

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCanonicalVideoEncoderContractPinsAVCC(t *testing.T) {
	profile := CanonicalVideoProfileV1Default
	quality := CanonicalPreparationQualityPolicyDefault
	joined := strings.Join(CanonicalVideoEncodeArgs(profile, quality), " ")
	for _, want := range []string{
		"-c:v libx264",
		"-preset veryfast",
		"-refs 1",
		"-colorspace bt709",
		"-color_primaries bt709",
		"-color_trc bt709",
		"-color_range tv",
		"-profile:v high",
		"-level:v 4.0",
		"-g 48",
		"-bf 0",
		"scenecut=0:open-gop=0:keyint=48:min-keyint=48",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("canonical encoder args %q do not pin %q", joined, want)
		}
	}

	frameWindow := strings.Join(CanonicalVideoWindowArgs(profile, quality, "in.mp4", "out.mp4", 0, 48, 24, 1, 48), " ")
	timestampWindow := strings.Join(CanonicalVideoWindowTimestampArgs(profile, quality, "in.mp4", "out.mp4", 0, 2_000_000, 48), " ")
	canonicalBlock := strings.Join(CanonicalVideoEncodeArgs(profile, quality), " ")
	for name, args := range map[string]string{"frame window": frameWindow, "timestamp window": timestampWindow} {
		if !strings.Contains(args, canonicalBlock) {
			t.Errorf("%s args do not contain the shared canonical encoder block: %q", name, args)
		}
	}
	if !strings.Contains(timestampWindow, "-t 2.041667") {
		t.Errorf("timestamp window decode duration must include one output-frame margin: %q", timestampWindow)
	}
	if !strings.Contains(timestampWindow, "-frames:v 48") {
		t.Errorf("timestamp window must retain the exact output-frame cap: %q", timestampWindow)
	}
}

func TestCanonicalVideoEncoderArgsDigestTracksContract(t *testing.T) {
	profile := CanonicalVideoProfileV1Default
	quality := CanonicalPreparationQualityPolicyDefault
	base, err := CanonicalVideoEncoderArgsDigest(profile, quality)
	if err != nil {
		t.Fatal(err)
	}
	again, err := CanonicalVideoEncoderArgsDigest(profile, quality)
	if err != nil || again != base {
		t.Fatalf("encoder args digest is not stable: %q != %q (err=%v)", base, again, err)
	}
	changedQuality := quality
	changedQuality.VideoBitrate = "1.5M"
	qualityDigest, err := CanonicalVideoEncoderArgsDigest(profile, changedQuality)
	if err != nil {
		t.Fatal(err)
	}
	if qualityDigest == base {
		t.Fatal("quality change did not change the encoder args digest")
	}
	changedProfile := profile
	changedProfile.BFrames++
	profileDigest, err := CanonicalVideoEncoderArgsDigest(changedProfile, quality)
	if err != nil {
		t.Fatal(err)
	}
	if profileDigest == base {
		t.Fatal("profile change did not change the encoder args digest")
	}
}

func TestCanonicalVideoAVCCMatchesBetweenMasterAndWorkerEncodes(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg is not installed")
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe is not installed")
	}
	root := t.TempDir()
	sourcePath := filepath.Join(root, "source.mp4")
	sourceArgs := []string{
		"-hide_banner", "-loglevel", "error", "-y",
		"-f", "lavfi", "-i", "testsrc2=size=1920x1080:rate=24:duration=2",
		"-an", "-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
		"-g", "48", sourcePath,
	}
	if output, err := exec.Command("ffmpeg", sourceArgs...).CombinedOutput(); err != nil {
		t.Skipf("local ffmpeg lacks the fixture encoder: %v: %s", err, output)
	}
	profile := CanonicalVideoProfileV1Default
	quality := CanonicalPreparationQualityPolicyDefault

	// Master-side canonical normalization: the exact video argument project
	// used by DataServer's trimmer.
	producerPath := filepath.Join(root, "producer.mp4")
	producerArgs := []string{
		"-hide_banner", "-loglevel", "error", "-y", "-i", sourcePath,
		"-map", "0:v:0", "-an", "-sn", "-dn", "-vf", CanonicalVideoScaleFilter(profile),
	}
	producerArgs = append(producerArgs, CanonicalVideoEncodeArgs(profile, quality)...)
	producerArgs = append(producerArgs, "-movflags", "+faststart", producerPath)
	if output, err := exec.Command("ffmpeg", producerArgs...).CombinedOutput(); err != nil {
		t.Fatalf("producer-style encode: %v: %s", err, output)
	}

	// Worker-side window encode: the same source window through the shared
	// canonical window projection.
	workerPath := filepath.Join(root, "worker.mp4")
	workerArgs := CanonicalVideoWindowArgs(profile, quality, sourcePath, workerPath, 0, 48, 24, 1, 48)
	if output, err := exec.Command("ffmpeg", workerArgs...).CombinedOutput(); err != nil {
		t.Fatalf("worker-style encode: %v: %s", err, output)
	}

	producerAVCC, err := probeAVCC(t, producerPath)
	if err != nil {
		t.Fatal(err)
	}
	workerAVCC, err := probeAVCC(t, workerPath)
	if err != nil {
		t.Fatal(err)
	}
	if producerAVCC != workerAVCC {
		t.Fatalf("Master and worker encodes disagree on avcC: producer=%s worker=%s", producerAVCC, workerAVCC)
	}
	if len(producerAVCC) != 64 {
		t.Fatalf("avcC hash is not a full SHA-256: %q", producerAVCC)
	}
}

func probeAVCC(t *testing.T, path string) (string, error) {
	t.Helper()
	args := []string{"-v", "error", "-select_streams", "v:0", "-show_data_hash", "sha256", "-show_entries", "stream=extradata_hash", "-of", "default=noprint_wrappers=1:nokey=1", path}
	data, err := exec.Command("ffprobe", args...).Output()
	if err != nil {
		return "", err
	}
	return strings.TrimPrefix(strings.TrimSpace(string(data)), "SHA256:"), nil
}
