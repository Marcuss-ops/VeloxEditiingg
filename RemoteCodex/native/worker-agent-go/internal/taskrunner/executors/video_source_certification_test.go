package executors

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	osexec "os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"velox-shared/contract"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/runtimeassets"
	"velox-worker-agent/pkg/binaryresolver"
)

func TestVideoSourceCertificationReturnsEvidenceWithoutMediaOutput(t *testing.T) {
	if _, err := osexec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg is not installed")
	}
	if _, err := osexec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe is not installed")
	}
	profile := contract.CanonicalVideoProfileV1Default
	quality := contract.CanonicalPreparationQualityPolicyDefault
	revision, err := contract.VideoWindowPreparationRevision(profile, quality)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	sourcePath := filepath.Join(root, "complete-source.mp4")
	args := []string{"-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i",
		fmt.Sprintf("color=c=black:s=%dx%d:r=%d/%d:d=4", profile.Width, profile.Height, profile.FPSNum, profile.FPSDen),
		"-an", "-vf", contract.CanonicalVideoScaleFilter(profile)}
	args = append(args, contract.CanonicalVideoEncodeArgs(profile, quality)...)
	args = append(args, "-video_track_timescale", strconv.Itoa(profile.TimeBaseDen), "-y", sourcePath)
	if output, err := osexec.Command("ffmpeg", args...).CombinedOutput(); err != nil {
		t.Skipf("ffmpeg cannot create canonical fixture: %v: %s", err, output)
	}
	data, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	sha := hex.EncodeToString(digest[:])
	request := contract.VideoSourceCertificationV1{
		Version:         contract.VideoSourceCertificationVersion,
		Source:          contract.AssetRefV2{AssetID: "source-cert-fixture", SHA256: sha, SizeBytes: int64(len(data)), Kind: "video"},
		OutputProfileID: profile.ProfileID, OutputProfileRevision: revision,
	}
	workerExecutor := NewVideoSourceCertification(filepath.Join(root, "prepared-windows"))
	workerExecutor.(*videoSourceCertificationExecutor).inspector.sourceProbeFn = func(context.Context, string) (windowSourceProbe, error) {
		return windowSourceProbe{
			FrameCount: 96, ExactFrameCount: 96, CodecName: profile.Codec,
			Profile: "high", Level: canonicalLevelNumber(profile.CodecLevel),
			Width: profile.Width, Height: profile.Height, PixelFormat: profile.PixelFormat,
			FPSNum: profile.FPSNum, FPSDen: profile.FPSDen,
			AverageFPSNum: profile.FPSNum, AverageFPSDen: profile.FPSDen,
			TimeBaseNum: profile.TimeBaseNum, TimeBaseDen: profile.TimeBaseDen,
			StartTimeTicks: 0, StartTimeVerified: true, StartTimeSec: 0, ExtradataSHA: contract.CanonicalVideoExtradataSHA256,
			KeyframeTimes: []float64{0, 2},
		}, nil
	}
	spec := executor.TaskSpec{JobID: "job-source-cert", ExecutorID: VideoSourceCertificationID, Payload: map[string]interface{}{contract.VideoSourceCertificationPayloadKey: request}}
	ctx := runtimeassets.WithBindings(context.Background(), runtimeassets.Bindings{
		request.Source.AssetID: {AssetID: request.Source.AssetID, Path: sourcePath, SHA256: sha, Size: int64(len(data)), Verified: true},
	})
	result, err := workerExecutor.Execute(ctx, nil, spec)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Status != "succeeded" || len(result.Outputs) != 0 || len(result.Segments) != 1 || result.Segments[0].SourceType != VideoSourceCertificationID {
		t.Fatalf("source certification did not return an artifact-free result: %+v", result)
	}
	var certificate contract.VideoSourceCertificateV1
	if err := json.Unmarshal([]byte(result.Segments[0].MetadataJSON), &certificate); err != nil {
		t.Fatal(err)
	}
	if err := certificate.Validate(); err != nil || certificate.SHA256 != sha || certificate.SizeBytes != int64(len(data)) || certificate.Evidence.FrameCount != 96 || certificate.Chunks == nil || !certificate.SourceTimestampOriginVerified || certificate.SourceTimestampOriginTicks != 0 || certificate.AverageFPSNum != profile.FPSNum || certificate.AverageFPSDen != profile.FPSDen {
		t.Fatalf("invalid complete source certificate: %+v err=%v", certificate, err)
	}
	if certificate.Chunks.Validate(int64(len(data))) != nil {
		t.Fatalf("invalid block integrity proof: %+v", certificate.Chunks)
	}
	if workerExecutor.Descriptor().OutputTypes != nil {
		t.Fatalf("source certification declares media outputs: %#v", workerExecutor.Descriptor().OutputTypes)
	}
}

func TestNativeSourceProbeRequiresCompleteLibAVEvidence(t *testing.T) {
	if _, err := osexec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg is not installed")
	}
	binary, err := (binaryresolver.Resolver{
		Name: "velox_video_engine", EnvVar: "VELOX_VIDEO_ENGINE_CPP_BIN",
		AbsCandidates: []string{"/usr/local/bin/velox_video_engine"},
		RelOffsets:    []string{filepath.Join("../../../../video-engine-cpp/build", "velox_video_engine")},
	}).Resolve(0)
	if err != nil {
		t.Skipf("LibAV video engine is not available: %v", err)
	}
	help, err := osexec.Command(binary, "--help").CombinedOutput()
	if err != nil || !bytes.Contains(help, []byte("--probe-source")) {
		t.Skip("installed video engine does not include the LibAV source-certification command")
	}
	t.Setenv("VELOX_VIDEO_ENGINE_CPP_BIN", binary)
	path := filepath.Join(t.TempDir(), "source.mp4")
	command := osexec.Command("ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "color=c=black:s=64x64:r=5:d=1.2", "-an", "-c:v", "libx264", "-pix_fmt", "yuv420p", "-y", path)
	if output, err := command.CombinedOutput(); err != nil {
		t.Skipf("ffmpeg cannot create native probe fixture: %v: %s", err, output)
	}
	probe, err := runNativeSourceProbe(context.Background(), path)
	if err != nil {
		t.Fatalf("native source probe: %v", err)
	}
	if probe.ExactFrameCount != 6 || probe.FrameCount != 6 || probe.StartTimeSec != 0 ||
		probe.TimeBaseNum <= 0 || probe.TimeBaseDen <= 0 || !isLowerSHA256(probe.ExtradataSHA) || len(probe.KeyframeTimes) == 0 {
		t.Fatalf("incomplete native source evidence: %+v", probe)
	}
}
