package executors

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	osexec "os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"velox-shared/contract"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/runtimeassets"
	"velox-worker-agent/pkg/video/ffmpegrunner"
)

type recordingWindowRunner struct {
	args  []string
	calls int
}

func (r *recordingWindowRunner) Run(_ context.Context, request ffmpegrunner.FFmpegRequest) (ffmpegrunner.FFmpegResult, error) {
	r.calls++
	r.args = append([]string(nil), request.Args...)
	if len(request.Args) == 0 {
		return ffmpegrunner.FFmpegResult{}, os.ErrInvalid
	}
	if err := os.WriteFile(request.Args[len(request.Args)-1], []byte("prepared-window"), 0o600); err != nil {
		return ffmpegrunner.FFmpegResult{}, err
	}
	return ffmpegrunner.FFmpegResult{ProcessWallMS: 12, ProcessSpawnMS: 2}, nil
}

type fixedWindowCertifier struct {
	frames    int64
	extradata string
}

type fixedWindowExtradataReference string

func (r fixedWindowExtradataReference) ExpectedExtradataSHA256(context.Context, contract.CanonicalVideoProfileV1, contract.PreparationQualityPolicy) (string, error) {
	return string(r), nil
}

func (c fixedWindowCertifier) Certify(_ context.Context, _ string, profile contract.CanonicalVideoProfileV1, _ int64) (windowOutputEvidence, error) {
	extradata := c.extradata
	if extradata == "" {
		extradata = contract.CanonicalVideoExtradataSHA256
	}
	return windowOutputEvidence{
		Codec: profile.Codec, Profile: strings.ToLower(profile.CodecProfile), Level: canonicalLevelNumber(profile.CodecLevel),
		Width: profile.Width, Height: profile.Height, PixelFormat: profile.PixelFormat,
		FPSNum: profile.FPSNum, FPSDen: profile.FPSDen, TimeBaseNum: profile.TimeBaseNum,
		TimeBaseDen: profile.TimeBaseDen, BFrames: profile.BFrames, FrameCount: c.frames,
		FirstFrameKeyframe: true, ExtradataSHA256: extradata,
	}, nil
}

func TestVideoPrepareWindow_AcceptsPersistedV1AndRejectsInvalidRevision(t *testing.T) {
	profile := contract.CanonicalVideoProfileV1Default
	request := contract.VideoPrepareWindowV1{
		Version:          contract.VideoPrepareWindowVersion,
		Source:           contract.AssetRefV2{AssetID: "source", SHA256: strings.Repeat("a", 64), SizeBytes: 20, Kind: "video"},
		SourceStartFrame: 0, SourceFrameCount: 24, SourceFPSNum: 24, SourceFPSDen: 1,
		OutputFrameCount: 24, TimelineRevision: 1, TimelineSHA256: strings.Repeat("b", 64),
		OutputProfileID: profile.ProfileID, OutputProfileRevision: "video-window-preparer.v1:" + strings.Repeat("a", 64),
	}
	exec := &videoPrepareWindowExecutor{runner: &recordingWindowRunner{}, certifier: fixedWindowCertifier{frames: 24}, profile: profile, quality: contract.CanonicalPreparationQualityPolicyDefault}
	spec := executor.TaskSpec{ExecutorID: VideoPrepareWindowID, Payload: map[string]interface{}{contract.VideoPrepareWindowPayloadKey: request}}
	if err := exec.Validate(spec); err != nil {
		t.Fatalf("persisted v1 task was rejected during the revision migration: %v", err)
	}
	request.OutputProfileRevision = "old-encoder-revision"
	spec.Payload[contract.VideoPrepareWindowPayloadKey] = request
	if err := exec.Validate(spec); err == nil {
		t.Fatal("malformed legacy revision accepted")
	}
}

func TestProbeLocalPreparationCacheHitDoesNotRequireFFmpeg(t *testing.T) {
	profile := contract.CanonicalVideoProfileV1Default
	quality := contract.CanonicalPreparationQualityPolicyDefault
	revision, err := contract.VideoWindowPreparationRevision(profile, quality)
	if err != nil {
		t.Fatal(err)
	}
	request := contract.VideoPrepareWindowV1{
		Version:          contract.VideoPrepareWindowVersion,
		Source:           contract.AssetRefV2{AssetID: "source", SHA256: strings.Repeat("a", 64), SizeBytes: 123, Kind: "video"},
		SourceStartFrame: 0, SourceFrameCount: 24, SourceFPSNum: profile.FPSNum, SourceFPSDen: profile.FPSDen,
		OutputFrameCount: 24, TimelineRevision: 1, TimelineSHA256: strings.Repeat("b", 64),
		OutputProfileID: profile.ProfileID, OutputProfileRevision: revision,
	}
	identity, err := contract.VideoPrepareWindowCacheIdentity(request)
	if err != nil {
		t.Fatal(err)
	}
	e := &videoPrepareWindowExecutor{
		outputRoot: t.TempDir(), runner: &recordingWindowRunner{}, certifier: fixedWindowCertifier{frames: request.OutputFrameCount},
		profile: profile, quality: quality,
	}
	outputPath := e.preparedWindowPath(identity, request, nil)
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(outputPath, []byte("immutable prepared bytes"), 0o440); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	attestation := windowArtifactAttestation{
		Version: contract.VideoPrepareWindowVersion, SourceSHA256: request.Source.SHA256,
		SourceStartFrame: request.SourceStartFrame, SourceFrameCount: request.SourceFrameCount,
		SourceFPSNum: request.SourceFPSNum, SourceFPSDen: request.SourceFPSDen,
		OutputFrameCount: request.OutputFrameCount, OutputProfileID: request.OutputProfileID,
		OutputProfileRevision: request.OutputProfileRevision, EncoderVersion: "original ffmpeg build",
		EncoderBuildSHA256: strings.Repeat("c", 64), OutputSHA256: strings.Repeat("d", 64),
		OutputSizeBytes: info.Size(), OutputModTimeUnixNano: info.ModTime().UnixNano(),
		OutputEvidence: windowOutputEvidence{
			Codec: profile.Codec, Profile: strings.ToLower(profile.CodecProfile), Level: canonicalLevelNumber(profile.CodecLevel),
			Width: profile.Width, Height: profile.Height, PixelFormat: profile.PixelFormat,
			FPSNum: profile.FPSNum, FPSDen: profile.FPSDen, TimeBaseNum: profile.TimeBaseNum,
			TimeBaseDen: profile.TimeBaseDen, BFrames: profile.BFrames, FrameCount: request.OutputFrameCount,
			FirstFrameKeyframe: true, ExtradataSHA256: contract.CanonicalVideoExtradataSHA256,
		},
		ClosedGOPAttested: true, PacketCopyUsed: true,
	}
	data, err := json.Marshal(attestation)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(outputPath+".cert.json", data, 0o440); err != nil {
		t.Fatal(err)
	}
	spec := executor.TaskSpec{ExecutorID: VideoPrepareWindowID, Payload: map[string]interface{}{contract.VideoPrepareWindowPayloadKey: request}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	hit, err := e.ProbeLocalPreparation(ctx, spec, nil)
	if err != nil {
		t.Fatalf("cache probe unexpectedly required an executable FFmpeg: %v", err)
	}
	if !hit {
		t.Fatal("valid immutable packet-copy attestation was not reused")
	}
}

func TestTimestampMatchesFramesUsesRationalFrameRateAndMicrosecondQuantization(t *testing.T) {
	tests := []struct {
		name   string
		timeUS int64
		frames int64
		fpsNum int
		fpsDen int
		want   bool
	}{
		{name: "integer fps exact", timeUS: 3_125_000, frames: 75, fpsNum: 24, fpsDen: 1, want: true},
		{name: "integer fps truncated ten frames", timeUS: 416_666, frames: 10, fpsNum: 24, fpsDen: 1, want: true},
		{name: "integer fps rounded ten frames", timeUS: 416_667, frames: 10, fpsNum: 24, fpsDen: 1, want: true},
		{name: "integer fps more than one microsecond off", timeUS: 416_665, frames: 10, fpsNum: 24, fpsDen: 1, want: false},
		{name: "fractional fps rounded one frame", timeUS: 41_708, frames: 1, fpsNum: 24_000, fpsDen: 1_001, want: true},
		{name: "fractional fps rounded ten frames", timeUS: 417_083, frames: 10, fpsNum: 24_000, fpsDen: 1_001, want: true},
		{name: "wrong frame count", timeUS: 417_083, frames: 9, fpsNum: 24_000, fpsDen: 1_001, want: false},
		{name: "off frame boundary", timeUS: 40_000, frames: 1, fpsNum: 24_000, fpsDen: 1_001, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := timestampMatchesFrames(tt.timeUS, tt.frames, tt.fpsNum, tt.fpsDen); got != tt.want {
				t.Fatalf("timestampMatchesFrames(%d, %d, %d/%d) = %v, want %v", tt.timeUS, tt.frames, tt.fpsNum, tt.fpsDen, got, tt.want)
			}
		})
	}
}

func TestVideoPrepareWindow_ProducesCertifiedPacketCopyForCanonicalKeyframeWindow(t *testing.T) {
	if _, err := osexec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg is not installed")
	}
	if _, err := osexec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe is not installed")
	}
	root := t.TempDir()
	profile := contract.CanonicalVideoProfileV1Default
	quality := contract.CanonicalPreparationQualityPolicyDefault
	revision, err := contract.VideoWindowPreparationRevision(profile, quality)
	if err != nil {
		t.Fatal(err)
	}
	sourcePath := filepath.Join(root, "source.mp4")
	args := []string{"-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i",
		fmt.Sprintf("color=c=black:s=%dx%d:r=%d/%d:d=4", profile.Width, profile.Height, profile.FPSNum, profile.FPSDen),
		"-an", "-vf", contract.CanonicalVideoScaleFilter(profile)}
	args = append(args, contract.CanonicalVideoEncodeArgs(profile, quality)...)
	args = append(args, "-video_track_timescale", strconv.Itoa(profile.TimeBaseDen), "-y", sourcePath)
	if output, err := osexec.Command("ffmpeg", args...).CombinedOutput(); err != nil {
		t.Skipf("ffmpeg cannot create canonical fixture: %v: %s", err, output)
	}
	source, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(source)
	request := contract.VideoPrepareWindowV1{
		Version:          contract.VideoPrepareWindowVersion,
		Source:           contract.AssetRefV2{AssetID: "copy-source", SHA256: hex.EncodeToString(sum[:]), SizeBytes: int64(len(source)), Kind: "video"},
		SourceStartFrame: 48, SourceFrameCount: 24, SourceFPSNum: profile.FPSNum, SourceFPSDen: profile.FPSDen,
		OutputFrameCount: 24, TimelineRevision: 1, TimelineSHA256: strings.Repeat("e", 64),
		OutputProfileID: profile.ProfileID, OutputProfileRevision: revision,
	}
	prepare := &videoPrepareWindowExecutor{
		outputRoot: filepath.Join(root, "prepared"), runner: ffmpegrunner.NewProcessRunner(),
		certifier: ffprobeWindowOutputCertifier{}, profile: profile, quality: quality,
	}
	spec := executor.TaskSpec{JobID: "job-certified-copy", ExecutorID: VideoPrepareWindowID,
		Payload: map[string]interface{}{contract.VideoPrepareWindowPayloadKey: request}}
	ctx := runtimeassets.WithBindings(context.Background(), runtimeassets.Bindings{
		"copy-source": {AssetID: "copy-source", Path: sourcePath, SHA256: hex.EncodeToString(sum[:]), Size: int64(len(source)), Verified: true},
	})
	result, err := prepare.Execute(ctx, nil, spec)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Status != "succeeded" || len(result.Outputs) != 1 || len(result.Segments) != 1 {
		t.Fatalf("canonical source window was not packet-copied successfully: %+v", result)
	}
	var certificate contract.VideoPrepareWindowCertificateV1
	if err := json.Unmarshal([]byte(result.Segments[0].MetadataJSON), &certificate); err != nil {
		t.Fatalf("decode output certificate: %v", err)
	}
	if !certificate.PacketCopyUsed || certificate.OutputEvidence.FrameCount != request.OutputFrameCount ||
		certificate.OutputEvidence.ExtradataSHA256 != contract.CanonicalVideoExtradataSHA256 {
		t.Fatalf("output lacks canonical packet-copy evidence: %+v", certificate)
	}
}

func TestDiagnoseSourceWindowCopyReportsIndependentFallbackCauses(t *testing.T) {
	profile := contract.CanonicalVideoProfileV1Default
	quality := contract.CanonicalPreparationQualityPolicyDefault
	request := contract.VideoPrepareWindowV1{
		SourceStartFrame: 24, SourceFrameCount: 24, SourceFPSNum: profile.FPSNum,
		SourceFPSDen: profile.FPSDen, OutputFrameCount: 24,
	}
	source := windowSourceProbe{
		CodecName: profile.Codec, Profile: strings.ToLower(profile.CodecProfile),
		Level: canonicalLevelNumber(profile.CodecLevel), Width: profile.Width, Height: profile.Height,
		PixelFormat: profile.PixelFormat, FPSNum: profile.FPSNum, FPSDen: profile.FPSDen,
		TimeBaseNum: profile.TimeBaseNum, TimeBaseDen: profile.TimeBaseDen,
		BitRateBPS: 10_000_000, ExtradataSHA: strings.Repeat("a", 64), KeyframeTimes: []float64{},
	}
	got := diagnoseSourceWindowCopy(source, request, profile, quality, strings.Repeat("b", 64))
	if got.Eligible {
		t.Fatal("incompatible source was eligible for packet copy")
	}
	want := map[string]bool{"extradata_mismatch": false, "window_start_not_keyframe": false}
	for _, reason := range got.Reasons {
		if _, ok := want[reason]; ok {
			want[reason] = true
		}
	}
	for reason, found := range want {
		if !found {
			t.Errorf("missing fallback reason %q in %v", reason, got.Reasons)
		}
	}
}

func TestDiagnoseSourceWindowCopyIgnoresAverageBitrateForCanonicalPacketCopy(t *testing.T) {
	profile := contract.CanonicalVideoProfileV1Default
	quality := contract.CanonicalPreparationQualityPolicyDefault
	request := contract.VideoPrepareWindowV1{
		SourceStartFrame: 24, SourceFrameCount: 24, SourceFPSNum: profile.FPSNum,
		SourceFPSDen: profile.FPSDen, OutputFrameCount: 24,
	}
	const extradata = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	source := windowSourceProbe{
		CodecName: profile.Codec, Profile: strings.ToLower(profile.CodecProfile),
		Level: canonicalLevelNumber(profile.CodecLevel), Width: profile.Width, Height: profile.Height,
		PixelFormat: profile.PixelFormat, FPSNum: profile.FPSNum, FPSDen: profile.FPSDen,
		TimeBaseNum: profile.TimeBaseNum, TimeBaseDen: profile.TimeBaseDen,
		BFrames: 0, BitRateBPS: 10_000_000, ExtradataSHA: extradata, KeyframeTimes: []float64{1},
	}
	got := diagnoseSourceWindowCopy(source, request, profile, quality, extradata)
	if !got.Eligible {
		t.Fatalf("canonical packet-copy source rejected for its average bitrate: %v", got.Reasons)
	}
}

func TestDiagnoseSourceWindowCopyUsesNonZeroPresentationOrigin(t *testing.T) {
	profile := contract.CanonicalVideoProfileV1Default
	quality := contract.CanonicalPreparationQualityPolicyDefault
	const extradata = "7201ce80a2b2b32a0c1a89501851099f267c1767d59ff799db744f72032aae5a"
	request := contract.VideoPrepareWindowV1{SourceStartUS: 0, SourceDurationUS: 4_958_333, OutputFrameCount: 119}
	source := windowSourceProbe{
		CodecName: profile.Codec, Profile: strings.ToLower(profile.CodecProfile),
		Level: canonicalLevelNumber(profile.CodecLevel), Width: profile.Width, Height: profile.Height,
		PixelFormat: profile.PixelFormat, FPSNum: profile.FPSNum, FPSDen: profile.FPSDen,
		TimeBaseNum: profile.TimeBaseNum, TimeBaseDen: profile.TimeBaseDen,
		BFrames: 0, StartTimeSec: 0.021, ExtradataSHA: extradata,
		KeyframeTimes: []float64{0.021, 2.021, 4.021},
	}
	got := diagnoseSourceWindowCopy(source, request, profile, quality, extradata)
	if !got.Eligible {
		t.Fatalf("canonical window with a non-zero PTS origin was rejected: %v", got.Reasons)
	}

	args := canonicalWindowStreamCopyArgs(profile, "source.mp4", "window.mp4", request, source.StartTimeSec)
	for i := 0; i+1 < len(args); i++ {
		if args[i] == "-ss" {
			if args[i+1] != "0.021000" {
				t.Fatalf("seek = %q, want source presentation origin 0.021000s", args[i+1])
			}
			return
		}
	}
	t.Fatal("packet-copy arguments are missing -ss")
}

func testSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
