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
	frames int64
}

type fixedWindowExtradataReference string

func (r fixedWindowExtradataReference) ExpectedExtradataSHA256(context.Context, contract.CanonicalVideoProfileV1, contract.PreparationQualityPolicy) (string, error) {
	return string(r), nil
}

func (c fixedWindowCertifier) Certify(_ context.Context, _ string, profile contract.CanonicalVideoProfileV1, _ int64) (windowOutputEvidence, error) {
	return windowOutputEvidence{
		Codec: profile.Codec, Profile: strings.ToLower(profile.CodecProfile), Level: canonicalLevelNumber(profile.CodecLevel),
		Width: profile.Width, Height: profile.Height, PixelFormat: profile.PixelFormat,
		FPSNum: profile.FPSNum, FPSDen: profile.FPSDen, TimeBaseNum: profile.TimeBaseNum,
		TimeBaseDen: profile.TimeBaseDen, BFrames: profile.BFrames, FrameCount: c.frames,
		FirstFrameKeyframe: true, ExtradataSHA256: strings.Repeat("c", 64),
	}, nil
}

func TestVideoPrepareWindow_EncodesAndEmitsBoundCertification(t *testing.T) {
	profile := contract.CanonicalVideoProfileV1Default
	quality := contract.CanonicalPreparationQualityPolicyDefault
	revision, err := contract.VideoWindowPreparationRevision(profile, quality)
	if err != nil {
		t.Fatal(err)
	}
	sourcePath := filepath.Join(t.TempDir(), "source.mp4")
	sourceBytes := []byte("verified source")
	if err := os.WriteFile(sourcePath, sourceBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	sourceSHA := testSHA256(sourceBytes)
	request := contract.VideoPrepareWindowV1{
		Version:          contract.VideoPrepareWindowVersion,
		Source:           contract.AssetRefV2{AssetID: "source-asset", AssetKey: "source-cache-key", SHA256: sourceSHA, SizeBytes: int64(len(sourceBytes)), Kind: "video"},
		SourceStartFrame: 48, SourceFrameCount: 60, SourceFPSNum: 30, SourceFPSDen: 1,
		OutputFrameCount: 48, TimelineRevision: 3, TimelineSHA256: strings.Repeat("b", 64),
		TimelineStartFrame: 72, OutputProfileID: profile.ProfileID, OutputProfileRevision: revision,
	}
	runner := &recordingWindowRunner{}
	exec := &videoPrepareWindowExecutor{
		outputRoot: t.TempDir(), runner: runner, certifier: fixedWindowCertifier{frames: request.OutputFrameCount},
		extradataRef: fixedWindowExtradataReference(strings.Repeat("c", 64)),
		profile:      profile, quality: quality, encoderVer: "ffmpeg test runner", encoderSHA: strings.Repeat("e", 64),
	}
	spec := executor.TaskSpec{JobID: "job-window", ExecutorID: VideoPrepareWindowID,
		Payload: map[string]interface{}{contract.VideoPrepareWindowPayloadKey: request}}
	if err := exec.Validate(spec); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	ctx := runtimeassets.WithBindings(context.Background(), runtimeassets.Bindings{
		"source-asset": {AssetID: "source-asset", CacheKey: "source-cache-key", Path: sourcePath, SHA256: sourceSHA, Size: int64(len(sourceBytes)), Verified: true},
	})
	result, err := exec.Execute(ctx, nil, spec)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Status != "succeeded" || len(result.Outputs) != 1 || len(result.Segments) != 1 {
		t.Fatalf("unexpected result shape: %+v", result)
	}
	if result.Segments[0].FramesEncoded != request.OutputFrameCount {
		t.Fatalf("fresh encode frames_encoded = %d, want %d", result.Segments[0].FramesEncoded, request.OutputFrameCount)
	}
	if !strings.Contains(strings.Join(runner.args, " "), "between(n\\,48\\,107)") || !strings.Contains(strings.Join(runner.args, " "), "open-gop=0") {
		t.Fatalf("encoder did not receive exact frame interval and closed-GOP settings: %v", runner.args)
	}
	var certificate struct {
		SourceSHA256          string `json:"source_sha256"`
		SourceStartFrame      int64  `json:"source_start_frame"`
		SourceFrameCount      int64  `json:"source_frame_count"`
		TimelineRevision      int64  `json:"timeline_revision"`
		TimelineStartFrame    int64  `json:"timeline_start_frame"`
		OutputFrameCount      int64  `json:"output_frame_count"`
		OutputProfileRevision string `json:"output_profile_revision"`
		EncoderVersion        string `json:"encoder_version"`
		EncoderBuildSHA256    string `json:"encoder_build_sha256"`
		OutputSHA256          string `json:"output_sha256"`
		ClosedGOPAttested     bool   `json:"closed_gop_attested"`
	}
	if err := json.Unmarshal([]byte(result.Segments[0].MetadataJSON), &certificate); err != nil {
		t.Fatalf("decode certificate: %v", err)
	}
	if certificate.SourceSHA256 != sourceSHA || certificate.SourceStartFrame != 48 || certificate.SourceFrameCount != 60 ||
		certificate.TimelineRevision != 3 || certificate.TimelineStartFrame != 72 || certificate.OutputFrameCount != 48 ||
		certificate.OutputProfileRevision != revision || certificate.EncoderVersion != "ffmpeg test runner" ||
		certificate.EncoderBuildSHA256 != strings.Repeat("e", 64) || certificate.OutputSHA256 != result.Outputs[0].Hash || !certificate.ClosedGOPAttested {
		t.Fatalf("certificate is not bound to request/output: %+v", certificate)
	}

	// The same source window is reused across jobs. Only the timeline binding
	// changes; the immutable fragment and its evidence remain warm in cache.
	request.TimelineRevision = 4
	request.TimelineSHA256 = strings.Repeat("f", 64)
	request.TimelineStartFrame = 144
	spec.JobID = "different-job-same-window"
	spec.Payload[contract.VideoPrepareWindowPayloadKey] = request
	// No source bindings on the warm call: the fragment is already local, so
	// dispatch/execution must not require the source to be resolved again.
	warmResult, err := exec.Execute(context.Background(), nil, spec)
	if err != nil {
		t.Fatalf("warm Execute: %v", err)
	}
	if runner.calls != 1 {
		t.Fatalf("warm cache miss reran FFmpeg: calls=%d", runner.calls)
	}
	if warmResult.Segments[0].FramesEncoded != 0 {
		t.Fatalf("cache hit reported frames_encoded = %d, want 0", warmResult.Segments[0].FramesEncoded)
	}
	hit, err := exec.ProbeLocalPreparation(context.Background(), spec, nil)
	if err != nil || !hit {
		t.Fatalf("local preparation probe = hit:%v err:%v, want a warm hit", hit, err)
	}
	var warmCertificate map[string]interface{}
	if err := json.Unmarshal([]byte(warmResult.Segments[0].MetadataJSON), &warmCertificate); err != nil {
		t.Fatal(err)
	}
	if warmCertificate["cache_hit"] != true || warmCertificate["timeline_revision"] != float64(4) || warmCertificate["timeline_start_frame"] != float64(144) {
		t.Fatalf("warm certificate was not rebound to this timeline: %v", warmCertificate)
	}
}

func TestVideoPrepareWindow_RejectsStaleProfileRevision(t *testing.T) {
	profile := contract.CanonicalVideoProfileV1Default
	request := contract.VideoPrepareWindowV1{
		Version:          contract.VideoPrepareWindowVersion,
		Source:           contract.AssetRefV2{AssetID: "source", SHA256: strings.Repeat("a", 64), SizeBytes: 20, Kind: "video"},
		SourceStartFrame: 0, SourceFrameCount: 24, SourceFPSNum: 24, SourceFPSDen: 1,
		OutputFrameCount: 24, TimelineRevision: 1, TimelineSHA256: strings.Repeat("b", 64),
		OutputProfileID: profile.ProfileID, OutputProfileRevision: "old-encoder-revision",
	}
	exec := &videoPrepareWindowExecutor{runner: &recordingWindowRunner{}, certifier: fixedWindowCertifier{frames: 24}, profile: profile, quality: contract.CanonicalPreparationQualityPolicyDefault}
	spec := executor.TaskSpec{ExecutorID: VideoPrepareWindowID, Payload: map[string]interface{}{contract.VideoPrepareWindowPayloadKey: request}}
	if err := exec.Validate(spec); err == nil {
		t.Fatal("stale encoder/profile revision accepted")
	}
}

func TestVideoPrepareWindow_UsesAccurateTimestampSeek(t *testing.T) {
	profile := contract.CanonicalVideoProfileV1Default
	quality := contract.CanonicalPreparationQualityPolicyDefault
	revision, err := contract.VideoWindowPreparationRevision(profile, quality)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("timestamp source")
	path := filepath.Join(t.TempDir(), "source.mp4")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	sha := testSHA256(data)
	request := contract.VideoPrepareWindowV1{
		Version:       contract.VideoPrepareWindowVersion,
		Source:        contract.AssetRefV2{AssetID: "source", SHA256: sha, SizeBytes: int64(len(data)), Kind: "video"},
		SourceStartUS: 1_250_000, SourceDurationUS: 2_500_000, OutputFrameCount: 60,
		TimelineRevision: 1, TimelineSHA256: strings.Repeat("a", 64), TimelineStartFrame: 24,
		OutputProfileID: profile.ProfileID, OutputProfileRevision: revision,
	}
	runner := &recordingWindowRunner{}
	e := &videoPrepareWindowExecutor{outputRoot: t.TempDir(), runner: runner,
		certifier:    fixedWindowCertifier{frames: request.OutputFrameCount},
		extradataRef: fixedWindowExtradataReference(strings.Repeat("c", 64)),
		profile:      profile, quality: quality,
		encoderVer: "ffmpeg test runner", encoderSHA: strings.Repeat("e", 64)}
	spec := executor.TaskSpec{JobID: "job-time-window", ExecutorID: VideoPrepareWindowID,
		Payload: map[string]interface{}{contract.VideoPrepareWindowPayloadKey: request}}
	ctx := runtimeassets.WithBindings(context.Background(), runtimeassets.Bindings{
		"source": {AssetID: "source", CacheKey: "source", Path: path, SHA256: sha, Size: int64(len(data)), Verified: true},
	})
	result, err := e.Execute(ctx, nil, spec)
	if err != nil || result.Status != "succeeded" {
		t.Fatalf("timestamp-window execute: status=%s err=%v detail=%s", result.Status, err, result.ErrorDetail)
	}
	joined := strings.Join(runner.args, " ")
	if !strings.Contains(joined, "-ss 1.250000 -i") || !strings.Contains(joined, "-t 2.541667") || !strings.Contains(joined, "tpad=stop_mode=clone:stop=1") || !strings.Contains(joined, "-fps_mode cfr") {
		t.Fatalf("timestamp seek was not emitted as accurate normalized FFmpeg input: %v", runner.args)
	}
	var cert windowPreparationCertificate
	if err := json.Unmarshal([]byte(result.Segments[0].MetadataJSON), &cert); err != nil {
		t.Fatal(err)
	}
	if cert.SourceStartUS != request.SourceStartUS || cert.SourceDurationUS != request.SourceDurationUS {
		t.Fatalf("source interval missing from certification: %+v", cert)
	}
}

func TestVideoPrepareWindow_RealFFmpegOutputPassesCertification(t *testing.T) {
	if _, err := osexec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg is not installed")
	}
	if _, err := osexec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe is not installed")
	}
	root := t.TempDir()
	sourcePath := filepath.Join(root, "source.mp4")
	command := osexec.Command("ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "testsrc2=size=640x360:rate=30:duration=4", "-an", "-c:v", "libx264", "-pix_fmt", "yuv420p", "-g", "30", "-y", sourcePath)
	if output, err := command.CombinedOutput(); err != nil {
		t.Skipf("local ffmpeg lacks the fixture encoder: %v: %s", err, output)
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
		Version:          contract.VideoPrepareWindowVersion,
		Source:           contract.AssetRefV2{AssetID: "integration-source", SHA256: hex.EncodeToString(sum[:]), SizeBytes: int64(len(source)), Kind: "video"},
		SourceStartFrame: 30, SourceFrameCount: 60, SourceFPSNum: 30, SourceFPSDen: 1,
		OutputFrameCount: 48, TimelineRevision: 1, TimelineSHA256: strings.Repeat("d", 64),
		OutputProfileID: profile.ProfileID, OutputProfileRevision: revision,
	}
	exec := &videoPrepareWindowExecutor{
		outputRoot: filepath.Join(root, "prepared"), runner: ffmpegrunner.NewProcessRunner(),
		certifier: ffprobeWindowOutputCertifier{}, profile: profile, quality: quality,
	}
	spec := executor.TaskSpec{JobID: "job-ffmpeg-integration", ExecutorID: VideoPrepareWindowID,
		Payload: map[string]interface{}{contract.VideoPrepareWindowPayloadKey: request}}
	ctx := runtimeassets.WithBindings(context.Background(), runtimeassets.Bindings{
		"integration-source": {AssetID: "integration-source", Path: sourcePath, SHA256: hex.EncodeToString(sum[:]), Size: int64(len(source)), Verified: true},
	})
	result, err := exec.Execute(ctx, nil, spec)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Status != "succeeded" {
		t.Fatalf("real FFmpeg result failed: %+v", result)
	}
	if result.Outputs[0].SizeBytes <= 0 || result.Outputs[0].Hash == "" {
		t.Fatalf("output artifact is not content-addressed: %+v", result.Outputs[0])
	}
	var certificate map[string]interface{}
	if err := json.Unmarshal([]byte(result.Segments[0].MetadataJSON), &certificate); err != nil {
		t.Fatal(err)
	}
	if certificate["output_sha256"] != result.Outputs[0].Hash || certificate["closed_gop_attested"] != true {
		t.Fatalf("real output certification is missing bindings: %v", certificate)
	}
	request.SourceStartFrame, request.SourceFrameCount = 0, 0
	request.SourceFPSNum, request.SourceFPSDen = 0, 0
	request.SourceStartUS, request.SourceDurationUS = 1_250_000, 2_500_000
	request.OutputFrameCount = 60
	request.TimelineStartFrame = 96
	spec.Payload[contract.VideoPrepareWindowPayloadKey] = request
	timeResult, err := exec.Execute(ctx, nil, spec)
	if err != nil || timeResult.Status != "succeeded" {
		t.Fatalf("real FFmpeg timestamp window failed: result=%+v err=%v", timeResult, err)
	}
	var timeCertificate windowPreparationCertificate
	if err := json.Unmarshal([]byte(timeResult.Segments[0].MetadataJSON), &timeCertificate); err != nil {
		t.Fatal(err)
	}
	if timeCertificate.SourceStartUS != 1_250_000 || timeCertificate.SourceDurationUS != 2_500_000 || timeCertificate.OutputEvidence.FrameCount != 60 {
		t.Fatalf("timestamp window output was not precisely certified: %+v", timeCertificate)
	}
	shortSource := filepath.Join(root, "short-source.mp4")
	shortSourceCmd := osexec.Command("ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "testsrc2=size=640x360:rate=24", "-frames:v", "119", "-an", "-c:v", "libx264", "-pix_fmt", "yuv420p", "-g", "48", "-y", shortSource)
	if output, err := shortSourceCmd.CombinedOutput(); err != nil {
		t.Skipf("local ffmpeg lacks the 119-frame fixture encoder: %v: %s", err, output)
	}
	shortBytes, err := os.ReadFile(shortSource)
	if err != nil {
		t.Fatal(err)
	}
	shortSum := sha256.Sum256(shortBytes)
	request.Source = contract.AssetRefV2{AssetID: "short-timestamp-source", SHA256: hex.EncodeToString(shortSum[:]), SizeBytes: int64(len(shortBytes)), Kind: "video"}
	request.SourceStartFrame, request.SourceFrameCount = 0, 0
	request.SourceFPSNum, request.SourceFPSDen = 0, 0
	request.SourceStartUS, request.SourceDurationUS = 0, 5_000_000
	request.OutputFrameCount = 120
	request.TimelineStartFrame = 144
	spec.Payload[contract.VideoPrepareWindowPayloadKey] = request
	shortCtx := runtimeassets.WithBindings(context.Background(), runtimeassets.Bindings{
		"short-timestamp-source": {AssetID: "short-timestamp-source", Path: shortSource, SHA256: hex.EncodeToString(shortSum[:]), Size: int64(len(shortBytes)), Verified: true},
	})
	shortResult, err := exec.Execute(shortCtx, nil, spec)
	if err != nil || shortResult.Status != "succeeded" {
		t.Fatalf("119-frame source did not produce a certified 120-frame window: result=%+v err=%v", shortResult, err)
	}
	var shortCertificate windowPreparationCertificate
	if err := json.Unmarshal([]byte(shortResult.Segments[0].MetadataJSON), &shortCertificate); err != nil {
		t.Fatal(err)
	}
	if shortCertificate.OutputEvidence.FrameCount != 120 {
		t.Fatalf("short timestamp window frame count = %d, want 120", shortCertificate.OutputEvidence.FrameCount)
	}
}

func TestVideoPrepareWindow_RejectsNonCanonicalExtradata(t *testing.T) {
	profile := contract.CanonicalVideoProfileV1Default
	quality := contract.CanonicalPreparationQualityPolicyDefault
	revision, err := contract.VideoWindowPreparationRevision(profile, quality)
	if err != nil {
		t.Fatal(err)
	}
	sourcePath := filepath.Join(t.TempDir(), "source.mp4")
	sourceBytes := []byte("verified source")
	if err := os.WriteFile(sourcePath, sourceBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	sourceSHA := testSHA256(sourceBytes)
	request := contract.VideoPrepareWindowV1{
		Version:          contract.VideoPrepareWindowVersion,
		Source:           contract.AssetRefV2{AssetID: "source-asset", SHA256: sourceSHA, SizeBytes: int64(len(sourceBytes)), Kind: "video"},
		SourceStartFrame: 0, SourceFrameCount: 48, SourceFPSNum: 24, SourceFPSDen: 1,
		OutputFrameCount: 48, TimelineRevision: 1, TimelineSHA256: strings.Repeat("b", 64),
		OutputProfileID: profile.ProfileID, OutputProfileRevision: revision,
	}
	exec := &videoPrepareWindowExecutor{
		outputRoot: t.TempDir(), runner: &recordingWindowRunner{}, certifier: fixedWindowCertifier{frames: 48},
		extradataRef: fixedWindowExtradataReference(strings.Repeat("d", 64)), profile: profile, quality: quality,
		encoderVer: "ffmpeg test runner", encoderSHA: strings.Repeat("e", 64),
	}
	spec := executor.TaskSpec{JobID: "job-extradata-mismatch", ExecutorID: VideoPrepareWindowID,
		Payload: map[string]interface{}{contract.VideoPrepareWindowPayloadKey: request}}
	ctx := runtimeassets.WithBindings(context.Background(), runtimeassets.Bindings{
		"source-asset": {AssetID: "source-asset", Path: sourcePath, SHA256: sourceSHA, Size: int64(len(sourceBytes)), Verified: true},
	})
	result, err := exec.Execute(ctx, nil, spec)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Status != "failed" || result.ErrorCode != "WINDOW_EXTRADATA_IDENTITY_MISMATCH" {
		t.Fatalf("non-canonical avcC was accepted: %+v", result)
	}
}

func testSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
