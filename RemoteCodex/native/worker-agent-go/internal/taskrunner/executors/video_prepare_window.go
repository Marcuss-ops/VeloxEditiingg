package executors

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"velox-shared/contract"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/runtimeassets"
	"velox-worker-agent/pkg/storage"
	"velox-worker-agent/pkg/video/ffmpegrunner"
)

const (
	VideoPrepareWindowID      = "video.prepare.window.v1"
	VideoPrepareWindowVersion = 1
)

type windowFFmpegRunner interface {
	Run(context.Context, ffmpegrunner.FFmpegRequest) (ffmpegrunner.FFmpegResult, error)
}

type windowOutputEvidence = contract.VideoWindowOutputEvidenceV1

type windowArtifactAttestation struct {
	Version               int                  `json:"version"`
	SourceSHA256          string               `json:"source_sha256"`
	SourceStartFrame      int64                `json:"source_start_frame"`
	SourceFrameCount      int64                `json:"source_frame_count"`
	SourceFPSNum          int                  `json:"source_fps_num"`
	SourceFPSDen          int                  `json:"source_fps_den"`
	SourceStartUS         int64                `json:"source_start_us,omitempty"`
	SourceDurationUS      int64                `json:"source_duration_us,omitempty"`
	OutputFrameCount      int64                `json:"output_frame_count"`
	OutputProfileID       string               `json:"output_profile_id"`
	OutputProfileRevision string               `json:"output_profile_revision"`
	EncoderVersion        string               `json:"encoder_version"`
	EncoderBuildSHA256    string               `json:"encoder_build_sha256"`
	OutputSHA256          string               `json:"output_sha256"`
	OutputSizeBytes       int64                `json:"output_size_bytes"`
	OutputModTimeUnixNano int64                `json:"output_mtime_unix_nano"`
	OutputEvidence        windowOutputEvidence `json:"output_evidence"`
	ClosedGOPAttested     bool                 `json:"closed_gop_attested"`
}

type windowPreparationCertificate = contract.VideoPrepareWindowCertificateV1

type windowOutputCertifier interface {
	Certify(context.Context, string, contract.CanonicalVideoProfileV1, int64) (windowOutputEvidence, error)
}

type ffprobeWindowOutputCertifier struct{}

type windowFFprobeDocument struct {
	Streams []struct {
		CodecName     string `json:"codec_name"`
		Profile       string `json:"profile"`
		Level         int    `json:"level"`
		Width         int    `json:"width"`
		Height        int    `json:"height"`
		PixelFormat   string `json:"pix_fmt"`
		FrameRate     string `json:"r_frame_rate"`
		TimeBase      string `json:"time_base"`
		BFrames       int    `json:"has_b_frames"`
		FrameCount    string `json:"nb_read_frames"`
		ExtradataHash string `json:"extradata_hash"`
	} `json:"streams"`
}

func (ffprobeWindowOutputCertifier) Certify(ctx context.Context, path string, profile contract.CanonicalVideoProfileV1, wantFrames int64) (windowOutputEvidence, error) {
	args := []string{"-v", "error", "-count_frames", "-show_data_hash", "sha256", "-show_entries", "stream=codec_type,codec_name,profile,level,width,height,pix_fmt,r_frame_rate,time_base,has_b_frames,nb_read_frames,extradata_hash", "-of", "json", path}
	data, err := exec.CommandContext(ctx, "ffprobe", args...).Output()
	if err != nil {
		return windowOutputEvidence{}, fmt.Errorf("ffprobe prepared window: %w", err)
	}
	var doc windowFFprobeDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		return windowOutputEvidence{}, fmt.Errorf("decode prepared window probe: %w", err)
	}
	var video *struct {
		CodecName     string `json:"codec_name"`
		Profile       string `json:"profile"`
		Level         int    `json:"level"`
		Width         int    `json:"width"`
		Height        int    `json:"height"`
		PixelFormat   string `json:"pix_fmt"`
		FrameRate     string `json:"r_frame_rate"`
		TimeBase      string `json:"time_base"`
		BFrames       int    `json:"has_b_frames"`
		FrameCount    string `json:"nb_read_frames"`
		ExtradataHash string `json:"extradata_hash"`
	}
	if len(doc.Streams) != 1 {
		return windowOutputEvidence{}, fmt.Errorf("prepared fragment has %d streams, expected one video stream", len(doc.Streams))
	}
	video = &doc.Streams[0]
	fpsNum, fpsDen := parseWindowRatio(video.FrameRate)
	tbNum, tbDen := parseWindowRatio(video.TimeBase)
	frameCount, parseErr := strconv.ParseInt(video.FrameCount, 10, 64)
	if parseErr != nil {
		return windowOutputEvidence{}, fmt.Errorf("prepared fragment frame count is unavailable: %w", parseErr)
	}
	keyframeArgs := []string{"-v", "error", "-select_streams", "v:0", "-read_intervals", "%+#1", "-show_entries", "frame=key_frame", "-of", "csv=p=0", path}
	keyframeData, err := exec.CommandContext(ctx, "ffprobe", keyframeArgs...).Output()
	if err != nil {
		return windowOutputEvidence{}, fmt.Errorf("ffprobe prepared window first frame: %w", err)
	}
	firstFrameField := strings.TrimSpace(strings.SplitN(string(keyframeData), ",", 2)[0])
	firstFrameKeyframe := firstFrameField == "1"
	evidence := windowOutputEvidence{
		Codec: video.CodecName, Profile: strings.ToLower(video.Profile), Level: video.Level,
		Width: video.Width, Height: video.Height, PixelFormat: video.PixelFormat,
		FPSNum: fpsNum, FPSDen: fpsDen, TimeBaseNum: tbNum, TimeBaseDen: tbDen,
		BFrames: video.BFrames, FrameCount: frameCount,
		FirstFrameKeyframe: firstFrameKeyframe,
		ExtradataSHA256:    strings.TrimPrefix(strings.TrimSpace(video.ExtradataHash), "SHA256:"),
	}
	if evidence.Codec != profile.Codec || evidence.Profile != strings.ToLower(profile.CodecProfile) || evidence.Level != canonicalLevelNumber(profile.CodecLevel) ||
		evidence.Width != profile.Width || evidence.Height != profile.Height || evidence.PixelFormat != profile.PixelFormat ||
		evidence.FPSNum != profile.FPSNum || evidence.FPSDen != profile.FPSDen || evidence.TimeBaseNum != profile.TimeBaseNum || evidence.TimeBaseDen != profile.TimeBaseDen ||
		evidence.BFrames != profile.BFrames || evidence.FrameCount != wantFrames || !evidence.FirstFrameKeyframe || len(evidence.ExtradataSHA256) != 64 {
		return windowOutputEvidence{}, fmt.Errorf("prepared fragment does not satisfy canonical profile %q: %+v", profile.ProfileID, evidence)
	}
	return evidence, nil
}

type videoPrepareWindowExecutor struct {
	outputRoot   string
	runner       windowFFmpegRunner
	certifier    windowOutputCertifier
	profile      contract.CanonicalVideoProfileV1
	quality      contract.PreparationQualityPolicy
	encoderMu    sync.Mutex
	encoderVer   string
	encoderSHA   string
	windowLockMu sync.Mutex
	windowLocks  map[string]*keyedWindowLock
}

type keyedWindowLock struct {
	mu   sync.Mutex
	refs int
}

func NewVideoPrepareWindow(outputRoot string) executor.Executor {
	if strings.TrimSpace(outputRoot) == "" {
		outputRoot = filepath.Join(os.TempDir(), "velox", "video-prepare-window")
	}
	return &videoPrepareWindowExecutor{
		outputRoot: outputRoot, runner: ffmpegrunner.NewProcessRunner(), certifier: ffprobeWindowOutputCertifier{},
		profile: contract.CanonicalVideoProfileV1Default, quality: contract.CanonicalPreparationQualityPolicyDefault,
	}
}

func (e *videoPrepareWindowExecutor) Descriptor() executor.Descriptor {
	return executor.Descriptor{
		ID: VideoPrepareWindowID, Version: VideoPrepareWindowVersion,
		InputTypes: []string{"video.prepare.window.v1"}, OutputTypes: []string{"video/mp4"},
		ResourceClass: executor.ResourceCPU, Deterministic: true, Cacheable: true,
		TemporalMode: executor.TemporalWindowed,
	}
}

func (e *videoPrepareWindowExecutor) request(spec executor.TaskSpec) (contract.VideoPrepareWindowV1, error) {
	if spec.ExecutorID != VideoPrepareWindowID {
		return contract.VideoPrepareWindowV1{}, fmt.Errorf("video.prepare.window.v1: executor_id must be %q", VideoPrepareWindowID)
	}
	if e == nil || e.runner == nil || e.certifier == nil {
		return contract.VideoPrepareWindowV1{}, errors.New("video.prepare.window.v1: encoder and output certifier are required")
	}
	raw, ok := spec.Payload[contract.VideoPrepareWindowPayloadKey]
	if !ok {
		return contract.VideoPrepareWindowV1{}, fmt.Errorf("video.prepare.window.v1: %q is required", contract.VideoPrepareWindowPayloadKey)
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return contract.VideoPrepareWindowV1{}, fmt.Errorf("video.prepare.window.v1: encode request: %w", err)
	}
	var request contract.VideoPrepareWindowV1
	if err := json.Unmarshal(encoded, &request); err != nil {
		return contract.VideoPrepareWindowV1{}, fmt.Errorf("video.prepare.window.v1: decode request: %w", err)
	}
	if err := request.Validate(); err != nil {
		return contract.VideoPrepareWindowV1{}, err
	}
	if request.OutputProfileID != e.profile.ProfileID {
		return contract.VideoPrepareWindowV1{}, fmt.Errorf("video.prepare.window.v1: profile %q is not enabled on this worker", request.OutputProfileID)
	}
	wantRevision, err := contract.VideoWindowPreparationRevision(e.profile, e.quality)
	if err != nil || request.OutputProfileRevision != wantRevision {
		return contract.VideoPrepareWindowV1{}, fmt.Errorf("video.prepare.window.v1: preparation revision mismatch")
	}
	return request, nil
}

func (e *videoPrepareWindowExecutor) Validate(spec executor.TaskSpec) error {
	_, err := e.request(spec)
	return err
}

func (e *videoPrepareWindowExecutor) Execute(ctx context.Context, execCtx executor.ExecutionContext, spec executor.TaskSpec) (executor.ExecutionResult, error) {
	started := time.Now().UTC()
	fail := func(code string, err error) (executor.ExecutionResult, error) {
		return executor.ExecutionResult{Status: "failed", ErrorCode: code, ErrorDetail: err.Error(), StartedAt: started, CompletedAt: time.Now().UTC()}, nil
	}
	request, err := e.request(spec)
	if err != nil {
		return fail("WINDOW_PLAN_INVALID", err)
	}
	encoderVersion, encoderBuildSHA, err := e.encoderIdentity(ctx)
	if err != nil {
		return fail("WINDOW_ENCODER_IDENTITY", err)
	}
	bindings, ok := runtimeassets.FromContext(ctx)
	if !ok {
		return fail("WINDOW_SOURCE_MISSING", errors.New("verified source binding is required"))
	}
	source, ok := bindings[request.Source.AssetID]
	if !ok || !source.Verified || source.Size != request.Source.SizeBytes {
		return fail("WINDOW_SOURCE_IDENTITY_MISMATCH", errors.New("resolved source binding does not match the immutable request identity"))
	}
	if request.SourceIdentityDiscovery {
		if !isLowerSHA256(source.SHA256) {
			return fail("WINDOW_SOURCE_IDENTITY_MISMATCH", errors.New("source identity discovery did not produce a SHA-256"))
		}
	} else if source.SHA256 != request.Source.SHA256 {
		return fail("WINDOW_SOURCE_IDENTITY_MISMATCH", errors.New("resolved source binding does not match the immutable request identity"))
	}
	if source.Sparse && request.SourceIdentityDiscovery {
		return fail("WINDOW_SOURCE_IDENTITY_MISMATCH", errors.New("a sparse source cannot attest a new source identity"))
	}
	// Identity discovery resolves the one missing field from the worker's
	// verified download; every downstream cache key, attestation and
	// certificate must use the resolved identity so independent jobs and
	// other workers can reuse the fragment by content.
	resolvedRequest := request
	if request.SourceIdentityDiscovery {
		resolvedRequest.Source.SHA256 = source.SHA256
		resolvedRequest.SourceIdentityDiscovery = false
	}
	info, err := os.Stat(source.Path)
	if err != nil || !info.Mode().IsRegular() || info.Size() != request.Source.SizeBytes {
		if err == nil {
			err = errors.New("source file size changed after resolution")
		}
		return fail("WINDOW_SOURCE_INVALID", err)
	}
	if strings.TrimSpace(spec.JobID) == "" {
		return fail("WINDOW_JOB_ID_INVALID", errors.New("job_id is required"))
	}
	identity, err := contract.VideoPrepareWindowCacheIdentity(resolvedRequest)
	if err != nil {
		return fail("WINDOW_CACHE_IDENTITY", err)
	}
	outputName := identity + ".mp4"
	outputPath := filepath.Join(e.outputRoot, outputName)
	if resolver := storageResolverFromExecutionContext(execCtx); resolver != nil {
		if placement, placeErr := resolver.Place(storage.CachePersistent, filepath.Join("prepared-windows", outputName), estimateOutputBytesFromDuration(int64(float64(request.OutputFrameCount)*1_000_000*float64(e.profile.FPSDen)/float64(e.profile.FPSNum)))); placeErr == nil {
			outputPath = placement.Path
		}
	}
	unlock := e.lockWindow(identity)
	defer unlock()
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o750); err != nil {
		return fail("WINDOW_OUTPUT_DIRECTORY", err)
	}
	if attestation, cacheHit := readWindowArtifactAttestation(outputPath, resolvedRequest, e.profile, encoderVersion, encoderBuildSHA); cacheHit {
		certificate := bindWindowCertificate(attestation, resolvedRequest)
		certificate.CacheHit = true
		encodedCertificate, _ := json.Marshal(certificate)
		completed := time.Now().UTC()
		artifact := executor.ArtifactRef{Type: "video/mp4", Hash: attestation.OutputSHA256, URI: outputPath, SizeBytes: attestation.OutputSizeBytes}
		return executor.ExecutionResult{
			Status: "succeeded", Outputs: []executor.ArtifactRef{artifact},
			Segments: []executor.SegmentTiming{{
				SceneID: request.Source.AssetID, SourceType: "video.prepare.window.v1",
				DurationMS: float64(completed.Sub(started).Milliseconds()), SourceBytes: request.Source.SizeBytes,
				OutputBytes: artifact.SizeBytes, FramesEncoded: request.OutputFrameCount, Codec: attestation.OutputEvidence.Codec,
				Status: "succeeded", CacheKey: "video-window:" + identity, MetadataJSON: string(encodedCertificate),
			}}, StartedAt: started, CompletedAt: completed,
		}, nil
	}
	tmpPath := strings.TrimSuffix(outputPath, ".mp4") + ".partial.mp4"
	_ = os.Remove(tmpPath)
	defer os.Remove(tmpPath)
	var args []string
	if resolvedRequest.SourceDurationUS > 0 {
		args = contract.CanonicalVideoWindowTimestampArgs(e.profile, e.quality, source.Path, tmpPath,
			resolvedRequest.SourceStartUS, resolvedRequest.SourceDurationUS, resolvedRequest.OutputFrameCount)
	} else {
		args = contract.CanonicalVideoWindowArgs(e.profile, e.quality, source.Path, tmpPath,
			resolvedRequest.SourceStartFrame, resolvedRequest.SourceFrameCount, resolvedRequest.SourceFPSNum, resolvedRequest.SourceFPSDen, resolvedRequest.OutputFrameCount)
	}
	encode, err := e.runner.Run(ctx, ffmpegrunner.FFmpegRequest{Operation: ffmpegrunner.OperationEncode, Args: args})
	if err != nil {
		return fail("WINDOW_ENCODE_FAILED", err)
	}
	evidence, err := e.certifier.Certify(ctx, tmpPath, e.profile, request.OutputFrameCount)
	if err != nil {
		return fail("WINDOW_CERTIFICATION_FAILED", err)
	}
	if err := os.Rename(tmpPath, outputPath); err != nil {
		return fail("WINDOW_OUTPUT_COMMIT_FAILED", err)
	}
	artifact, err := artifactFromFile("video/mp4", outputPath)
	if err != nil {
		return fail("WINDOW_OUTPUT_INVALID", err)
	}
	if err := os.Chmod(outputPath, 0o440); err != nil {
		return fail("WINDOW_OUTPUT_IMMUTABLE", err)
	}
	outputInfo, err := os.Stat(outputPath)
	if err != nil {
		return fail("WINDOW_OUTPUT_STAT", err)
	}
	attestation := windowArtifactAttestation{
		Version: contract.VideoPrepareWindowVersion, SourceSHA256: source.SHA256,
		SourceStartFrame: request.SourceStartFrame, SourceFrameCount: request.SourceFrameCount,
		SourceFPSNum: request.SourceFPSNum, SourceFPSDen: request.SourceFPSDen,
		SourceStartUS: request.SourceStartUS, SourceDurationUS: request.SourceDurationUS,
		OutputFrameCount: request.OutputFrameCount, OutputProfileID: request.OutputProfileID,
		OutputProfileRevision: request.OutputProfileRevision, EncoderVersion: encoderVersion,
		EncoderBuildSHA256: encoderBuildSHA, OutputSHA256: artifact.Hash,
		OutputSizeBytes: artifact.SizeBytes, OutputModTimeUnixNano: outputInfo.ModTime().UnixNano(),
		OutputEvidence: evidence, ClosedGOPAttested: e.profile.ClosedGOP,
	}
	attestationJSON, err := json.Marshal(attestation)
	if err != nil {
		return fail("WINDOW_ATTESTATION_ENCODE_FAILED", err)
	}
	if err := writeWindowAttestation(outputPath+".cert.json", attestationJSON); err != nil {
		return fail("WINDOW_ATTESTATION_COMMIT_FAILED", err)
	}
	certificate := bindWindowCertificate(attestation, resolvedRequest)
	certificate.CacheHit = false
	certificateJSON, err := json.Marshal(certificate)
	if err != nil {
		return fail("WINDOW_CERTIFICATE_ENCODE_FAILED", err)
	}
	completed := time.Now().UTC()
	return executor.ExecutionResult{
		Status: "succeeded", Outputs: []executor.ArtifactRef{artifact},
		RawMetrics: rawMetricsFromFFmpegResult(encode),
		Segments: []executor.SegmentTiming{{
			SceneID: request.Source.AssetID, SourceType: "video.prepare.window.v1",
			DurationMS: float64(completed.Sub(started).Milliseconds()), FfmpegEncodeMS: float64(encode.ProcessWallMS),
			SourceBytes: request.Source.SizeBytes, OutputBytes: artifact.SizeBytes, FramesEncoded: request.OutputFrameCount,
			Codec: evidence.Codec, Status: "succeeded", CacheKey: "video-window:" + identity,
			MetadataJSON: string(certificateJSON),
		}}, StartedAt: started, CompletedAt: completed,
	}, nil
}

func (e *videoPrepareWindowExecutor) lockWindow(key string) func() {
	e.windowLockMu.Lock()
	if e.windowLocks == nil {
		e.windowLocks = make(map[string]*keyedWindowLock)
	}
	lock := e.windowLocks[key]
	if lock == nil {
		lock = &keyedWindowLock{}
		e.windowLocks[key] = lock
	}
	lock.refs++
	e.windowLockMu.Unlock()
	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		e.windowLockMu.Lock()
		lock.refs--
		if lock.refs == 0 {
			delete(e.windowLocks, key)
		}
		e.windowLockMu.Unlock()
	}
}

func (e *videoPrepareWindowExecutor) encoderIdentity(ctx context.Context) (string, string, error) {
	e.encoderMu.Lock()
	if e.encoderVer != "" && e.encoderSHA != "" {
		version, build := e.encoderVer, e.encoderSHA
		e.encoderMu.Unlock()
		return version, build, nil
	}
	e.encoderMu.Unlock()
	data, err := exec.CommandContext(ctx, "ffmpeg", "-version").Output()
	if err != nil {
		return "", "", fmt.Errorf("identify FFmpeg encoder build: %w", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) == 0 || strings.TrimSpace(lines[0]) == "" {
		return "", "", errors.New("FFmpeg returned an empty build identity")
	}
	sum := sha256.Sum256(data)
	version, build := strings.TrimSpace(lines[0]), hex.EncodeToString(sum[:])
	e.encoderMu.Lock()
	if e.encoderVer == "" {
		e.encoderVer, e.encoderSHA = version, build
	}
	version, build = e.encoderVer, e.encoderSHA
	e.encoderMu.Unlock()
	return version, build, nil
}

func readWindowArtifactAttestation(
	outputPath string,
	request contract.VideoPrepareWindowV1,
	profile contract.CanonicalVideoProfileV1,
	encoderVersion, encoderBuildSHA string,
) (windowArtifactAttestation, bool) {
	var attestation windowArtifactAttestation
	data, err := os.ReadFile(outputPath + ".cert.json")
	if err != nil || json.Unmarshal(data, &attestation) != nil {
		return windowArtifactAttestation{}, false
	}
	info, err := os.Stat(outputPath)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() != attestation.OutputSizeBytes ||
		info.ModTime().UnixNano() != attestation.OutputModTimeUnixNano || info.Mode().Perm()&0o222 != 0 {
		return windowArtifactAttestation{}, false
	}
	if attestation.Version != contract.VideoPrepareWindowVersion || attestation.SourceSHA256 != request.Source.SHA256 ||
		attestation.SourceStartFrame != request.SourceStartFrame || attestation.SourceFrameCount != request.SourceFrameCount ||
		attestation.SourceFPSNum != request.SourceFPSNum || attestation.SourceFPSDen != request.SourceFPSDen ||
		attestation.SourceStartUS != request.SourceStartUS || attestation.SourceDurationUS != request.SourceDurationUS ||
		attestation.OutputFrameCount != request.OutputFrameCount || attestation.OutputProfileID != request.OutputProfileID ||
		attestation.OutputProfileRevision != request.OutputProfileRevision || attestation.EncoderVersion != encoderVersion ||
		attestation.EncoderBuildSHA256 != encoderBuildSHA || !attestation.ClosedGOPAttested ||
		!isLowerSHA256(attestation.OutputSHA256) || !windowEvidenceMatchesProfile(attestation.OutputEvidence, profile, request.OutputFrameCount) {
		return windowArtifactAttestation{}, false
	}
	return attestation, true
}

func windowEvidenceMatchesProfile(evidence windowOutputEvidence, profile contract.CanonicalVideoProfileV1, frameCount int64) bool {
	return evidence.Codec == profile.Codec && evidence.Profile == strings.ToLower(profile.CodecProfile) &&
		evidence.Level == canonicalLevelNumber(profile.CodecLevel) && evidence.Width == profile.Width && evidence.Height == profile.Height &&
		evidence.PixelFormat == profile.PixelFormat && evidence.FPSNum == profile.FPSNum && evidence.FPSDen == profile.FPSDen &&
		evidence.TimeBaseNum == profile.TimeBaseNum && evidence.TimeBaseDen == profile.TimeBaseDen &&
		evidence.BFrames == profile.BFrames && evidence.FrameCount == frameCount && evidence.FirstFrameKeyframe &&
		isLowerSHA256(evidence.ExtradataSHA256)
}

func bindWindowCertificate(attestation windowArtifactAttestation, request contract.VideoPrepareWindowV1) windowPreparationCertificate {
	return windowPreparationCertificate{
		Version: attestation.Version, SourceSHA256: attestation.SourceSHA256,
		SourceStartFrame: attestation.SourceStartFrame, SourceFrameCount: attestation.SourceFrameCount,
		SourceFPSNum: attestation.SourceFPSNum, SourceFPSDen: attestation.SourceFPSDen,
		SourceStartUS: attestation.SourceStartUS, SourceDurationUS: attestation.SourceDurationUS,
		OutputFrameCount: attestation.OutputFrameCount, OutputProfileID: attestation.OutputProfileID,
		OutputProfileRevision: attestation.OutputProfileRevision, EncoderVersion: attestation.EncoderVersion,
		EncoderBuildSHA256: attestation.EncoderBuildSHA256, OutputSHA256: attestation.OutputSHA256,
		OutputSizeBytes: attestation.OutputSizeBytes, OutputEvidence: attestation.OutputEvidence,
		ClosedGOPAttested: attestation.ClosedGOPAttested,
		SourceAssetID:     request.Source.AssetID, SourceSizeBytes: request.Source.SizeBytes,
		TimelineRevision: request.TimelineRevision, TimelineSHA256: request.TimelineSHA256,
		TimelineStartFrame: request.TimelineStartFrame,
	}
}

func writeWindowAttestation(path string, data []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".window-attestation-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := file.Name()
	defer os.Remove(tmpPath)
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Chmod(0o440); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func parseWindowRatio(raw string) (int, int) {
	parts := strings.Split(strings.TrimSpace(raw), "/")
	if len(parts) != 2 {
		return 0, 0
	}
	numerator, err1 := strconv.Atoi(parts[0])
	denominator, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil || numerator <= 0 || denominator <= 0 {
		return 0, 0
	}
	return numerator, denominator
}

func canonicalLevelNumber(level string) int {
	parts := strings.Split(strings.TrimSpace(level), ".")
	if len(parts) != 2 {
		return 0
	}
	major, err1 := strconv.Atoi(parts[0])
	minor, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil {
		return 0
	}
	return major*10 + minor
}

func isLowerSHA256(value string) bool {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
