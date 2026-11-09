package executors

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
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

// windowExtradataReference produces the avcC (SPS/PPS) hash that the
// canonical encoder contract must yield on this worker's encoder build. A
// fragment whose extradata differs can pass a formal 64-hex check yet be
// rejected later by the packet-copy muxer, so certification compares against
// this reference instead of mere presence.
type windowExtradataReference interface {
	ExpectedExtradataSHA256(context.Context, contract.CanonicalVideoProfileV1, contract.PreparationQualityPolicy) (string, error)
}

type ffmpegWindowExtradataReference struct{}

func (ffmpegWindowExtradataReference) ExpectedExtradataSHA256(ctx context.Context, profile contract.CanonicalVideoProfileV1, quality contract.PreparationQualityPolicy) (string, error) {
	if err := profile.Validate(); err != nil {
		return "", err
	}
	if err := quality.Validate(); err != nil {
		return "", err
	}
	dir, err := os.MkdirTemp("", "velox-avcc-reference-*")
	if err != nil {
		return "", fmt.Errorf("canonical extradata reference: create temp dir: %w", err)
	}
	defer os.RemoveAll(dir)
	outputPath := filepath.Join(dir, "reference.mp4")
	source := fmt.Sprintf("color=c=black:s=%dx%d:r=%d/%d:d=1", profile.Width, profile.Height, profile.FPSNum, profile.FPSDen)
	args := []string{
		"-hide_banner", "-loglevel", "error", "-y", "-f", "lavfi", "-i", source,
		"-frames:v", "1", "-an", "-sn", "-dn", "-vf", contract.CanonicalVideoScaleFilter(profile),
	}
	args = append(args, contract.CanonicalVideoEncodeArgs(profile, quality)...)
	args = append(args, "-movflags", "+faststart", outputPath)
	if output, err := exec.CommandContext(ctx, "ffmpeg", args...).CombinedOutput(); err != nil {
		return "", fmt.Errorf("canonical extradata reference encode: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return probeExtradataSHA256(ctx, outputPath)
}

func probeExtradataSHA256(ctx context.Context, path string) (string, error) {
	args := []string{"-v", "error", "-select_streams", "v:0", "-show_data_hash", "sha256", "-show_entries", "stream=extradata_hash", "-of", "default=noprint_wrappers=1:nokey=1", path}
	data, err := exec.CommandContext(ctx, "ffprobe", args...).Output()
	if err != nil {
		return "", fmt.Errorf("ffprobe canonical extradata: %w", err)
	}
	hash := strings.TrimPrefix(strings.TrimSpace(string(data)), "SHA256:")
	if !isLowerSHA256(hash) {
		return "", fmt.Errorf("canonical extradata reference: ffprobe returned %q, want a lowercase SHA-256", hash)
	}
	return hash, nil
}

type ffprobeWindowOutputCertifier struct{}

type windowFFprobeDocument struct {
	Streams []windowFFprobeStream `json:"streams"`
	Frames  []struct {
		KeyFrame            int    `json:"key_frame"`
		BestEffortTimestamp string `json:"best_effort_timestamp_time"`
	} `json:"frames"`
}

type windowFFprobeStream struct {
	CodecType     string `json:"codec_type"`
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
	BitRate       string `json:"bit_rate"`
	StartTime     string `json:"start_time"`
}

func (ffprobeWindowOutputCertifier) Certify(ctx context.Context, path string, profile contract.CanonicalVideoProfileV1, wantFrames int64) (windowOutputEvidence, error) {
	// Include frame key flags in the stream probe so certification needs one
	// ffprobe process per fragment. Prepared windows are short, and this keeps
	// the independent first-frame keyframe check without spawning a second
	// process for every preparation task.
	args := []string{"-v", "error", "-count_frames", "-show_frames", "-show_data_hash", "sha256", "-show_entries", "stream=codec_type,codec_name,profile,level,width,height,pix_fmt,r_frame_rate,time_base,has_b_frames,nb_read_frames,extradata_hash:frame=key_frame", "-of", "json", path}
	data, err := exec.CommandContext(ctx, "ffprobe", args...).Output()
	if err != nil {
		return windowOutputEvidence{}, fmt.Errorf("ffprobe prepared window: %w", err)
	}
	var doc windowFFprobeDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		return windowOutputEvidence{}, fmt.Errorf("decode prepared window probe: %w", err)
	}
	if len(doc.Streams) != 1 {
		return windowOutputEvidence{}, fmt.Errorf("prepared fragment has %d streams, expected one video stream", len(doc.Streams))
	}
	video := &doc.Streams[0]
	fpsNum, fpsDen := parseWindowRatio(video.FrameRate)
	tbNum, tbDen := parseWindowRatio(video.TimeBase)
	frameCount, parseErr := strconv.ParseInt(video.FrameCount, 10, 64)
	if parseErr != nil {
		return windowOutputEvidence{}, fmt.Errorf("prepared fragment frame count is unavailable: %w", parseErr)
	}
	firstFrameKeyframe := len(doc.Frames) > 0 && doc.Frames[0].KeyFrame == 1
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
	outputRoot    string
	runner        windowFFmpegRunner
	certifier     windowOutputCertifier
	extradataRef  windowExtradataReference
	profile       contract.CanonicalVideoProfileV1
	quality       contract.PreparationQualityPolicy
	encoderMu     sync.Mutex
	encoderVer    string
	encoderSHA    string
	extradataMu   sync.Mutex
	extradataKey  string
	extradataSHA  string
	windowLockMu  sync.Mutex
	windowLocks   map[string]*keyedWindowLock
	sourceProbeMu sync.Mutex
	sourceProbes  map[string]windowSourceProbe
}

type windowSourceProbe struct {
	CodecName     string
	Profile       string
	Level         int
	Width         int
	Height        int
	PixelFormat   string
	FPSNum        int
	FPSDen        int
	TimeBaseNum   int
	TimeBaseDen   int
	BFrames       int
	BitRateBPS    int64
	StartTimeSec  float64
	ExtradataSHA  string
	KeyframeTimes []float64
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
		extradataRef: ffmpegWindowExtradataReference{},
		profile:      contract.CanonicalVideoProfileV1Default, quality: contract.CanonicalPreparationQualityPolicyDefault,
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
	expectedExtradata, err := e.canonicalExtradata(ctx, encoderBuildSHA)
	if err != nil {
		return fail("WINDOW_EXTRADATA_REFERENCE", err)
	}
	if strings.TrimSpace(spec.JobID) == "" {
		return fail("WINDOW_JOB_ID_INVALID", errors.New("job_id is required"))
	}
	resolver := storageResolverFromExecutionContext(execCtx)
	cacheHitResult := func(identity, outputPath string, resolved contract.VideoPrepareWindowV1, attestation windowArtifactAttestation) (executor.ExecutionResult, error) {
		certificate := bindWindowCertificate(attestation, resolved)
		certificate.CacheHit = true
		encodedCertificate, _ := json.Marshal(certificate)
		completed := time.Now().UTC()
		artifact := executor.ArtifactRef{Type: "video/mp4", Hash: attestation.OutputSHA256, URI: outputPath, SizeBytes: attestation.OutputSizeBytes}
		return executor.ExecutionResult{
			Status: "succeeded", Outputs: []executor.ArtifactRef{artifact},
			Segments: []executor.SegmentTiming{{
				SceneID: request.Source.AssetID, SourceType: "video.prepare.window.v1",
				DurationMS: float64(completed.Sub(started).Milliseconds()), SourceBytes: request.Source.SizeBytes,
				// A cache hit performs no encode. FramesEncoded is encoder work;
				// the pre-existing output frame count is carried separately by
				// the certificate's output_frame_count.
				OutputBytes: artifact.SizeBytes, FramesEncoded: 0, Codec: attestation.OutputEvidence.Codec,
				Status: "succeeded", CacheKey: "video-window:" + identity, MetadataJSON: string(encodedCertificate),
			}}, StartedAt: started, CompletedAt: completed,
		}, nil
	}

	// Cache-first: a known content identity can be served from the local
	// fragment cache before the worker resolves, downloads or hashes the
	// source. Only a miss proceeds to source materialization.
	var identity, outputPath string
	resolvedRequest := request
	if !request.SourceIdentityDiscovery {
		identity, err = contract.VideoPrepareWindowCacheIdentity(request)
		if err != nil {
			return fail("WINDOW_CACHE_IDENTITY", err)
		}
		outputPath = e.preparedWindowPath(identity, request, resolver)
		unlock := e.lockWindow(identity)
		defer unlock()
		if attestation, cacheHit := readWindowArtifactAttestation(outputPath, request, e.profile, encoderVersion, encoderBuildSHA, expectedExtradata); cacheHit {
			return cacheHitResult(identity, outputPath, request, attestation)
		}
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
	if identity == "" {
		identity, err = contract.VideoPrepareWindowCacheIdentity(resolvedRequest)
		if err != nil {
			return fail("WINDOW_CACHE_IDENTITY", err)
		}
		outputPath = e.preparedWindowPath(identity, resolvedRequest, resolver)
		unlock := e.lockWindow(identity)
		defer unlock()
		if attestation, cacheHit := readWindowArtifactAttestation(outputPath, resolvedRequest, e.profile, encoderVersion, encoderBuildSHA, expectedExtradata); cacheHit {
			return cacheHitResult(identity, outputPath, resolvedRequest, attestation)
		}
	}
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o750); err != nil {
		return fail("WINDOW_OUTPUT_DIRECTORY", err)
	}
	tmpPath := strings.TrimSuffix(outputPath, ".mp4") + ".partial.mp4"
	_ = os.Remove(tmpPath)
	defer os.Remove(tmpPath)
	var (
		encode          ffmpegrunner.FFmpegResult
		evidence        windowOutputEvidence
		packetCopied    bool
		closedGOPProven bool
	)
	if sourceProbe, probeErr := e.probeSource(ctx, source.Path, source.SHA256); probeErr == nil && sourceWindowCopyEligible(sourceProbe, resolvedRequest, e.profile, e.quality, expectedExtradata) {
		copyResult, copyErr := e.runner.Run(ctx, ffmpegrunner.FFmpegRequest{
			Operation: ffmpegrunner.OperationEncode,
			Args:      canonicalWindowStreamCopyArgs(e.profile, source.Path, tmpPath, resolvedRequest),
		})
		if copyErr == nil {
			copyEvidence, certifyErr := e.certifier.Certify(ctx, tmpPath, e.profile, request.OutputFrameCount)
			if certifyErr == nil && copyEvidence.ExtradataSHA256 == expectedExtradata && copyEvidence.BFrames == 0 && isFirstFrameIDR(ctx, tmpPath) {
				encode, evidence, packetCopied, closedGOPProven = copyResult, copyEvidence, true, true
			}
		}
	}
	if !packetCopied {
		var args []string
		if resolvedRequest.SourceDurationUS > 0 {
			args = contract.CanonicalVideoWindowTimestampArgs(e.profile, e.quality, source.Path, tmpPath,
				resolvedRequest.SourceStartUS, resolvedRequest.SourceDurationUS, resolvedRequest.OutputFrameCount)
		} else {
			args = contract.CanonicalVideoWindowArgs(e.profile, e.quality, source.Path, tmpPath,
				resolvedRequest.SourceStartFrame, resolvedRequest.SourceFrameCount, resolvedRequest.SourceFPSNum, resolvedRequest.SourceFPSDen, resolvedRequest.OutputFrameCount)
		}
		encode, err = e.runner.Run(ctx, ffmpegrunner.FFmpegRequest{Operation: ffmpegrunner.OperationEncode, Args: args})
		if err != nil {
			return fail("WINDOW_ENCODE_FAILED", err)
		}
		evidence, err = e.certifier.Certify(ctx, tmpPath, e.profile, request.OutputFrameCount)
		if err != nil {
			return fail("WINDOW_CERTIFICATION_FAILED", err)
		}
		if evidence.ExtradataSHA256 != expectedExtradata {
			return fail("WINDOW_EXTRADATA_IDENTITY_MISMATCH", fmt.Errorf("prepared fragment avcC %s does not match the canonical encoder reference %s", evidence.ExtradataSHA256, expectedExtradata))
		}
		closedGOPProven = e.profile.ClosedGOP
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
		OutputEvidence: evidence, ClosedGOPAttested: closedGOPProven,
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
			DurationMS: float64(completed.Sub(started).Milliseconds()), FfmpegEncodeMS: ffmpegEncodeMS(encode.ProcessWallMS, packetCopied),
			SourceBytes: request.Source.SizeBytes, OutputBytes: artifact.SizeBytes, FramesEncoded: encodedFrames(request.OutputFrameCount, packetCopied),
			Codec: evidence.Codec, Status: "succeeded", CacheKey: "video-window:" + identity,
			MetadataJSON: string(certificateJSON),
		}}, StartedAt: started, CompletedAt: completed,
	}, nil
}

func (e *videoPrepareWindowExecutor) preparedWindowPath(identity string, request contract.VideoPrepareWindowV1, resolver *storage.Resolver) string {
	outputName := identity + ".mp4"
	outputPath := filepath.Join(e.outputRoot, outputName)
	if resolver != nil {
		if placement, err := resolver.Place(storage.CachePersistent, filepath.Join("prepared-windows", outputName), estimateOutputBytesFromDuration(int64(float64(request.OutputFrameCount)*1_000_000*float64(e.profile.FPSDen)/float64(e.profile.FPSNum)))); err == nil {
			outputPath = placement.Path
		}
	}
	return outputPath
}

func (e *videoPrepareWindowExecutor) probeSource(ctx context.Context, path, sha256 string) (windowSourceProbe, error) {
	if e != nil {
		e.sourceProbeMu.Lock()
		cached, ok := e.sourceProbes[sha256]
		e.sourceProbeMu.Unlock()
		if ok {
			return cached, nil
		}
	}
	args := []string{
		"-v", "error", "-skip_frame", "nokey", "-select_streams", "v:0", "-show_streams", "-show_frames", "-show_data_hash", "sha256",
		"-show_entries", "stream=codec_type,codec_name,profile,level,width,height,pix_fmt,r_frame_rate,time_base,has_b_frames,extradata_hash,bit_rate,start_time:frame=key_frame,best_effort_timestamp_time",
		"-of", "json", path,
	}
	data, err := exec.CommandContext(ctx, "ffprobe", args...).Output()
	if err != nil {
		return windowSourceProbe{}, fmt.Errorf("ffprobe source window: %w", err)
	}
	var doc windowFFprobeDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		return windowSourceProbe{}, fmt.Errorf("decode source window probe: %w", err)
	}
	if len(doc.Streams) != 1 || doc.Streams[0].CodecType != "video" {
		return windowSourceProbe{}, fmt.Errorf("source window has %d selected video streams, expected one", len(doc.Streams))
	}
	stream := doc.Streams[0]
	fpsNum, fpsDen := parseWindowRatio(stream.FrameRate)
	tbNum, tbDen := parseWindowRatio(stream.TimeBase)
	bitRate, err := parseWindowBitRateBPS(stream.BitRate)
	if err != nil {
		return windowSourceProbe{}, err
	}
	startTime := 0.0
	if strings.TrimSpace(stream.StartTime) != "" {
		startTime, err = strconv.ParseFloat(stream.StartTime, 64)
		if err != nil {
			return windowSourceProbe{}, fmt.Errorf("source window has invalid start_time %q", stream.StartTime)
		}
	}
	probe := windowSourceProbe{
		CodecName: stream.CodecName, Profile: strings.ToLower(stream.Profile), Level: stream.Level,
		Width: stream.Width, Height: stream.Height, PixelFormat: stream.PixelFormat,
		FPSNum: fpsNum, FPSDen: fpsDen, TimeBaseNum: tbNum, TimeBaseDen: tbDen,
		BFrames: stream.BFrames, BitRateBPS: bitRate, StartTimeSec: startTime,
		ExtradataSHA: strings.TrimPrefix(strings.TrimSpace(stream.ExtradataHash), "SHA256:"),
	}
	for _, frame := range doc.Frames {
		if frame.KeyFrame != 1 || strings.TrimSpace(frame.BestEffortTimestamp) == "" {
			continue
		}
		timestamp, err := strconv.ParseFloat(frame.BestEffortTimestamp, 64)
		if err != nil {
			continue
		}
		probe.KeyframeTimes = append(probe.KeyframeTimes, timestamp)
	}
	if e != nil {
		e.sourceProbeMu.Lock()
		if e.sourceProbes == nil {
			e.sourceProbes = make(map[string]windowSourceProbe)
		}
		e.sourceProbes[sha256] = probe
		e.sourceProbeMu.Unlock()
	}
	return probe, nil
}

func sourceWindowCopyEligible(source windowSourceProbe, request contract.VideoPrepareWindowV1, profile contract.CanonicalVideoProfileV1, quality contract.PreparationQualityPolicy, expectedExtradata string) bool {
	if profile.BFrames != 0 || !profile.ClosedGOP || source.BFrames != 0 || source.CodecName != profile.Codec ||
		source.Profile != strings.ToLower(profile.CodecProfile) || source.Level != canonicalLevelNumber(profile.CodecLevel) ||
		source.Width != profile.Width || source.Height != profile.Height || source.PixelFormat != profile.PixelFormat ||
		source.FPSNum != profile.FPSNum || source.FPSDen != profile.FPSDen ||
		source.TimeBaseNum != profile.TimeBaseNum || source.TimeBaseDen != profile.TimeBaseDen ||
		source.ExtradataSHA != expectedExtradata || !isLowerSHA256(source.ExtradataSHA) {
		return false
	}
	maxRate, err := parseWindowBitRateBPS(quality.VideoMaxRate)
	if err != nil || maxRate <= 0 || source.BitRateBPS <= 0 || source.BitRateBPS > maxRate {
		return false
	}
	startSeconds, durationSeconds, ok := requestedWindowSeconds(request)
	if !ok || absFloat(source.StartTimeSec) > 1e-6 || request.OutputFrameCount <= 0 {
		return false
	}
	wantFrameCount := durationSeconds * float64(profile.FPSNum) / float64(profile.FPSDen)
	if absFloat(wantFrameCount-float64(request.OutputFrameCount)) > 1e-6 {
		return false
	}
	startFrame := startSeconds * float64(profile.FPSNum) / float64(profile.FPSDen)
	if absFloat(startFrame-math.Round(startFrame)) > 1e-6 {
		return false
	}
	for _, keyframeTime := range source.KeyframeTimes {
		if absFloat(keyframeTime-startSeconds) <= 1e-6 {
			// The end may be any frame: with B-frames disabled no packet in
			// the copied window references a frame after its final packet.
			return true
		}
	}
	return false
}

func requestedWindowSeconds(request contract.VideoPrepareWindowV1) (float64, float64, bool) {
	if request.SourceDurationUS > 0 {
		return float64(request.SourceStartUS) / 1_000_000, float64(request.SourceDurationUS) / 1_000_000, true
	}
	if request.SourceFPSNum <= 0 || request.SourceFPSDen <= 0 || request.SourceFrameCount <= 0 {
		return 0, 0, false
	}
	fps := float64(request.SourceFPSNum) / float64(request.SourceFPSDen)
	return float64(request.SourceStartFrame) / fps, float64(request.SourceFrameCount) / fps, true
}

func canonicalWindowStreamCopyArgs(profile contract.CanonicalVideoProfileV1, inputPath, outputPath string, request contract.VideoPrepareWindowV1) []string {
	startSeconds, _, _ := requestedWindowSeconds(request)
	return []string{
		"-hide_banner", "-loglevel", "error", "-y", "-ss", strconv.FormatFloat(startSeconds, 'f', 6, 64), "-i", inputPath,
		"-map", "0:v:0", "-an", "-sn", "-dn", "-c:v", "copy", "-frames:v", strconv.FormatInt(request.OutputFrameCount, 10),
		"-video_track_timescale", strconv.Itoa(profile.TimeBaseDen), "-avoid_negative_ts", "make_zero", "-reset_timestamps", "1",
		"-movflags", "+faststart", outputPath,
	}
}

func isFirstFrameIDR(ctx context.Context, path string) bool {
	args := []string{
		"-hide_banner", "-loglevel", "verbose", "-i", path, "-map", "0:v:0", "-frames:v", "1", "-c:v", "copy",
		"-bsf:v", "trace_headers", "-f", "null", "-",
	}
	output, err := exec.CommandContext(ctx, "ffmpeg", args...).CombinedOutput()
	if err != nil {
		return false
	}
	for _, line := range strings.Split(string(output), "\n") {
		if !strings.Contains(line, "nal_unit_type") {
			continue
		}
		if strings.TrimSpace(line[strings.LastIndex(line, "=")+1:]) == "5" {
			return true
		}
	}
	return false
}

func parseWindowBitRateBPS(value string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" || value == "N/A" {
		return 0, nil
	}
	multiplier := float64(1)
	last := value[len(value)-1]
	switch last {
	case 'k', 'K':
		multiplier, value = 1_000, value[:len(value)-1]
	case 'm', 'M':
		multiplier, value = 1_000_000, value[:len(value)-1]
	case 'g', 'G':
		multiplier, value = 1_000_000_000, value[:len(value)-1]
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || parsed <= 0 || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return 0, fmt.Errorf("invalid video bitrate %q", value)
	}
	return int64(parsed * multiplier), nil
}

func absFloat(value float64) float64 {
	if value < 0 {
		return -value
	}
	return value
}

func encodedFrames(frameCount int64, packetCopied bool) int64 {
	if packetCopied {
		return 0
	}
	return frameCount
}

func ffmpegEncodeMS(processWallMS int64, packetCopied bool) float64 {
	if packetCopied {
		return 0
	}
	return float64(processWallMS)
}

// ProbeLocalPreparation reports whether this task can be served entirely from
// the worker-local prepared-fragment cache. Worker dispatch calls it before
// resolving the source, so a warm fragment never triggers a download merely to
// discover that the output already exists.
func (e *videoPrepareWindowExecutor) ProbeLocalPreparation(ctx context.Context, spec executor.TaskSpec, resolver *storage.Resolver) (bool, error) {
	request, err := e.request(spec)
	if err != nil {
		return false, err
	}
	if request.SourceIdentityDiscovery {
		return false, nil
	}
	encoderVersion, encoderBuildSHA, err := e.encoderIdentity(ctx)
	if err != nil {
		return false, err
	}
	expectedExtradata, err := e.canonicalExtradata(ctx, encoderBuildSHA)
	if err != nil {
		return false, err
	}
	identity, err := contract.VideoPrepareWindowCacheIdentity(request)
	if err != nil {
		return false, err
	}
	outputPath := e.preparedWindowPath(identity, request, resolver)
	_, cacheHit := readWindowArtifactAttestation(outputPath, request, e.profile, encoderVersion, encoderBuildSHA, expectedExtradata)
	return cacheHit, nil
}

func (e *videoPrepareWindowExecutor) canonicalExtradata(ctx context.Context, encoderBuildSHA string) (string, error) {
	e.extradataMu.Lock()
	if e.extradataKey == encoderBuildSHA && e.extradataSHA != "" {
		hash := e.extradataSHA
		e.extradataMu.Unlock()
		return hash, nil
	}
	e.extradataMu.Unlock()
	ref := e.extradataRef
	if ref == nil {
		ref = ffmpegWindowExtradataReference{}
	}
	hash, err := ref.ExpectedExtradataSHA256(ctx, e.profile, e.quality)
	if err != nil {
		return "", err
	}
	e.extradataMu.Lock()
	if e.extradataKey == "" {
		e.extradataKey, e.extradataSHA = encoderBuildSHA, hash
	}
	cached := e.extradataSHA
	e.extradataMu.Unlock()
	return cached, nil
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
	encoderVersion, encoderBuildSHA, expectedExtradata string,
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
		!isLowerSHA256(attestation.OutputSHA256) || attestation.OutputEvidence.ExtradataSHA256 != expectedExtradata ||
		!windowEvidenceMatchesProfile(attestation.OutputEvidence, profile, request.OutputFrameCount) {
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
