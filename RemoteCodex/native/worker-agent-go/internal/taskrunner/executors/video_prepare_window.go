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
	PacketCopyUsed        bool                 `json:"packet_copy_used"`
}

type windowPreparationCertificate = contract.VideoPrepareWindowCertificateV1

type windowOutputCertifier interface {
	Certify(context.Context, string, contract.CanonicalVideoProfileV1, int64) (windowOutputEvidence, error)
}

// ffprobeWindowOutputCertifier verifies packet-copy output against the
// canonical stream contract before it can enter the prepared-fragment cache.
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
	profile       contract.CanonicalVideoProfileV1
	quality       contract.PreparationQualityPolicy
	encoderMu     sync.Mutex
	encoderVer    string
	encoderSHA    string
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

// windowSourceProbeIndex binds a persisted keyframe index to the immutable
// source bytes and probe schema that produced it. The source hash is part of
// the filename too, but keeping it in the document makes accidental/misplaced
// cache entries fail closed.
type windowSourceProbeIndex struct {
	Version   int               `json:"version"`
	SourceSHA string            `json:"source_sha256"`
	Probe     windowSourceProbe `json:"probe"`
}

const windowSourceProbeIndexVersion = 1

type keyedWindowLock struct {
	mu   sync.Mutex
	refs int
}

func NewVideoPrepareWindow(outputRoot string) executor.Executor {
	if strings.TrimSpace(outputRoot) == "" {
		outputRoot = filepath.Join(os.TempDir(), "velox", "video-prepare-window")
	}
	exec := &videoPrepareWindowExecutor{
		outputRoot: outputRoot, runner: ffmpegrunner.NewProcessRunner(), certifier: ffprobeWindowOutputCertifier{},
		profile: contract.CanonicalVideoProfileV1Default, quality: contract.CanonicalPreparationQualityPolicyDefault,
	}
	return exec
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
		return contract.VideoPrepareWindowV1{}, errors.New("video.prepare.window.v1: packet-copy runner and output certifier are required")
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
	if !contract.SupportsVideoWindowPreparationRevision(e.profile, e.quality, request.OutputProfileRevision) {
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
	expectedExtradata := contract.CanonicalVideoExtradataSHA256
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
		copyResult   ffmpegrunner.FFmpegResult
		evidence     windowOutputEvidence
		packetCopied bool
		copyEligible bool
		copyFallback []string
	)
	sourceProbe, probeErr := e.probeSource(ctx, source.Path, source.SHA256)
	if probeErr != nil {
		copyFallback = []string{"source_probe_failed"}
	} else {
		eligibility := diagnoseSourceWindowCopy(sourceProbe, resolvedRequest, e.profile, e.quality, expectedExtradata)
		copyEligible = eligibility.Eligible
		copyFallback = eligibility.Reasons
		if eligibility.Eligible {
			var copyErr error
			copyResult, copyErr = e.runner.Run(ctx, ffmpegrunner.FFmpegRequest{
				Operation: ffmpegrunner.OperationPacketCopy,
				Args:      canonicalWindowStreamCopyArgs(e.profile, source.Path, tmpPath, resolvedRequest),
			})
			if copyErr != nil {
				copyFallback = []string{"packet_copy_failed"}
			} else {
				copyEvidence, certifyErr := e.certifier.Certify(ctx, tmpPath, e.profile, request.OutputFrameCount)
				if certifyErr != nil {
					copyFallback = []string{"packet_copy_certification_failed"}
				} else if copyEvidence.ExtradataSHA256 != expectedExtradata {
					copyFallback = []string{"packet_copy_extradata_mismatch"}
				} else if copyEvidence.BFrames != 0 {
					copyFallback = []string{"packet_copy_b_frames"}
				} else if !isFirstFrameIDR(ctx, tmpPath) {
					copyFallback = []string{"packet_copy_first_frame_not_idr"}
				} else {
					evidence, packetCopied = copyEvidence, true
				}
			}
		}
	}
	if !packetCopied {
		reason := strings.Join(copyFallback, ",")
		if reason == "" {
			reason = "packet_copy_not_certified"
		}
		return fail("WINDOW_PACKET_COPY_INCOMPATIBLE", fmt.Errorf("source window cannot be assembled without video re-encoding: %s", reason))
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
		OutputEvidence: evidence, ClosedGOPAttested: true, PacketCopyUsed: true,
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
	var certificateFields map[string]json.RawMessage
	if err := json.Unmarshal(certificateJSON, &certificateFields); err != nil {
		return fail("WINDOW_CERTIFICATE_ENCODE_FAILED", err)
	}
	copyDiagnostic, err := json.Marshal(struct {
		Eligible bool     `json:"packet_copy_eligible"`
		Used     bool     `json:"packet_copy_used"`
		Reasons  []string `json:"packet_copy_fallback_reasons,omitempty"`
	}{Eligible: copyEligible, Used: packetCopied, Reasons: copyFallback})
	if err != nil {
		return fail("WINDOW_CERTIFICATE_ENCODE_FAILED", err)
	}
	var copyDiagnosticFields map[string]json.RawMessage
	if err := json.Unmarshal(copyDiagnostic, &copyDiagnosticFields); err != nil {
		return fail("WINDOW_CERTIFICATE_ENCODE_FAILED", err)
	}
	for key, value := range copyDiagnosticFields {
		certificateFields[key] = value
	}
	certificateJSON, err = json.Marshal(certificateFields)
	if err != nil {
		return fail("WINDOW_CERTIFICATE_ENCODE_FAILED", err)
	}
	completed := time.Now().UTC()
	return executor.ExecutionResult{
		Status: "succeeded", Outputs: []executor.ArtifactRef{artifact},
		RawMetrics: rawMetricsFromFFmpegResult(copyResult),
		Segments: []executor.SegmentTiming{{
			SceneID: request.Source.AssetID, SourceType: "video.prepare.window.v1",
			DurationMS: float64(completed.Sub(started).Milliseconds()), FfmpegEncodeMS: 0,
			SourceBytes: request.Source.SizeBytes, OutputBytes: artifact.SizeBytes, FramesEncoded: 0,
			Codec: evidence.Codec, Status: "succeeded", CacheKey: "video-window:" + identity,
			MetadataJSON: string(certificateJSON),
		}}, StartedAt: started, CompletedAt: completed,
	}, nil
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
	expectedExtradata := contract.CanonicalVideoExtradataSHA256
	identity, err := contract.VideoPrepareWindowCacheIdentity(request)
	if err != nil {
		return false, err
	}
	outputPath := e.preparedWindowPath(identity, request, resolver)
	_, cacheHit := readWindowArtifactAttestation(outputPath, request, e.profile, encoderVersion, encoderBuildSHA, expectedExtradata)
	return cacheHit, nil
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
	defer e.encoderMu.Unlock()
	if e.encoderVer != "" && e.encoderSHA != "" {
		return e.encoderVer, e.encoderSHA, nil
	}
	data, err := exec.CommandContext(ctx, "ffmpeg", "-version").Output()
	if err != nil {
		return "", "", fmt.Errorf("identify FFmpeg packet-copy toolchain: %w", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) == 0 || strings.TrimSpace(lines[0]) == "" {
		return "", "", errors.New("FFmpeg returned an empty toolchain identity")
	}
	sum := sha256.Sum256(data)
	version, build := strings.TrimSpace(lines[0]), hex.EncodeToString(sum[:])
	e.encoderVer, e.encoderSHA = version, build
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
		attestation.EncoderBuildSHA256 != encoderBuildSHA || !attestation.ClosedGOPAttested || !attestation.PacketCopyUsed ||
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
		ClosedGOPAttested: attestation.ClosedGOPAttested, PacketCopyUsed: attestation.PacketCopyUsed,
		SourceAssetID: request.Source.AssetID, SourceSizeBytes: request.Source.SizeBytes,
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
