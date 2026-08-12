package executors

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"velox-shared/contract"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/publisher"
	"velox-worker-agent/internal/runtimeassets"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/video/ffmpegrunner"
)

const (
	// RenderBatchID is the canonical executor ID for the V2 compiled-plan path.
	RenderBatchID = "render_batch"

	// RenderBatchVersion is the first version of the V2 batch contract.
	RenderBatchVersion = 1
)

var (
	ErrMissingRenderBatchBindings = errors.New("render_batch@1: resolved asset bindings are required")
	ErrRenderBatchAssetIntegrity  = errors.New("render_batch@1: asset binding integrity mismatch")
)

type renderBatchExecutor struct {
	descriptor executor.Descriptor
	runner     ffmpegrunner.FFmpegRunner
	outputRoot string
	probe      func(context.Context, string) (publisher.MediaProbe, error)
}

// NewRenderBatch constructs the canonical render_batch@1 executor.
func NewRenderBatch(runner ffmpegrunner.FFmpegRunner, outputRoot string) executor.Executor {
	if runner == nil {
		runner = ffmpegrunner.NewProcessRunner()
	}
	if strings.TrimSpace(outputRoot) == "" {
		outputRoot = filepath.Join(os.TempDir(), "velox", "render-batch")
	}
	return &renderBatchExecutor{
		descriptor: executor.Descriptor{
			ID:            RenderBatchID,
			Version:       RenderBatchVersion,
			InputTypes:    []string{"render.compiled.v2"},
			OutputTypes:   []string{"video/mp4"},
			ResourceClass: executor.ResourceCPU,
			Deterministic: true,
			Cacheable:     true,
			TemporalMode:  executor.TemporalGlobal,
		},
		runner:     runner,
		outputRoot: outputRoot,
		probe:      publisher.ProbeMediaDetails,
	}
}

func (e *renderBatchExecutor) Descriptor() executor.Descriptor { return e.descriptor }

// Validate admits only a complete, strict, canonical V2 envelope. Legacy
// render_plan/render_plan_json payloads remain owned by the V1 executors.
func (e *renderBatchExecutor) Validate(spec executor.TaskSpec) error {
	if spec.ExecutorID != RenderBatchID {
		return fmt.Errorf("render_batch@1: executor_id must be %q, got %q", RenderBatchID, spec.ExecutorID)
	}
	if spec.Payload == nil {
		return errors.New("render_batch@1: payload is required")
	}
	if raw, ok := spec.Payload[contract.PayloadKeyCompiledRenderPlanJSON].(string); !ok || strings.TrimSpace(raw) == "" {
		return fmt.Errorf("render_batch@1: %q is required", contract.PayloadKeyCompiledRenderPlanJSON)
	}
	if raw, ok := spec.Payload[contract.PayloadKeyCompiledRenderPlanSHA].(string); !ok || strings.TrimSpace(raw) == "" {
		return fmt.Errorf("render_batch@1: %q is required", contract.PayloadKeyCompiledRenderPlanSHA)
	}
	if err := contract.ValidateCompiledRenderPlanV2Payload(spec.Payload); err != nil {
		return fmt.Errorf("render_batch@1: invalid CompiledRenderPlanV2: %w", err)
	}
	return nil
}

// Execute renders the visual timeline without audio, then muxes the one
// already-finalized audio asset. It never calls AudioMix and never encodes
// the final audio stream: the mux command uses -c:v copy -c:a copy.
func (e *renderBatchExecutor) Execute(ctx context.Context, execCtx executor.ExecutionContext, spec executor.TaskSpec) (executor.ExecutionResult, error) {
	started := time.Now().UTC()
	obs := newRenderBatchObservability(execCtx, compiledPlanSHA(spec))
	obs.info("render_batch.started", map[string]interface{}{"job_id_present": spec.JobID != ""})

	validation := obs.begin("validation", "worker.plan", "validate")
	if err := e.Validate(spec); err != nil {
		obs.finish(validation, telemetry.StatusFailed, "validation_failed", err)
		return obs.failure(started, "validation_failed", err), nil
	}
	plan, err := decodeRenderPlanV2(spec)
	if err != nil {
		obs.finish(validation, telemetry.StatusFailed, "validation_failed", err)
		return obs.failure(started, "validation_failed", err), nil
	}
	obs.timelineSHA = plan.TimelineSHA256
	obs.finish(validation, telemetry.StatusOK, "", nil)

	jobID, err := safeOutputJobID(spec.JobID)
	if err != nil {
		obs.logFailure("job_id", "INVALID_JOB_ID", err)
		return obs.failure(started, "INVALID_JOB_ID", err), nil
	}
	assetResolution := obs.begin("asset_resolution", "worker.plan", "resolve_assets")
	bindings, ok := runtimeassets.FromContext(ctx)
	if !ok {
		err := ErrMissingRenderBatchBindings
		obs.finish(assetResolution, telemetry.StatusFailed, "ASSET_BINDINGS_MISSING", err)
		return obs.failure(started, "ASSET_BINDINGS_MISSING", err), nil
	}
	if err := validateBindings(plan, bindings); err != nil {
		obs.finish(assetResolution, telemetry.StatusFailed, "ASSET_BINDINGS_INVALID", err)
		return obs.failure(started, "ASSET_BINDINGS_INVALID", err), nil
	}
	if err := validateMediaFile(e.probe, ctx, bindings[plan.FinalAudio.AssetID].Path, "final audio", plan.DurationUS, false, true, &plan.FinalAudio); err != nil {
		obs.finish(assetResolution, telemetry.StatusFailed, "FINAL_AUDIO_INVALID", err)
		return obs.failure(started, "FINAL_AUDIO_INVALID", err), nil
	}
	obs.finish(assetResolution, telemetry.StatusOK, "", nil)

	if err := os.MkdirAll(e.outputRoot, 0o750); err != nil {
		obs.logFailure("output_directory", "output_directory", err)
		return obs.failure(started, "output_directory", err), nil
	}
	videoOnlyPath := filepath.Join(e.outputRoot, jobID+".video-only.mp4")
	finalPath := filepath.Join(e.outputRoot, jobID+".mp4")
	defer os.Remove(videoOnlyPath)

	visual := obs.begin("visual_render", "engine", "render")
	visualArgs, err := buildVideoOnlyArgs(plan, bindings, videoOnlyPath)
	if err != nil {
		obs.finish(visual, telemetry.StatusFailed, "visual_plan_invalid", err)
		return obs.failure(started, "visual_plan_invalid", err), nil
	}
	visualArtifact, visualProfile, err := e.runCommand(ctx, execCtx, ffmpegrunner.OperationCompose, visualArgs, videoOnlyPath, "video-only")
	if err != nil {
		obs.finish(visual, telemetry.StatusFailed, "visual_execute_failed", err)
		return obs.failure(started, "visual_execute_failed", err), nil
	}
	if visualArtifact.SizeBytes <= 0 {
		err := errors.New("video-only output is empty")
		obs.finish(visual, telemetry.StatusFailed, "visual_output_empty", err)
		return obs.failure(started, "visual_output_empty", err), nil
	}
	if err := validateMediaFile(e.probe, ctx, videoOnlyPath, "video-only output", plan.DurationUS, true, false, nil); err != nil {
		obs.finish(visual, telemetry.StatusFailed, "VISUAL_OUTPUT_INVALID", err)
		return obs.failure(started, "VISUAL_OUTPUT_INVALID", err), nil
	}
	obs.finish(visual, telemetry.StatusOK, "", nil)

	mux := obs.begin("final_mux", "engine.mux", "packet_write")
	muxArgs := buildFinalAudioCopyArgs(videoOnlyPath, bindings[plan.FinalAudio.AssetID].Path, finalPath)
	finalArtifact, muxProfile, err := e.runCommand(ctx, execCtx, ffmpegrunner.OperationEncode, muxArgs, finalPath, "final-mux")
	if err != nil {
		obs.finish(mux, telemetry.StatusFailed, "final_mux_failed", err)
		return obs.failure(started, "final_mux_failed", err), nil
	}
	if err := validateMediaFile(e.probe, ctx, finalPath, "final output", plan.DurationUS, true, true, &plan.FinalAudio); err != nil {
		obs.finish(mux, telemetry.StatusFailed, "FINAL_OUTPUT_INVALID", err)
		return obs.failure(started, "FINAL_OUTPUT_INVALID", err), nil
	}
	obs.finish(mux, telemetry.StatusOK, "", nil)

	metrics := obs.metrics
	metrics["compiled_asset_count"] = int64(len(plan.Assets))
	metrics["audio_mix_count"] = int64(0)
	metrics["audio_encode_count"] = int64(0)
	metrics["final_audio_copy"] = int64(1)
	metrics["timeline_revision"] = plan.TimelineRevision
	metrics["video_only_bytes"] = visualArtifact.SizeBytes
	metrics["final_output_bytes"] = finalArtifact.SizeBytes
	metrics["ffmpeg_visual_profile"] = visualProfile
	metrics["ffmpeg_mux_profile"] = muxProfile
	obs.info("render_batch.succeeded", map[string]interface{}{"compiled_asset_count": int64(len(plan.Assets)), "final_audio_copy": int64(1)})

	return executor.ExecutionResult{
		Status: "succeeded", Outputs: []executor.ArtifactRef{finalArtifact},
		Metrics: metrics, StartedAt: started, CompletedAt: time.Now().UTC(),
	}, nil
}

func decodeRenderPlanV2(spec executor.TaskSpec) (*contract.CompiledRenderPlanV2, error) {
	raw, ok := spec.Payload[contract.PayloadKeyCompiledRenderPlanJSON].(string)
	if !ok {
		return nil, errors.New("render_batch@1: compiled plan JSON must be a string")
	}
	plan, err := contract.DecodeCompiledRenderPlanV2([]byte(raw))
	if err != nil {
		return nil, fmt.Errorf("render_batch@1: decode V2 plan: %w", err)
	}
	return plan, nil
}

func validateBindings(plan *contract.CompiledRenderPlanV2, bindings runtimeassets.Bindings) error {
	if plan == nil || bindings == nil {
		return ErrMissingRenderBatchBindings
	}
	assetByID := make(map[string]contract.AssetRefV2, len(plan.Assets))
	for _, asset := range plan.Assets {
		assetByID[asset.AssetID] = asset
		if err := validateBinding(asset.AssetID, asset.SHA256, asset.SizeBytes, bindings); err != nil {
			return err
		}
	}
	for _, track := range plan.VideoTracks {
		for _, segment := range track.Segments {
			asset, ok := assetByID[segment.AssetID]
			if !ok {
				return fmt.Errorf("%w: segment asset_id=%q is not declared", ErrRenderBatchAssetIntegrity, segment.AssetID)
			}
			if err := validateBinding(segment.AssetID, asset.SHA256, asset.SizeBytes, bindings); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateBinding(assetID, wantSHA string, wantSize int64, bindings runtimeassets.Bindings) error {
	binding, ok := bindings[assetID]
	if !ok || strings.TrimSpace(binding.Path) == "" {
		return fmt.Errorf("%w: asset_id=%q", ErrMissingRenderBatchBindings, assetID)
	}
	if strings.TrimSpace(binding.SHA256) == "" || binding.SHA256 != wantSHA || wantSize <= 0 || binding.Size != wantSize {
		return fmt.Errorf("%w: asset_id=%q declared metadata does not match plan", ErrRenderBatchAssetIntegrity, assetID)
	}
	info, err := os.Stat(binding.Path)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 {
		if err == nil {
			err = errors.New("file is empty or not regular")
		}
		return fmt.Errorf("%w: asset_id=%q path: %v", ErrRenderBatchAssetIntegrity, assetID, err)
	}
	if info.Size() != wantSize {
		return fmt.Errorf("%w: asset_id=%q actual size=%d want=%d", ErrRenderBatchAssetIntegrity, assetID, info.Size(), wantSize)
	}
	file, err := os.Open(binding.Path)
	if err != nil {
		return fmt.Errorf("%w: asset_id=%q open: %v", ErrRenderBatchAssetIntegrity, assetID, err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return fmt.Errorf("%w: asset_id=%q hash: %v", ErrRenderBatchAssetIntegrity, assetID, err)
	}
	actualSHA := hex.EncodeToString(hash.Sum(nil))
	if actualSHA != wantSHA || actualSHA != binding.SHA256 {
		return fmt.Errorf("%w: asset_id=%q actual sha256=%s want=%s", ErrRenderBatchAssetIntegrity, assetID, actualSHA, wantSHA)
	}
	return nil
}

const renderBatchDurationToleranceSec = 0.050

func validateMediaFile(probe func(context.Context, string) (publisher.MediaProbe, error), ctx context.Context, path, label string, wantDurationUS int64, requireVideo, requireAudio bool, expectedAudio *contract.FinalAudioV2) error {
	if probe == nil {
		return errors.New("media probe is not configured")
	}
	media, err := probe(ctx, path)
	if err != nil {
		return fmt.Errorf("%s probe: %w", label, err)
	}
	if requireVideo && (!media.HasVideo || media.VideoTrackCount != 1) {
		return fmt.Errorf("%s must contain exactly one video stream", label)
	}
	if requireAudio && (!media.HasAudio || media.AudioTrackCount != 1) {
		return fmt.Errorf("%s must contain exactly one audio stream", label)
	}
	if expectedAudio != nil {
		if media.AudioCodec != expectedAudio.Codec || media.AudioSampleRateHz != expectedAudio.SampleRateHz || media.AudioChannels != expectedAudio.Channels {
			return fmt.Errorf("%s audio codec=%q sample_rate_hz=%d channels=%d want codec=%q sample_rate_hz=%d channels=%d", label, media.AudioCodec, media.AudioSampleRateHz, media.AudioChannels, expectedAudio.Codec, expectedAudio.SampleRateHz, expectedAudio.Channels)
		}
	}
	want := float64(wantDurationUS) / 1_000_000
	if media.DurationSec <= 0 || math.Abs(media.DurationSec-want) > renderBatchDurationToleranceSec {
		return fmt.Errorf("%s duration=%0.6fs want=%0.6fs tolerance=%0.3fs", label, media.DurationSec, want, renderBatchDurationToleranceSec)
	}
	return nil
}

func safeOutputJobID(jobID string) (string, error) {
	if strings.TrimSpace(jobID) == "" || jobID == "." || jobID == ".." || filepath.IsAbs(jobID) || strings.ContainsAny(jobID, "/\\\\\x00") || filepath.Base(jobID) != jobID {
		return "", errors.New("job_id must be a non-empty path-free identifier")
	}
	return jobID, nil
}

func buildVideoOnlyArgs(plan *contract.CompiledRenderPlanV2, bindings runtimeassets.Bindings, outputPath string) ([]string, error) {
	if plan == nil || len(plan.VideoTracks) == 0 {
		return nil, errors.New("render_batch@1: video_tracks must not be empty")
	}
	duration := float64(plan.DurationUS) / 1_000_000
	args := []string{
		"-f", "lavfi", "-i",
		fmt.Sprintf("color=c=black:s=%dx%d:r=%d/%d:d=%.6f", plan.Output.Width, plan.Output.Height, plan.Output.FPSNum, plan.Output.FPSDen, duration),
	}
	inputIndex := map[string]int{}
	for _, track := range plan.VideoTracks {
		for _, segment := range track.Segments {
			if _, exists := inputIndex[segment.AssetID]; exists {
				continue
			}
			inputIndex[segment.AssetID] = len(inputIndex) + 1
			args = append(args, "-i", bindings[segment.AssetID].Path)
		}
	}

	filters := make([]string, 0)
	base := "[0:v]"
	segmentIndex := 0
	for _, track := range plan.VideoTracks {
		for _, segment := range track.Segments {
			input := inputIndex[segment.AssetID]
			start := float64(segment.TimelineStartFrame*int64(plan.Output.FPSDen)) / float64(plan.Output.FPSNum)
			sourceIn := float64(segment.SourceInUS) / 1_000_000
			sourceDuration := float64(segment.SourceDurationUS) / 1_000_000
			frameDuration := float64(segment.FrameCount*int64(plan.Output.FPSDen)) / float64(plan.Output.FPSNum)
			if math.Abs(frameDuration-sourceDuration) > 1.0/float64(plan.Output.FPSNum) {
				return nil, fmt.Errorf("segment %q source_duration_us=%d does not match frame_count=%d at %d/%d fps", segment.SegmentID, segment.SourceDurationUS, segment.FrameCount, plan.Output.FPSNum, plan.Output.FPSDen)
			}
			segmentLabel := fmt.Sprintf("[batch_segment_%d]", segmentIndex)
			filters = append(filters, fmt.Sprintf("[%d:v]trim=start=%.6f:duration=%.6f,setpts=PTS-STARTPTS+%.6f/TB,scale=%d:%d:force_original_aspect_ratio=decrease,pad=%d:%d:(ow-iw)/2:(oh-ih)/2:color=black%s", input, sourceIn, sourceDuration, start, plan.Output.Width, plan.Output.Height, plan.Output.Width, plan.Output.Height, segmentLabel))
			overlayOutput := fmt.Sprintf("[batch_overlay_%d]", segmentIndex)
			filters = append(filters, fmt.Sprintf("%s%soverlay=eof_action=pass:shortest=0%s", base, segmentLabel, overlayOutput))
			base = overlayOutput
			segmentIndex++
		}
	}
	filters = append(filters, base+"format=yuv420p[vout]")

	pixelFormat := plan.Output.PixelFormat
	if pixelFormat == "" {
		pixelFormat = "yuv420p"
	}
	args = append(args,
		"-filter_complex", strings.Join(filters, ";"),
		"-map", "[vout]", "-an",
		"-c:v", plan.Output.VideoCodec,
		"-pix_fmt", pixelFormat,
		"-r", strconv.Itoa(plan.Output.FPSNum)+"/"+strconv.Itoa(plan.Output.FPSDen),
		"-t", fmt.Sprintf("%.6f", duration),
		"-y", outputPath,
	)
	return args, nil
}

func buildFinalAudioCopyArgs(videoOnlyPath, audioPath, outputPath string) []string {
	return []string{
		"-i", videoOnlyPath,
		"-i", audioPath,
		"-map", "0:v:0",
		"-map", "1:a:0",
		"-c:v", "copy",
		"-c:a", "copy",
		"-movflags", "+faststart",
		"-y", outputPath,
	}
}

func (e *renderBatchExecutor) runCommand(ctx context.Context, execCtx executor.ExecutionContext, operation ffmpegrunner.OperationType, args []string, outputPath, outputType string) (executor.ArtifactRef, map[string]interface{}, error) {
	profileResult, runErr := e.runner.Run(ctx, ffmpegrunner.FFmpegRequest{Operation: operation, Args: args})
	if sink, ok := execCtx.(interface {
		FFmpegProfiles() *ffmpegrunner.Aggregator
	}); ok && sink.FFmpegProfiles() != nil {
		sink.FFmpegProfiles().Add(profileResult)
	}
	profile := ffmpegProfileMetadata(profileResult)
	if runErr != nil {
		return executor.ArtifactRef{}, profile, fmt.Errorf("render_batch@1 %s: %w (exit_code=%d)", outputType, runErr, profileResult.ExitCode)
	}
	artifact, err := artifactFromFile("video/mp4", outputPath)
	if err != nil {
		return executor.ArtifactRef{}, profile, fmt.Errorf("render_batch@1 %s artifact: %w", outputType, err)
	}
	return artifact, profile, nil
}

type renderBatchPhase struct {
	stage   string
	started time.Time
	handle  *telemetry.EventHandle
}

type renderBatchObservability struct {
	logger      executor.Logger
	recorder    *telemetry.EventRecorder
	planSHA     string
	timelineSHA string
	metrics     map[string]interface{}
}

func newRenderBatchObservability(execCtx executor.ExecutionContext, planSHA string) *renderBatchObservability {
	obs := &renderBatchObservability{
		planSHA: planSHA,
		metrics: make(map[string]interface{}),
	}
	if execCtx != nil {
		obs.logger = execCtx.Logger()
		obs.recorder = recorderFromExecutionContext(execCtx)
	}
	return obs
}

func compiledPlanSHA(spec executor.TaskSpec) string {
	sha, _ := spec.Payload[contract.PayloadKeyCompiledRenderPlanSHA].(string)
	return strings.TrimSpace(sha)
}

func (o *renderBatchObservability) identityFields() map[string]interface{} {
	fields := make(map[string]interface{}, 2)
	if o.planSHA != "" {
		fields["plan_sha256"] = o.planSHA
	}
	if o.timelineSHA != "" {
		fields["timeline_sha256"] = o.timelineSHA
	}
	return fields
}

func (o *renderBatchObservability) info(event string, fields map[string]interface{}) {
	if o == nil || o.logger == nil {
		return
	}
	merged := o.identityFields()
	for key, value := range fields {
		merged[key] = value
	}
	o.logger.Info(event, merged)
}

func (o *renderBatchObservability) logFailure(stage, code string, err error) {
	if o == nil {
		return
	}
	fields := map[string]interface{}{"stage": stage, "error_code": code}
	// Do not pass err to the logger: asset errors can contain worker-local
	// paths. The stable code and identity fields are the structured contract.
	o.error("render_batch.failed", fields)
}

func (o *renderBatchObservability) error(event string, fields map[string]interface{}) {
	if o == nil || o.logger == nil {
		return
	}
	merged := o.identityFields()
	for key, value := range fields {
		merged[key] = value
	}
	o.logger.Error(event, nil, merged)
}

func (o *renderBatchObservability) begin(stage, component, action string) *renderBatchPhase {
	if o == nil {
		return nil
	}
	o.info("render_batch."+stage+".started", map[string]interface{}{"stage": stage})
	phase := &renderBatchPhase{stage: stage, started: time.Now()}
	if o.recorder == nil {
		return phase
	}
	spec, ok := telemetry.LookupCanonicalPhaseSpec(component, action)
	if !ok {
		o.info("render_batch.telemetry_unregistered", map[string]interface{}{"stage": stage})
		return phase
	}
	metadata := o.identityFields()
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return phase
	}
	phase.handle = o.recorder.Start(telemetry.EventSpec{
		Origin: spec.Origin, Scope: spec.Scope, Component: spec.Component,
		Action: spec.Action, Phase: spec.Phase, EventType: spec.EventType,
		SchemaVersion: telemetry.SchemaVersion, MetadataJSON: string(encoded),
	})
	return phase
}

func (o *renderBatchObservability) finish(phase *renderBatchPhase, status, code string, err error) {
	if o == nil || phase == nil {
		return
	}
	duration := time.Since(phase.started).Milliseconds()
	fields := o.identityFields()
	fields["stage"] = phase.stage
	fields["duration_ms"] = duration
	fields["status"] = status
	if code != "" {
		fields["error_code"] = code
	}
	if status == telemetry.StatusFailed {
		o.error("render_batch."+phase.stage+".failed", fields)
	} else {
		o.info("render_batch."+phase.stage+".completed", fields)
	}
	switch phase.stage {
	case "validation":
		o.metrics["render_plan_validate_ms"] = duration
	case "asset_resolution":
		o.metrics["compiled_asset_resolve_ms"] = duration
	case "visual_render":
		o.metrics["visual_execute_ms"] = duration
	case "final_mux":
		o.metrics["final_mux_ms"] = duration
	}
	if phase.handle == nil {
		return
	}
	metadata := o.identityFields()
	for key, value := range fields {
		metadata[key] = value
	}
	encoded, marshalErr := json.Marshal(metadata)
	if marshalErr == nil {
		phase.handle.SetMetadataJSON(string(encoded))
	}
	if status == telemetry.StatusFailed {
		phase.handle.Abort(code, code)
	} else {
		phase.handle.CompleteWith(0, 0, 0, telemetry.StatusOK, "", "")
	}
}

func (o *renderBatchObservability) failure(started time.Time, code string, err error) executor.ExecutionResult {
	if o == nil {
		return executor.ExecutionResult{Status: "failed", ErrorCode: code, ErrorDetail: err.Error(), StartedAt: started, CompletedAt: time.Now().UTC()}
	}
	o.logFailure("execution", code, err)
	return executor.ExecutionResult{
		Status: "failed", ErrorCode: code, ErrorDetail: err.Error(), Metrics: o.metrics,
		StartedAt: started, CompletedAt: time.Now().UTC(),
	}
}

// RegisterRenderBatchExecutor adds exactly one render_batch@1 entry to the
// canonical registry. Existing V1 registrations are neither replaced nor
// modified.
func RegisterRenderBatchExecutor(reg *executor.Registry, runner ffmpegrunner.FFmpegRunner, outputRoot string) error {
	if reg == nil {
		return errors.New("render_batch@1: registry is nil")
	}
	return reg.Register(NewRenderBatch(runner, outputRoot))
}
