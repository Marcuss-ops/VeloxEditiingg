package executors

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"velox-shared/contract"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/runtimeassets"
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
	}
}

func (e *renderBatchExecutor) Descriptor() executor.Descriptor { return e.descriptor }

// Validate admits only a complete, strict, canonical V2 envelope. Legacy
// render_plan/render_plan_json payloads remain owned by the V1 executors.
func (e *renderBatchExecutor) Validate(spec executor.TaskSpec) error {
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
	validationStarted := time.Now()
	if err := e.Validate(spec); err != nil {
		return batchFailure(started, "validation_failed", err), nil
	}

	plan, err := decodeRenderPlanV2(spec)
	if err != nil {
		return batchFailure(started, "validation_failed", err), nil
	}
	bindings, ok := runtimeassets.FromContext(ctx)
	if !ok {
		return batchFailure(started, "ASSET_BINDINGS_MISSING", ErrMissingRenderBatchBindings), nil
	}
	if err := validateBindings(plan, bindings); err != nil {
		return batchFailure(started, "ASSET_BINDINGS_INVALID", err), nil
	}
	validationMS := time.Since(validationStarted).Milliseconds()

	if err := os.MkdirAll(e.outputRoot, 0o750); err != nil {
		return batchFailure(started, "output_directory", err), nil
	}
	videoOnlyPath := filepath.Join(e.outputRoot, spec.JobID+".video-only.mp4")
	finalPath := filepath.Join(e.outputRoot, spec.JobID+".mp4")
	defer os.Remove(videoOnlyPath)

	visualStarted := time.Now()
	visualArgs, err := buildVideoOnlyArgs(plan, bindings, videoOnlyPath)
	if err != nil {
		return batchFailure(started, "visual_plan_invalid", err), nil
	}
	visualArtifact, visualProfile, err := e.runCommand(ctx, execCtx, ffmpegrunner.OperationCompose, visualArgs, videoOnlyPath, "video-only")
	if err != nil {
		return batchFailure(started, "visual_execute_failed", err), nil
	}
	if visualArtifact.SizeBytes <= 0 {
		return batchFailure(started, "visual_output_empty", errors.New("video-only output is empty")), nil
	}
	visualMS := time.Since(visualStarted).Milliseconds()

	muxStarted := time.Now()
	muxArgs := buildFinalAudioCopyArgs(videoOnlyPath, bindings[plan.FinalAudio.AssetID].Path, finalPath)
	finalArtifact, muxProfile, err := e.runCommand(ctx, execCtx, ffmpegrunner.OperationEncode, muxArgs, finalPath, "final-mux")
	if err != nil {
		return batchFailure(started, "final_mux_failed", err), nil
	}
	muxMS := time.Since(muxStarted).Milliseconds()

	metrics := map[string]interface{}{
		"render_plan_validate_ms": validationMS,
		"visual_execute_ms":       visualMS,
		"final_mux_ms":            muxMS,
		"compiled_asset_count":    int64(len(plan.Assets)),
		"audio_mix_count":         int64(0),
		"audio_encode_count":      int64(0),
		"final_audio_copy":        int64(1),
		"timeline_revision":       plan.TimelineRevision,
		"timeline_sha256":         plan.TimelineSHA256,
		"final_audio_asset_id":    plan.FinalAudio.AssetID,
		"video_only_bytes":        visualArtifact.SizeBytes,
		"final_output_bytes":      finalArtifact.SizeBytes,
		"ffmpeg_visual_profile":   visualProfile,
		"ffmpeg_mux_profile":      muxProfile,
	}

	return executor.ExecutionResult{
		Status:    "succeeded",
		Outputs:   []executor.ArtifactRef{finalArtifact},
		Metrics:   metrics,
		StartedAt: started, CompletedAt: time.Now().UTC(),
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
	for _, asset := range plan.Assets {
		if err := validateBinding(asset.AssetID, asset.SHA256, asset.SizeBytes, bindings); err != nil {
			return err
		}
	}
	if err := validateBinding(plan.FinalAudio.AssetID, plan.FinalAudio.SHA256, plan.FinalAudio.SizeBytes, bindings); err != nil {
		return err
	}
	for _, track := range plan.VideoTracks {
		for _, segment := range track.Segments {
			if err := validateBinding(segment.AssetID, segment.SHA256, 0, bindings); err != nil {
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
	if binding.SHA256 != "" && binding.SHA256 != wantSHA {
		return fmt.Errorf("%w: asset_id=%q", ErrRenderBatchAssetIntegrity, assetID)
	}
	if binding.Size > 0 && wantSize > 0 && binding.Size != wantSize {
		return fmt.Errorf("%w: asset_id=%q size=%d want=%d", ErrRenderBatchAssetIntegrity, assetID, binding.Size, wantSize)
	}
	if info, err := os.Stat(binding.Path); err != nil || !info.Mode().IsRegular() || info.Size() <= 0 {
		if err == nil {
			err = errors.New("file is empty or not regular")
		}
		return fmt.Errorf("%w: asset_id=%q path: %v", ErrMissingRenderBatchBindings, assetID, err)
	}
	return nil
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

func batchFailure(started time.Time, code string, err error) executor.ExecutionResult {
	return executor.ExecutionResult{
		Status: "failed", ErrorCode: code, ErrorDetail: err.Error(),
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
