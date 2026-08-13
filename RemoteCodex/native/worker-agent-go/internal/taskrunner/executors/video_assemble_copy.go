package executors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"velox-shared/contract"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/runtimeassets"
	"velox-worker-agent/pkg/video/pipeline"
)

const (
	VideoAssembleCopyID      = "video.assemble.copy.v1"
	VideoAssembleCopyVersion = 1
)

var (
	ErrCopyOnlyProfileMismatch = errors.New("video.assemble.copy.v1: canonical profile mismatch")
	ErrCopyOnlyCertification   = errors.New("video.assemble.copy.v1: prepared asset certification failed")
)

type videoAssembleCopyExecutor struct {
	runner     *pipeline.Runner
	outputRoot string
}

// NewVideoAssembleCopy creates the strict receiver-side assembler. The
// pipeline runner is only used to reach the native RenderCompiledPlanV2
// method; no FFmpeg runner or fallback is accepted by this executor.
func NewVideoAssembleCopy(runner *pipeline.Runner, outputRoot string) executor.Executor {
	if strings.TrimSpace(outputRoot) == "" {
		outputRoot = filepath.Join(os.TempDir(), "velox", "video-assemble-copy")
	}
	return &videoAssembleCopyExecutor{runner: runner, outputRoot: outputRoot}
}

func (e *videoAssembleCopyExecutor) Descriptor() executor.Descriptor {
	return executor.Descriptor{
		ID: VideoAssembleCopyID, Version: VideoAssembleCopyVersion,
		InputTypes: []string{"render.compiled.v2"}, OutputTypes: []string{"video/mp4"},
		ResourceClass: executor.ResourceIO, Deterministic: true, Cacheable: true,
		TemporalMode: executor.TemporalGlobal,
	}
}

func (e *videoAssembleCopyExecutor) Validate(spec executor.TaskSpec) error {
	if spec.ExecutorID != VideoAssembleCopyID {
		return fmt.Errorf("video.assemble.copy.v1: executor_id must be %q, got %q", VideoAssembleCopyID, spec.ExecutorID)
	}
	if e == nil || e.runner == nil {
		return fmt.Errorf("video.assemble.copy.v1: native V2 renderer is not configured")
	}
	if spec.Payload == nil {
		return errors.New("video.assemble.copy.v1: payload is required")
	}
	if err := contract.ValidateCompiledRenderPlanV2Payload(spec.Payload); err != nil {
		return fmt.Errorf("video.assemble.copy.v1: invalid CompiledRenderPlanV2: %w", err)
	}
	plan, err := decodeRenderPlanV2(spec)
	if err != nil {
		return err
	}
	if err := validateCopyOnlyPlan(plan); err != nil {
		return err
	}
	return nil
}

func (e *videoAssembleCopyExecutor) Execute(ctx context.Context, _ executor.ExecutionContext, spec executor.TaskSpec) (executor.ExecutionResult, error) {
	started := time.Now().UTC()
	fail := func(code string, err error) (executor.ExecutionResult, error) {
		return executor.ExecutionResult{Status: "failed", ErrorCode: code, ErrorDetail: err.Error(), StartedAt: started, CompletedAt: time.Now().UTC()}, nil
	}
	if err := e.Validate(spec); err != nil {
		return fail("COPY_ONLY_PLAN_INVALID", err)
	}
	plan, err := decodeRenderPlanV2(spec)
	if err != nil {
		return fail("COPY_ONLY_PLAN_INVALID", err)
	}
	bindings, ok := runtimeassets.FromContext(ctx)
	if !ok {
		return fail("PREPARED_ASSET_BINDINGS_MISSING", errors.New("runtime asset bindings are required"))
	}
	if err := validateBindings(plan, bindings); err != nil {
		return fail("PREPARED_ASSET_INTEGRITY_FAILED", err)
	}
	if err := validatePreparedAssetBindings(plan, bindings); err != nil {
		return fail("PREPARED_ASSET_CERTIFICATION_FAILED", err)
	}

	jobID, err := safeOutputJobID(spec.JobID)
	if err != nil {
		return fail("INVALID_JOB_ID", err)
	}
	if err := os.MkdirAll(e.outputRoot, 0o750); err != nil {
		return fail("OUTPUT_DIRECTORY", err)
	}
	outputPath := filepath.Join(e.outputRoot, jobID+".mp4")
	wire, err := marshalCopyOnlyWire(plan, spec.JobID, outputPath, bindings)
	if err != nil {
		return fail("COPY_ONLY_PLAN_INVALID", err)
	}
	metrics, err := e.runner.RenderCompiledPlanV2(ctx, wire, outputPath)
	if err != nil {
		return fail("PACKET_COPY_FAILED", err)
	}
	artifact, err := artifactFromFile("video/mp4", outputPath)
	if err != nil {
		return fail("FINAL_OUTPUT_INVALID", err)
	}
	return executor.ExecutionResult{
		Status: "succeeded", Outputs: []executor.ArtifactRef{artifact},
		Metrics: map[string]interface{}{
			"concat_mode": "packet_copy", "frames_decoded": int64(0),
			"frames_encoded": int64(0), "frames_composited": int64(0),
			"ffmpeg_exec": int64(0), "ffprobe_exec": int64(0),
			"final_audio_copy": int64(1), "packet_copy": int64(1),
			"native_render_ms": metrics.TotalMs,
		},
		StartedAt: started, CompletedAt: time.Now().UTC(),
	}, nil
}

type copyOnlyPlanWire struct {
	*contract.CompiledRenderPlanV2
	JobID      string            `json:"job_id"`
	OutputPath string            `json:"output_path"`
	Bindings   map[string]string `json:"bindings"`
}

func marshalCopyOnlyWire(plan *contract.CompiledRenderPlanV2, jobID, outputPath string, bindings runtimeassets.Bindings) ([]byte, error) {
	if plan == nil || strings.TrimSpace(jobID) == "" || strings.TrimSpace(outputPath) == "" {
		return nil, errors.New("video.assemble.copy.v1: incomplete native wire document")
	}
	paths := make(map[string]string, len(bindings))
	for assetID, binding := range bindings {
		paths[assetID] = binding.Path
	}
	return json.Marshal(copyOnlyPlanWire{CompiledRenderPlanV2: plan, JobID: jobID, OutputPath: outputPath, Bindings: paths})
}

func validateCopyOnlyPlan(plan *contract.CompiledRenderPlanV2) error {
	if plan == nil {
		return errors.New("video.assemble.copy.v1: plan is nil")
	}
	if plan.Output.ProfileID == "" {
		return fmt.Errorf("%w: output.profile_id is required", ErrCopyOnlyProfileMismatch)
	}
	profile, err := contract.KnownCanonicalVideoProfileV1(plan.Output.ProfileID)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrCopyOnlyProfileMismatch, err)
	}
	if err := profile.MatchesOutput(plan.Output); err != nil {
		return fmt.Errorf("%w: %v", ErrCopyOnlyProfileMismatch, err)
	}
	if plan.Output.CodecProfile != profile.CodecProfile || plan.Output.CodecLevel != profile.CodecLevel || plan.Output.GOPSize != profile.GOPSize || plan.Output.BFrames != profile.BFrames || plan.Output.TimeBaseNum != profile.TimeBaseNum || plan.Output.TimeBaseDen != profile.TimeBaseDen || !plan.Output.ClosedGOP {
		return fmt.Errorf("%w: output encoder/time-base fields do not match profile %q", ErrCopyOnlyProfileMismatch, profile.ProfileID)
	}
	if len(plan.VideoTracks) != 1 || len(plan.VideoTracks[0].Segments) == 0 {
		return errors.New("video.assemble.copy.v1: exactly one non-empty video track is required")
	}
	assets := make(map[string]contract.AssetRefV2, len(plan.Assets))
	for _, asset := range plan.Assets {
		assets[asset.AssetID] = asset
	}
	for _, segment := range plan.VideoTracks[0].Segments {
		asset, ok := assets[segment.AssetID]
		if !ok || (asset.Kind != "video" && asset.Kind != "prepared_video_fragment") {
			return fmt.Errorf("%w: segment %q does not reference a prepared video asset", ErrCopyOnlyCertification, segment.SegmentID)
		}
		if asset.SHA256 != segment.SHA256 || asset.ProfileID != profile.ProfileID || asset.FrameCount != segment.FrameCount || asset.TimelineRevision != plan.TimelineRevision || asset.TimelineSHA256 != plan.TimelineSHA256 || asset.TimelineStartFrame != segment.TimelineStartFrame || asset.DurationUS != segment.SourceDurationUS || !asset.FirstFrameKeyframe || !asset.ClosedGOP || segment.SourceInUS != 0 {
			return fmt.Errorf("%w: segment %q manifest binding is not exact", ErrCopyOnlyCertification, segment.SegmentID)
		}
	}
	return nil
}

func validatePreparedAssetBindings(plan *contract.CompiledRenderPlanV2, bindings runtimeassets.Bindings) error {
	if err := validateCopyOnlyPlan(plan); err != nil {
		return err
	}
	for _, segment := range plan.VideoTracks[0].Segments {
		binding := bindings[segment.AssetID]
		if strings.TrimSpace(binding.Path) == "" {
			return fmt.Errorf("%w: missing binding for %q", ErrCopyOnlyCertification, segment.AssetID)
		}
	}
	return nil
}
