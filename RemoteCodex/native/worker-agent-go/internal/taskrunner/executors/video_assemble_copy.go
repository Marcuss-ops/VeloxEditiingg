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
	"velox-worker-agent/internal/chunkfactory"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/runtimeassets"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/storage"
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
	chunkStore *chunkfactory.Store
}

// NewVideoAssembleCopy creates the strict receiver-side assembler. The
// pipeline runner is only used to reach the native RenderCompiledPlanV2
// method; no FFmpeg runner or fallback is accepted by this executor.
func NewVideoAssembleCopy(runner *pipeline.Runner, outputRoot string) executor.Executor {
	return NewVideoAssembleCopyWithChunkStore(runner, outputRoot, nil)
}

// NewVideoAssembleCopyWithChunkStore enables the W5 first consumer. A nil
// store preserves the legacy monolithic path and means the capability is
// DISABLED, never a hidden noop.
func NewVideoAssembleCopyWithChunkStore(runner *pipeline.Runner, outputRoot string, store *chunkfactory.Store) executor.Executor {
	if strings.TrimSpace(outputRoot) == "" {
		outputRoot = filepath.Join(os.TempDir(), "velox", "video-assemble-copy")
	}
	return &videoAssembleCopyExecutor{runner: runner, outputRoot: outputRoot, chunkStore: store}
}

// AttachChunkStore is called only by the composition root after a READY store
// has been constructed. It keeps the default registry wiring unchanged while
// making the W5 consumer opt-in and fail-closed.
func (e *videoAssembleCopyExecutor) AttachChunkStore(store *chunkfactory.Store) {
	if e != nil {
		e.chunkStore = store
	}
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
	if _, err := contract.DecodeCompiledRenderPlanV2Payload(spec.Payload); err != nil {
		return fmt.Errorf("video.assemble.copy.v1: invalid CompiledRenderPlanV2: %w", err)
	}
	plan, err := decodeCompiledPlanV2(spec)
	if err != nil {
		return err
	}
	if err := validateCopyOnlyPlan(plan); err != nil {
		return err
	}
	return nil
}

func (e *videoAssembleCopyExecutor) Execute(ctx context.Context, execCtx executor.ExecutionContext, spec executor.TaskSpec) (executor.ExecutionResult, error) {
	started := time.Now().UTC()
	fail := func(code string, err error) (executor.ExecutionResult, error) {
		return executor.ExecutionResult{Status: "failed", ErrorCode: code, ErrorDetail: err.Error(), StartedAt: started, CompletedAt: time.Now().UTC()}, nil
	}
	if err := e.Validate(spec); err != nil {
		return fail("COPY_ONLY_PLAN_INVALID", err)
	}
	plan, err := decodeCompiledPlanV2(spec)
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
	// The packet-copy output is the final upload artifact too. Route it through
	// the same canonical staging resolver as encode/scene.composite
	// so this executor cannot silently bypass tmpfs reservations.
	outputPath := filepath.Join(e.outputRoot, jobID+".mp4")
	if resolver := storageResolverFromExecutionContext(execCtx); resolver != nil {
		if placement, placeErr := resolver.Place(storage.ArtifactStaging, jobID+".mp4", estimateOutputBytesFromDuration(plan.DurationUS)); placeErr == nil {
			outputPath = placement.Path
		}
	}
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o750); err != nil {
		return fail("OUTPUT_DIRECTORY", err)
	}
	wire, err := marshalCopyOnlyWire(plan, spec.JobID, outputPath, bindings)
	if err != nil {
		return fail("COPY_ONLY_PLAN_INVALID", err)
	}
	metrics, err := e.runner.RenderCompiledPlanV2(ctx, wire, outputPath)
	if err != nil {
		return fail(copyOnlyRenderErrorCode(err), err)
	}
	profile, err := validateCopyOnlyProfile(plan)
	if err != nil {
		return fail("COPY_ONLY_PLAN_INVALID", err)
	}
	// The profile is the producer's explicit request, but the bytes are the
	// final authority. Never publish a progressive-looking job when the native
	// mux emitted a classic MP4 or performed a backward seek.
	if profile.ContainerLayout == contract.ContainerLayoutFragmented {
		if err := validateFragmentedMP4Output(outputPath, metrics); err != nil {
			return fail("FMP4_OUTPUT_INVALID", err)
		}
	}
	artifact, err := artifactFromFile("video/mp4", outputPath)
	if err != nil {
		return fail("FINAL_OUTPUT_INVALID", err)
	}
	if e.chunkStore != nil {
		if err := e.emitChunkManifest(plan, profile, bindings, spec.JobID); err != nil {
			return fail("CHUNK_MANIFEST_FAILED", err)
		}
	}
	// The packet-copy path is the only executor that previously dropped the
	// engine ledgers: its cost IS engine work (packet mux, input opens, seeks),
	// so without these rows the Master read model had no engine phase to
	// project and every engine_* column stayed 0 for a job whose render was
	// pure packet copy. Attach both streams exactly as scene.composite.v1 does.
	segments := projectSegments(metrics)
	detailedPhases := projectDetailedPhases(metrics)
	return executor.ExecutionResult{
		Status: "succeeded", Outputs: []executor.ArtifactRef{artifact},
		RawMetrics:     copyOnlyRawMetrics(plan, metrics, artifact),
		Segments:       segments,
		DetailedPhases: detailedPhases,
		StartedAt:      started, CompletedAt: time.Now().UTC(),
	}, nil
}

func (e *videoAssembleCopyExecutor) emitChunkManifest(plan *contract.CompiledRenderPlanV2, profile contract.CanonicalVideoProfileV1, bindings runtimeassets.Bindings, jobID string) error {
	if e == nil || e.chunkStore == nil || plan == nil || len(plan.VideoTracks) != 1 {
		return fmt.Errorf("chunk manifest: incomplete input")
	}
	manifest := chunkfactory.Manifest{
		Version: chunkfactory.ManifestVersion, JobID: jobID, ProfileID: profile.StreamProfile(),
		DurationUS: plan.DurationUS, TimelineSHA: plan.TimelineSHA256,
		Chunks: make([]chunkfactory.ManifestChunk, 0, len(plan.VideoTracks[0].Segments)),
	}
	assets := make(map[string]contract.AssetRefV2, len(plan.Assets))
	for _, asset := range plan.Assets {
		assets[asset.AssetID] = asset
	}
	for index, segment := range plan.VideoTracks[0].Segments {
		asset := assets[segment.AssetID]
		binding := bindings[segment.AssetID]
		actual, err := artifactFromFile("video/mp4", binding.Path)
		if err != nil {
			return fmt.Errorf("chunk manifest: inspect %s: %w", segment.AssetID, err)
		}
		if actual.Hash != asset.SHA256 || actual.SizeBytes != asset.SizeBytes {
			return fmt.Errorf("chunk manifest: source identity changed for %s", segment.AssetID)
		}
		assetKey := asset.AssetKey
		if strings.TrimSpace(assetKey) == "" {
			assetKey = "sha256:" + actual.Hash
		}
		chunks, err := chunkfactory.PlanChunks(assetKey, profile.StreamProfile(), 0, segment.SourceDurationUS, segment.SourceDurationUS, chunkfactory.KeyframeIndex{0})
		if err != nil || len(chunks) != 1 {
			if err == nil {
				err = fmt.Errorf("planned %d chunks, want 1", len(chunks))
			}
			return fmt.Errorf("chunk manifest: plan %s: %w", segment.SegmentID, err)
		}
		planned := chunks[0]
		descriptor := chunkfactory.ChunkDescriptor{
			ChunkID: planned.ChunkID, AssetKey: planned.AssetKey, ProfileID: planned.ProfileID,
			ChunkIndex: planned.ChunkIndex, SourceInUS: planned.SourceInUS, SourceOutUS: planned.SourceOutUS,
			ChunkDurationUS: planned.SourceOutUS - planned.SourceInUS,
			PayloadSHA256:   actual.Hash, SizeBytes: actual.SizeBytes,
		}
		if err := e.chunkStore.PutChunk(descriptor, binding.Path); err != nil {
			return fmt.Errorf("chunk manifest: store %s: %w", segment.SegmentID, err)
		}
		manifest.Chunks = append(manifest.Chunks, chunkfactory.ManifestChunk{
			ChunkID: planned.ChunkID, AssetKey: planned.AssetKey, ProfileID: planned.ProfileID,
			ChunkIndex: index, SourceInUS: planned.SourceInUS, SourceOutUS: planned.SourceOutUS,
			PayloadSHA256: actual.Hash, SizeBytes: actual.SizeBytes,
			PayloadPath: e.chunkStore.PayloadPath(planned.ChunkID),
		})
	}
	if err := e.chunkStore.PutManifest(manifest); err != nil {
		return err
	}
	return nil
}

// copyOnlyRawMetrics is the canonical worker projection for the native V2
// packet-copy path. Keep native facts intact here so the TaskResult exposes
// the actual work done by the remote worker: no encode, packet-copy ratio,
// engine I/O, output identity, and mux/finalization timings.
func copyOnlyRawMetrics(plan *contract.CompiledRenderPlanV2, native pipeline.RenderMetrics, artifact executor.ArtifactRef) *telemetry.RawExecutionMetrics {
	segmentsTotal := native.SegmentsTotal
	segmentsPacketCopy := native.SegmentsPacketCopy
	segmentsReencoded := native.SegmentsReencoded
	if segmentsTotal == 0 && plan != nil && len(plan.VideoTracks) == 1 {
		segmentsTotal = int64(len(plan.VideoTracks[0].Segments))
		segmentsPacketCopy = segmentsTotal
	}
	packetCopyRatio := native.PacketCopyRatio
	if packetCopyRatio == 0 && segmentsTotal > 0 {
		packetCopyRatio = float64(segmentsPacketCopy) / float64(segmentsTotal) * 100
	}
	concatMode := native.ConcatMode
	if concatMode == "" {
		concatMode = "packet_copy"
	}
	audioCopied := int64(0)
	var audioInputBytes int64
	if plan != nil && plan.FinalAudio.AssetID != "" {
		audioCopied = 1
		for _, asset := range plan.Assets {
			if asset.AssetID == plan.FinalAudio.AssetID {
				audioInputBytes = asset.SizeBytes
				break
			}
		}
	}
	return &telemetry.RawExecutionMetrics{
		InputBytes:                native.TotalBytesRead,
		OutputBytes:               artifact.SizeBytes,
		CpuTimeMs:                 native.CPUUserMs + native.CPUSystemMs,
		CpuUserMs:                 native.CPUUserMs,
		CpuSystemMs:               native.CPUSystemMs,
		FramesDecoded:             native.FramesDecoded,
		FramesEncoded:             native.Frames,
		FramesComposited:          native.FramesComposited,
		FfmpegSpeedRatio:          native.SpeedX,
		EncodePasses:              int32(native.EncodePasses),
		FinalConcatStreamCopy:     true,
		ConcatMode:                concatMode,
		TempBytesWritten:          native.TempBytes,
		MediaDurationSeconds:      native.DurationSec,
		WallClockSeconds:          float64(native.TotalMs) / 1000,
		OutputFileSize:            artifact.SizeBytes,
		OutputSha256:              artifact.Hash,
		DiskReadBytes:             native.StorageBytesRead,
		DiskWriteBytes:            native.StorageBytesWritten,
		OutputWriteMs:             native.FirstOutputWriteMS,
		OutputFinalizeMs:          native.TrailerToPublishUS / 1000,
		VideoConcatMs:             native.TotalMs,
		SegmentsTotal:             int32(segmentsTotal),
		SegmentsPacketCopy:        int32(segmentsPacketCopy),
		SegmentsReencoded:         int32(segmentsReencoded),
		PacketCopyRatio:           packetCopyRatio,
		AudioPacketCopy:           audioCopied,
		AudioInputBytes:           audioInputBytes,
		FfmpegExecCount:           native.FfmpegExecCount,
		FfprobeExecCount:          native.FfprobeExecCount,
		ProcessSpawnCount:         native.EngineSpawnCount,
		ProcessStartupMs:          native.EngineSpawnMs,
		ExternalProcessSpawnExact: native.EngineExternalSpawnCount,
		JobRenderWallMs:           native.TotalMs,
	}
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

// copyOnlyRenderErrorCode classifies a native copy-only render failure into
// a stable worker-facing error code. The native engine already fails closed
// for every packet-mux rejection (it never re-encodes), but it reports them
// all under a generic packet_mux_failed code; the worker re-classifies the
// keyframe-safety rejection so operators can distinguish "the cut is not
// packet-copy safe" from an unrelated mux failure without losing the exact
// engine reason.
func copyOnlyRenderErrorCode(err error) string {
	if err == nil {
		return "PACKET_COPY_FAILED"
	}
	lower := strings.ToLower(err.Error())
	if strings.Contains(lower, "must start on an exact video keyframe") ||
		strings.Contains(lower, "not keyframe-safe for packet copy") {
		return "COPY_ONLY_NOT_KEYFRAME_SAFE"
	}
	return "PACKET_COPY_FAILED"
}

func validateCopyOnlyPlan(plan *contract.CompiledRenderPlanV2) error {
	if plan == nil {
		return errors.New("video.assemble.copy.v1: plan is nil")
	}
	profile, err := validateCopyOnlyProfile(plan)
	if err != nil {
		return err
	}
	if len(plan.VideoTracks) != 1 || len(plan.VideoTracks[0].Segments) == 0 {
		return errors.New("video.assemble.copy.v1: exactly one non-empty video track is required")
	}
	assets := make(map[string]contract.AssetRefV2, len(plan.Assets))
	for _, asset := range plan.Assets {
		assets[asset.AssetID] = asset
	}
	for _, segment := range plan.VideoTracks[0].Segments {
		if err := validateCopyOnlySegment(plan, profile, assets, segment); err != nil {
			return err
		}
	}
	return nil
}

// validateCopyOnlyProfile resolves the output's canonical profile and checks
// every stream-identity field against it. The packet-copy path can only accept
// streams produced under an admitted profile.
func validateCopyOnlyProfile(plan *contract.CompiledRenderPlanV2) (contract.CanonicalVideoProfileV1, error) {
	if plan.Output.ProfileID == "" {
		return contract.CanonicalVideoProfileV1{}, fmt.Errorf("%w: output.profile_id is required", ErrCopyOnlyProfileMismatch)
	}
	profile, err := contract.KnownCanonicalVideoProfileV1(plan.Output.ProfileID)
	if err != nil {
		return contract.CanonicalVideoProfileV1{}, fmt.Errorf("%w: %v", ErrCopyOnlyProfileMismatch, err)
	}
	if err := profile.MatchesOutput(plan.Output); err != nil {
		return contract.CanonicalVideoProfileV1{}, fmt.Errorf("%w: %v", ErrCopyOnlyProfileMismatch, err)
	}
	if plan.Output.CodecProfile != profile.CodecProfile || plan.Output.CodecLevel != profile.CodecLevel || plan.Output.GOPSize != profile.GOPSize || plan.Output.BFrames != profile.BFrames || plan.Output.TimeBaseNum != profile.TimeBaseNum || plan.Output.TimeBaseDen != profile.TimeBaseDen || !plan.Output.ClosedGOP {
		return contract.CanonicalVideoProfileV1{}, fmt.Errorf("%w: output encoder/time-base fields do not match profile %q", ErrCopyOnlyProfileMismatch, profile.ProfileID)
	}
	return profile, nil
}

// validateCopyOnlySegment certifies one prepared video fragment against its
// declared asset manifest: the reference must exist, be a prepared-video kind,
// and match the plan's timeline identity and frame bounds exactly.
func validateCopyOnlySegment(plan *contract.CompiledRenderPlanV2, profile contract.CanonicalVideoProfileV1, assets map[string]contract.AssetRefV2, segment contract.VideoSegmentV2) error {
	asset, ok := assets[segment.AssetID]
	if !ok || (asset.Kind != "video" && asset.Kind != "prepared_video_fragment") {
		return fmt.Errorf("%w: segment %q does not reference a prepared video asset", ErrCopyOnlyCertification, segment.SegmentID)
	}
	if asset.SHA256 != segment.SHA256 || asset.ProfileID != profile.StreamProfile() || asset.FrameCount != segment.FrameCount || asset.TimelineRevision != plan.TimelineRevision || asset.TimelineSHA256 != plan.TimelineSHA256 || asset.TimelineStartFrame != segment.TimelineStartFrame || asset.DurationUS != segment.SourceDurationUS || !asset.FirstFrameKeyframe || !asset.ClosedGOP || segment.SourceInUS != 0 {
		return fmt.Errorf("%w: segment %q manifest binding is not exact", ErrCopyOnlyCertification, segment.SegmentID)
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
