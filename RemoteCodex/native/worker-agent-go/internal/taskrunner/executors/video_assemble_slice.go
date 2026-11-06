package executors

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"velox-shared/contract"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/publisher"
	"velox-worker-agent/internal/runtimeassets"
	"velox-worker-agent/pkg/storage"
	"velox-worker-agent/pkg/video/pipeline"
)

const (
	VideoAssembleSliceID      = "video.assemble.slice.v1"
	VideoAssembleSliceVersion = 1
)

// videoAssembleSliceExecutor copies compatible source packets from verified
// runtime assets in the worker cache into the final mux. It never encodes.
type videoAssembleSliceExecutor struct {
	runner     *pipeline.Runner
	prober     sliceMediaProber
	outputRoot string
}

type sliceMediaProber interface {
	ProbeMediaDetails(context.Context, string) (publisher.MediaProbe, error)
}

type ffprobeSliceMediaProber struct{}

func (ffprobeSliceMediaProber) ProbeMediaDetails(ctx context.Context, path string) (publisher.MediaProbe, error) {
	return publisher.ProbeMediaDetails(ctx, path)
}

func NewVideoAssembleSlice(outputRoot string, runner *pipeline.Runner) executor.Executor {
	if strings.TrimSpace(outputRoot) == "" {
		outputRoot = filepath.Join(os.TempDir(), "velox", "video-assemble-slice")
	}
	return &videoAssembleSliceExecutor{runner: runner, prober: ffprobeSliceMediaProber{}, outputRoot: outputRoot}
}

func (e *videoAssembleSliceExecutor) Descriptor() executor.Descriptor {
	return executor.Descriptor{
		ID: VideoAssembleSliceID, Version: VideoAssembleSliceVersion,
		InputTypes: []string{"render.compiled.v2.drive"}, OutputTypes: []string{"video/mp4"},
		ResourceClass: executor.ResourceCPU, Deterministic: true, Cacheable: true,
		TemporalMode: executor.TemporalGlobal,
	}
}

func (e *videoAssembleSliceExecutor) Validate(spec executor.TaskSpec) error {
	if spec.ExecutorID != VideoAssembleSliceID {
		return fmt.Errorf("video.assemble.slice.v1: executor_id must be %q", VideoAssembleSliceID)
	}
	if e == nil || e.runner == nil || e.runner.RenderClient() == nil {
		return errors.New("video.assemble.slice.v1: native packet-copy renderer is not configured")
	}
	if e.prober == nil {
		return errors.New("video.assemble.slice.v1: FFprobe media validation is not configured")
	}
	plan, err := decodeCompiledPlanV2(spec)
	if err != nil {
		return fmt.Errorf("video.assemble.slice.v1: invalid CompiledRenderPlanV2: %w", err)
	}
	if _, err := contract.KnownCanonicalVideoProfileV1(plan.Output.ProfileID); err != nil {
		return fmt.Errorf("video.assemble.slice.v1: canonical output profile required: %w", err)
	}
	assets := make(map[string]contract.AssetRefV2, len(plan.Assets))
	runtimeDriveIDs := sliceRuntimeDriveFileIDs(spec.Payload["runtime_assets"])
	runtimeStockIDs := sliceRuntimeStockAssetIDs(spec.Payload["runtime_assets"])
	hasDrive := plan.StockPool != nil
	for _, asset := range plan.Assets {
		assets[asset.AssetID] = asset
		driveSource := asset.Kind == "video" && isSliceDriveSource(asset, runtimeDriveIDs)
		hasDrive = hasDrive || driveSource
	}
	if !hasDrive {
		return errors.New("video.assemble.slice.v1: plan must contain at least one Drive video source")
	}
	if len(plan.VideoTracks) != 1 || (len(plan.VideoTracks[0].Segments) == 0 && plan.StockPool == nil) {
		return errors.New("video.assemble.slice.v1: exactly one video track is required; segments or stock_pool must be supplied")
	}
	if plan.StockPool != nil {
		members := 0
		for _, item := range sliceRuntimePoolMembers(spec.Payload["runtime_assets"]) {
			if item.StockPoolMember && strings.TrimSpace(item.DriveFileID) != "" {
				members++
			}
		}
		if members == 0 {
			return errors.New("video.assemble.slice.v1: Master did not resolve any stock_pool files")
		}
	}
	var nextFrame int64
	stockPoolSlots := 0
	for _, segment := range plan.VideoTracks[0].Segments {
		if segment.StockPoolSlot {
			if plan.StockPool == nil {
				return fmt.Errorf("video.assemble.slice.v1: segment %q has stock_pool_slot without stock_pool", segment.SegmentID)
			}
			stockPoolSlots++
			if segment.FrameCount <= 0 {
				return fmt.Errorf("video.assemble.slice.v1: stock pool slot %q has invalid frame count", segment.SegmentID)
			}
		} else {
			asset, ok := assets[segment.AssetID]
			if !ok || (asset.Kind != "video" && asset.Kind != "prepared_video_fragment") {
				return fmt.Errorf("video.assemble.slice.v1: segment %q must reference a video asset", segment.SegmentID)
			}
			if asset.ProfileID != plan.Output.ProfileID || asset.FrameCount <= 0 || !asset.FirstFrameKeyframe || !asset.ClosedGOP {
				return fmt.Errorf("video.assemble.slice.v1: asset %q is not certified for packet-copy assembly", segment.AssetID)
			}
			if segment.SourceInUS != 0 {
				if _, isStock := runtimeStockIDs[segment.AssetID]; isStock {
					return fmt.Errorf("video.assemble.slice.v1: stock segment %q starts at %d us inside its Drive clip; worker accepts whole-clip stock_pool slots only and will not probe, cut, or re-encode stock", segment.SegmentID, segment.SourceInUS)
				}
				return fmt.Errorf("video.assemble.slice.v1: segment %q starts at a non-keyframe offset (%d us); packet-copy is unsafe", segment.SegmentID, segment.SourceInUS)
			}
			if segment.FrameCount <= 0 || segment.FrameCount > asset.FrameCount {
				return fmt.Errorf("video.assemble.slice.v1: segment %q frame count %d exceeds certified source frames %d", segment.SegmentID, segment.FrameCount, asset.FrameCount)
			}
		}
		if segment.TimelineStartFrame != nextFrame {
			return fmt.Errorf("video.assemble.slice.v1: segment %q leaves a timeline gap or overlap at frame %d", segment.SegmentID, nextFrame)
		}
		nextFrame += segment.FrameCount
	}
	if plan.StockPool != nil && stockPoolSlots == 0 {
		return errors.New("video.assemble.slice.v1: stock_pool requires at least one stock slot")
	}
	framesNum := int64(plan.DurationUS) * int64(plan.Output.FPSNum)
	framesDen := int64(plan.Output.FPSDen) * 1_000_000
	wantFrames := (framesNum + framesDen - 1) / framesDen
	if nextFrame != wantFrames {
		return fmt.Errorf("video.assemble.slice.v1: timeline has %d frames, plan duration requires %d", nextFrame, wantFrames)
	}
	return nil
}

func sliceRuntimeStockAssetIDs(raw interface{}) map[string]struct{} {
	data, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var assets []struct {
		AssetID  string `json:"asset_id"`
		AssetKey string `json:"asset_key"`
		Role     string `json:"role"`
	}
	if json.Unmarshal(data, &assets) != nil {
		return nil
	}
	ids := make(map[string]struct{})
	for _, asset := range assets {
		if !strings.EqualFold(strings.TrimSpace(asset.Role), "stock") {
			continue
		}
		if id := strings.TrimSpace(asset.AssetID); id != "" {
			ids[id] = struct{}{}
		}
		if id := strings.TrimSpace(asset.AssetKey); id != "" {
			ids[id] = struct{}{}
		}
	}
	return ids
}

func (e *videoAssembleSliceExecutor) Execute(ctx context.Context, execCtx executor.ExecutionContext, spec executor.TaskSpec) (executor.ExecutionResult, error) {
	started := time.Now().UTC()
	fail := func(code string, err error) (executor.ExecutionResult, error) {
		return executor.ExecutionResult{Status: "failed", ErrorCode: code, ErrorDetail: err.Error(), StartedAt: started, CompletedAt: time.Now().UTC()}, nil
	}
	if err := e.Validate(spec); err != nil {
		return fail("DRIVE_SLICE_PLAN_INVALID", err)
	}
	plan, _ := decodeCompiledPlanV2(spec)
	bindings, ok := runtimeassets.FromContext(ctx)
	if !ok {
		return fail("DRIVE_SLICE_ASSETS_MISSING", errors.New("verified runtime asset bindings are required"))
	}
	if plan.StockPool != nil {
		compiled, err := e.compileStockPoolPlan(ctx, spec, plan, bindings)
		if err != nil {
			return fail("DRIVE_STOCK_POOL_INCOMPATIBLE", err)
		}
		plan = compiled
	}
	if err := validateCopyOnlyPlan(plan); err != nil {
		return fail("DRIVE_SLICE_PLAN_INVALID", err)
	}
	audio, ok := bindings[plan.FinalAudio.AssetID]
	if !ok || !audio.Verified || audio.Size <= 0 || audio.SHA256 == "" || audio.SHA256 != plan.FinalAudio.SHA256 || audio.Size != plan.FinalAudio.SizeBytes {
		return fail("DRIVE_SLICE_ASSETS_INVALID", errors.New("final audio integrity binding does not match the plan"))
	}
	if info, statErr := os.Stat(audio.Path); statErr != nil || !info.Mode().IsRegular() || info.Size() != audio.Size {
		return fail("DRIVE_SLICE_ASSETS_INVALID", errors.New("final audio local file identity is invalid"))
	}
	jobID, err := safeOutputJobID(spec.JobID)
	if err != nil {
		return fail("INVALID_JOB_ID", err)
	}
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
		return fail("DRIVE_SLICE_PLAN_INVALID", err)
	}
	metrics, renderErr := e.runner.RenderCompiledPlanV2(ctx, wire, outputPath)
	if renderErr != nil {
		return fail(copyOnlyRenderErrorCode(renderErr), renderErr)
	}
	if err := e.verifyFinalMedia(ctx, outputPath, plan); err != nil {
		return fail("DRIVE_SLICE_OUTPUT_INVALID", err)
	}
	artifact, err := artifactFromFile("video/mp4", outputPath)
	if err != nil {
		return fail("FINAL_OUTPUT_INVALID", err)
	}
	raw := copyOnlyRawMetrics(plan, metrics, artifact)
	return executor.ExecutionResult{
		Status: "succeeded", Outputs: []executor.ArtifactRef{artifact}, RawMetrics: raw,
		Segments: projectSegments(metrics), DetailedPhases: projectDetailedPhases(metrics),
		StartedAt: started, CompletedAt: time.Now().UTC(),
	}, nil
}

type slicePoolMember struct {
	AssetID         string `json:"asset_id"`
	DriveFileID     string `json:"drive_file_id"`
	SizeBytes       int64  `json:"size_bytes"`
	DurationMS      int64  `json:"duration_ms"`
	StockPoolMember bool   `json:"stock_pool_member"`
}

func sliceRuntimePoolMembers(raw interface{}) []slicePoolMember {
	data, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var members []slicePoolMember
	_ = json.Unmarshal(data, &members)
	return members
}

func (e *videoAssembleSliceExecutor) compileStockPoolPlan(_ context.Context, spec executor.TaskSpec, plan *contract.CompiledRenderPlanV2, bindings runtimeassets.Bindings) (*contract.CompiledRenderPlanV2, error) {
	if plan.StockPool == nil || plan.StockPool.Selection != "shuffle" {
		return nil, errors.New("unsupported stock_pool selection")
	}
	members := sliceRuntimePoolMembers(spec.Payload["runtime_assets"])
	ordered := make([]slicePoolMember, 0, len(members))
	for _, member := range members {
		binding, selected := bindings["stockpool:"+member.DriveFileID]
		if member.StockPoolMember && selected && binding.Verified && binding.Size > 0 && member.DurationMS > 0 {
			ordered = append(ordered, member)
		}
	}
	if len(ordered) == 0 {
		return nil, errors.New("stock_pool has no selected sources with Drive duration metadata")
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].DriveFileID < ordered[j].DriveFileID })
	seedBytes := sha256.Sum256([]byte(spec.JobID + ":" + plan.TimelineSHA256 + ":" + plan.StockPool.DriveFolderID))
	rng := rand.New(rand.NewSource(int64(binary.LittleEndian.Uint64(seedBytes[:8]))))
	rng.Shuffle(len(ordered), func(i, j int) { ordered[i], ordered[j] = ordered[j], ordered[i] })
	compiled := *plan
	compiled.StockPool = nil
	compiled.Assets = append([]contract.AssetRefV2(nil), plan.Assets...)
	compiled.VideoTracks = append([]contract.VideoTrackV2(nil), plan.VideoTracks...)
	for i := range compiled.VideoTracks {
		compiled.VideoTracks[i].Segments = append([]contract.VideoSegmentV2(nil), plan.VideoTracks[i].Segments...)
	}
	poolCursor := 0
	framesLeft := make(map[string]int64, len(ordered))
	for _, member := range ordered {
		assetID := "stockpool:" + member.DriveFileID
		framesLeft[assetID] = member.DurationMS * int64(plan.Output.FPSNum) / (1000 * int64(plan.Output.FPSDen))
	}
	for trackIndex := range compiled.VideoTracks {
		segments := make([]contract.VideoSegmentV2, 0, len(compiled.VideoTracks[trackIndex].Segments))
		for _, segment := range compiled.VideoTracks[trackIndex].Segments {
			if !segment.StockPoolSlot {
				segments = append(segments, segment)
				continue
			}
			remaining, timelineFrame := segment.FrameCount, segment.TimelineStartFrame
			part := 0
			for remaining > 0 {
				for poolCursor < len(ordered) && framesLeft["stockpool:"+ordered[poolCursor].DriveFileID] <= 0 {
					poolCursor++
				}
				if poolCursor >= len(ordered) {
					return nil, errors.New("selected stock pool duration is shorter than its timeline slots")
				}
				member := ordered[poolCursor]
				part++
				baseAssetID := "stockpool:" + member.DriveFileID
				binding := bindings[baseAssetID]
				count := framesLeft[baseAssetID]
				if count > remaining {
					count = remaining
				}
				assetID := fmt.Sprintf("%s:timeline:%d", baseAssetID, timelineFrame)
				durationUS := count * int64(plan.Output.FPSDen) * 1_000_000 / int64(plan.Output.FPSNum)
				asset := contract.AssetRefV2{AssetID: assetID, AssetKey: member.DriveFileID, SourceKind: "drive", DriveFileID: member.DriveFileID, SHA256: binding.SHA256, SizeBytes: binding.Size, Kind: "video", MIME: "video/mp4", ProfileID: plan.Output.ProfileID, FrameCount: count, TimelineRevision: plan.TimelineRevision, TimelineSHA256: plan.TimelineSHA256, TimelineStartFrame: timelineFrame, FirstFrameKeyframe: true, ClosedGOP: true, DurationUS: durationUS, Width: plan.Output.Width, Height: plan.Output.Height}
				compiled.Assets = append(compiled.Assets, asset)
				bindings[assetID] = binding
				segments = append(segments, contract.VideoSegmentV2{SegmentID: fmt.Sprintf("%s-source-%03d", segment.SegmentID, part), AssetID: assetID, SHA256: asset.SHA256, TimelineStartFrame: timelineFrame, FrameCount: count, SourceDurationUS: durationUS})
				framesLeft[baseAssetID] -= count
				remaining -= count
				timelineFrame += count
				if framesLeft[baseAssetID] == 0 {
					poolCursor++
				}
			}
		}
		compiled.VideoTracks[trackIndex].Segments = segments
	}
	return &compiled, nil
}

func (e *videoAssembleSliceExecutor) verifyFinalMedia(ctx context.Context, outputPath string, plan *contract.CompiledRenderPlanV2) error {
	probe, err := e.prober.ProbeMediaDetails(ctx, outputPath)
	if err != nil {
		return fmt.Errorf("probe assembled output: %w", err)
	}
	if !probe.HasVideo || !probe.HasAudio || probe.VideoCodec != "h264" || probe.AudioCodec != "aac" || probe.Width != plan.Output.Width || probe.Height != plan.Output.Height {
		return fmt.Errorf("assembled output streams/profile invalid: video=%t/%s audio=%t/%s size=%dx%d want=h264/aac %dx%d", probe.HasVideo, probe.VideoCodec, probe.HasAudio, probe.AudioCodec, probe.Width, probe.Height, plan.Output.Width, plan.Output.Height)
	}
	if probe.VideoFrameCount != slicePlanFrameCount(plan) {
		return fmt.Errorf("assembled output has %d frames, plan requires %d; refusing to publish", probe.VideoFrameCount, slicePlanFrameCount(plan))
	}
	wantDuration := float64(plan.DurationUS) / 1_000_000
	frameTolerance := float64(plan.Output.FPSDen)/float64(plan.Output.FPSNum) + 0.1
	if probe.DurationSec <= 0 || probe.DurationSec-wantDuration > frameTolerance || wantDuration-probe.DurationSec > frameTolerance {
		return fmt.Errorf("assembled output duration %.3fs does not match plan %.3fs (tolerance %.3fs)", probe.DurationSec, wantDuration, frameTolerance)
	}
	return nil
}

func slicePlanFrameCount(plan *contract.CompiledRenderPlanV2) int64 {
	var frames int64
	for _, segment := range plan.VideoTracks[0].Segments {
		frames += segment.FrameCount
	}
	return frames
}
