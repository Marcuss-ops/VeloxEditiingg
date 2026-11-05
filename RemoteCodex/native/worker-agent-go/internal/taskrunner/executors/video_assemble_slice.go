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
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"velox-shared/contract"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/publisher"
	"velox-worker-agent/internal/runtimeassets"
	"velox-worker-agent/pkg/storage"
	"velox-worker-agent/pkg/video/ffmpegrunner"
)

const (
	VideoAssembleSliceID      = "video.assemble.slice.v1"
	VideoAssembleSliceVersion = 1
)

// videoAssembleSliceExecutor copies compatible source packets from verified
// runtime assets in the worker cache into the final mux. It never encodes.
type videoAssembleSliceExecutor struct {
	runner     ffmpegrunner.FFmpegRunner
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

func NewVideoAssembleSlice(outputRoot string, runner ffmpegrunner.FFmpegRunner) executor.Executor {
	if strings.TrimSpace(outputRoot) == "" {
		outputRoot = filepath.Join(os.TempDir(), "velox", "video-assemble-slice")
	}
	if runner == nil {
		runner = ffmpegrunner.NewProcessRunner()
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
	if e == nil || e.runner == nil {
		return errors.New("video.assemble.slice.v1: FFmpeg runner is not configured")
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
	if err := e.verifyCopyInputs(ctx, plan, bindings); err != nil {
		return fail("DRIVE_SLICE_COPY_INCOMPATIBLE", err)
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
	// The concat manifest is the only temporary file. Video packets are copied
	// directly from the verified cache; this executor never invokes an encoder.
	workRoot := filepath.Dir(outputPath)
	if resolver := storageResolverFromExecutionContext(execCtx); resolver != nil {
		workRoot = resolver.Config().TempDir
	}
	if strings.TrimSpace(workRoot) == "" {
		return fail("DRIVE_SLICE_STORAGE_INVALID", errors.New("disk-backed slice work directory is not configured"))
	}
	tempDir, err := os.MkdirTemp(workRoot, "drive-slice-copy-"+jobID+"-")
	if err != nil {
		return fail("DRIVE_SLICE_STORAGE_INVALID", err)
	}
	defer os.RemoveAll(tempDir)
	listPath := filepath.Join(tempDir, "segments.ffconcat")
	if err := writeSliceConcatList(listPath, plan, bindings); err != nil {
		return fail("DRIVE_SLICE_STORAGE_INVALID", err)
	}
	args, err := sliceConcatMuxArgs(listPath, audio.Path, outputPath, plan)
	if err != nil {
		return fail("DRIVE_SLICE_ASSETS_INVALID", err)
	}
	metrics, renderErr := e.runner.Run(ctx, ffmpegrunner.FFmpegRequest{Operation: ffmpegrunner.OperationEncode, Args: args})
	if renderErr != nil {
		return fail("DRIVE_SLICE_COPY_FAILED", fmt.Errorf("stream-copy concat/mux: %w", renderErr))
	}
	if err := e.verifyFinalMedia(ctx, outputPath, plan); err != nil {
		return fail("DRIVE_SLICE_OUTPUT_INVALID", err)
	}
	artifact, err := artifactFromFile("video/mp4", outputPath)
	if err != nil {
		return fail("FINAL_OUTPUT_INVALID", err)
	}
	raw := rawMetricsFromFFmpegResult(metrics)
	raw.OutputBytes = artifact.SizeBytes
	raw.OutputFileSize = artifact.SizeBytes
	raw.OutputSha256 = artifact.Hash
	raw.EncodePasses = 0
	raw.ConcatMode = "stream_copy"
	raw.FinalConcatStreamCopy = true
	raw.MediaDurationSeconds = float64(plan.DurationUS) / 1_000_000
	raw.WallClockSeconds = float64(metrics.ProcessWallMS) / 1000
	return executor.ExecutionResult{
		Status: "succeeded", Outputs: []executor.ArtifactRef{artifact}, RawMetrics: raw,
		StartedAt: started, CompletedAt: time.Now().UTC(),
	}, nil
}

type slicePoolMember struct {
	AssetID         string `json:"asset_id"`
	DriveFileID     string `json:"drive_file_id"`
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

func (e *videoAssembleSliceExecutor) compileStockPoolPlan(ctx context.Context, spec executor.TaskSpec, plan *contract.CompiledRenderPlanV2, bindings runtimeassets.Bindings) (*contract.CompiledRenderPlanV2, error) {
	if plan.StockPool == nil || plan.StockPool.Selection != "shuffle" {
		return nil, errors.New("unsupported stock_pool selection")
	}
	members := sliceRuntimePoolMembers(spec.Payload["runtime_assets"])
	ordered := make([]slicePoolMember, 0, len(members))
	for _, member := range members {
		if member.StockPoolMember {
			ordered = append(ordered, member)
		}
	}
	if len(ordered) == 0 {
		return nil, errors.New("stock_pool has no resolved video files")
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].DriveFileID < ordered[j].DriveFileID })
	probes := make(map[string]publisher.MediaProbe, len(ordered))
	var baseline *publisher.MediaProbe
	// Validate the complete folder snapshot before selecting any clips. A
	// mixed/incompatible pool fails the job without starting ffmpeg.
	for _, member := range ordered {
		assetID := "stockpool:" + member.DriveFileID
		binding, ok := bindings[assetID]
		if !ok || !binding.Verified || binding.Size <= 0 {
			return nil, fmt.Errorf("stock source %q has no verified worker cache binding", member.DriveFileID)
		}
		probe, err := e.prober.ProbeMediaDetails(ctx, binding.Path)
		if err != nil {
			return nil, fmt.Errorf("probe stock source %q: %w", member.DriveFileID, err)
		}
		if !probe.HasVideo || probe.VideoCodec != "h264" || probe.Width != plan.Output.Width || probe.Height != plan.Output.Height || probe.VideoFPSNum != plan.Output.FPSNum || probe.VideoFPSDen != plan.Output.FPSDen || probe.VideoPixelFormat != plan.Output.PixelFormat || probe.VideoFrameCount <= 0 || probe.VideoExtradataHash == "" {
			return nil, fmt.Errorf("stock source %q is incompatible with packet-copy output; refusing to transcode", member.DriveFileID)
		}
		if err := verifyFirstPacketKeyframe(ctx, binding.Path); err != nil {
			return nil, fmt.Errorf("stock source %q cannot be concatenated safely: %w", member.DriveFileID, err)
		}
		if baseline == nil {
			cp := probe
			baseline = &cp
		} else if !sameSliceCopySignature(*baseline, probe) {
			return nil, fmt.Errorf("stock source %q codec parameters differ; refusing to transcode", member.DriveFileID)
		}
		probes[member.DriveFileID] = probe
	}
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
	assetByID := make(map[string]contract.AssetRefV2, len(ordered))
	for _, member := range ordered {
		assetID := "stockpool:" + member.DriveFileID
		binding := bindings[assetID]
		probe := probes[member.DriveFileID]
		assetByID[assetID] = contract.AssetRefV2{AssetID: assetID, AssetKey: member.DriveFileID, SourceKind: "drive", DriveFileID: member.DriveFileID, SHA256: binding.SHA256, SizeBytes: binding.Size, Kind: "video", ProfileID: plan.Output.ProfileID, FrameCount: probe.VideoFrameCount, FirstFrameKeyframe: true, ClosedGOP: true, DurationUS: int64(probe.DurationSec * 1_000_000), Width: probe.Width, Height: probe.Height}
		compiled.Assets = append(compiled.Assets, assetByID[assetID])
	}
	poolCursor := 0
	for trackIndex := range compiled.VideoTracks {
		segments := make([]contract.VideoSegmentV2, 0, len(compiled.VideoTracks[trackIndex].Segments))
		for _, segment := range compiled.VideoTracks[trackIndex].Segments {
			if segment.StockPoolSlot {
				remaining, timelineFrame := segment.FrameCount, segment.TimelineStartFrame
				part := 0
				for remaining > 0 {
					member := ordered[poolCursor%len(ordered)]
					poolCursor++
					part++
					assetID := "stockpool:" + member.DriveFileID
					asset := assetByID[assetID]
					count := probes[member.DriveFileID].VideoFrameCount
					if count > remaining {
						count = remaining
					}
					segments = append(segments, contract.VideoSegmentV2{SegmentID: fmt.Sprintf("%s-source-%03d", segment.SegmentID, part), AssetID: assetID, SHA256: asset.SHA256, TimelineStartFrame: timelineFrame, FrameCount: count, SourceDurationUS: count * int64(plan.Output.FPSDen) * 1_000_000 / int64(plan.Output.FPSNum)})
					remaining -= count
					timelineFrame += count
				}
				continue
			}
			segments = append(segments, segment)
		}
		compiled.VideoTracks[trackIndex].Segments = segments
	}
	return &compiled, nil
}

func verifyFirstPacketKeyframe(ctx context.Context, path string) error {
	cmd := exec.CommandContext(ctx, "ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "packet=flags", "-read_intervals", "%+#1", "-of", "csv=p=0", path)
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("ffprobe first packet: %w", err)
	}
	if !strings.Contains(string(out), "K") {
		return errors.New("first video packet is not a keyframe")
	}
	return nil
}

func slicePlanDurationFrameCount(plan *contract.CompiledRenderPlanV2) int64 {
	n := plan.DurationUS * int64(plan.Output.FPSNum)
	d := int64(plan.Output.FPSDen) * 1_000_000
	return (n + d - 1) / d
}

func (e *videoAssembleSliceExecutor) verifyCopyInputs(ctx context.Context, plan *contract.CompiledRenderPlanV2, bindings runtimeassets.Bindings) error {
	assets := make(map[string]contract.AssetRefV2, len(plan.Assets))
	for _, asset := range plan.Assets {
		assets[asset.AssetID] = asset
	}
	seen := make(map[string]struct{})
	probes := make(map[string]publisher.MediaProbe)
	var baseline *publisher.MediaProbe
	for _, segment := range plan.VideoTracks[0].Segments {
		asset := assets[segment.AssetID]
		if _, done := seen[asset.AssetID]; done {
			continue
		}
		seen[asset.AssetID] = struct{}{}
		binding := bindings[asset.AssetID]
		if !binding.Verified || binding.SHA256 != asset.SHA256 || binding.Size != asset.SizeBytes || strings.TrimSpace(binding.Path) == "" {
			return fmt.Errorf("source %q is not integrity-verified against the plan", asset.AssetID)
		}
		info, statErr := os.Stat(binding.Path)
		if statErr != nil || !info.Mode().IsRegular() || info.Size() != binding.Size {
			return fmt.Errorf("source %q local file identity is invalid", asset.AssetID)
		}
		probe, err := e.prober.ProbeMediaDetails(ctx, binding.Path)
		if err != nil {
			return fmt.Errorf("probe source %q: %w", asset.AssetID, err)
		}
		if !probe.HasVideo || probe.DurationSec <= 0 || asset.ProfileID != plan.Output.ProfileID || !asset.FirstFrameKeyframe || !asset.ClosedGOP {
			return fmt.Errorf("source %q lacks certified keyframe/closed-GOP assembly metadata", asset.AssetID)
		}
		if probe.VideoFrameCount <= 0 || probe.VideoFrameCount != asset.FrameCount {
			return fmt.Errorf("source %q has %d frames but its plan certifies %d; refusing to transcode", asset.AssetID, probe.VideoFrameCount, asset.FrameCount)
		}
		wantLevel, _ := strconv.Atoi(strings.ReplaceAll(plan.Output.CodecLevel, ".", ""))
		if probe.VideoCodec != "h264" || !strings.EqualFold(probe.VideoProfile, plan.Output.CodecProfile) || probe.VideoLevel != wantLevel || probe.VideoBFrames != plan.Output.BFrames || probe.VideoExtradataHash == "" || probe.VideoTimeBaseNum <= 0 || probe.VideoTimeBaseDen <= 0 || probe.Width != plan.Output.Width || probe.Height != plan.Output.Height || probe.VideoFPSNum != plan.Output.FPSNum || probe.VideoFPSDen != plan.Output.FPSDen || probe.VideoPixelFormat != plan.Output.PixelFormat {
			return fmt.Errorf("source %q media profile differs from the output; refusing to transcode", asset.AssetID)
		}
		if baseline == nil {
			copy := probe
			baseline = &copy
		} else if !sameSliceCopySignature(*baseline, probe) {
			return fmt.Errorf("source %q codec parameters/time base differ from the other inputs; refusing to transcode", asset.AssetID)
		}
		probes[asset.AssetID] = probe
	}
	for _, segment := range plan.VideoTracks[0].Segments {
		probe := probes[segment.AssetID]
		actualUS := int64(probe.DurationSec * 1_000_000)
		windowEnd := segment.SourceInUS + segment.SourceDurationUS
		frameUS := int64(1_000_000 * plan.Output.FPSDen / plan.Output.FPSNum)
		if windowEnd > actualUS+frameUS {
			return fmt.Errorf("source %q window ends at %d us beyond probed duration %d us", segment.AssetID, windowEnd, actualUS)
		}
	}
	return nil
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

func sameSliceCopySignature(a, b publisher.MediaProbe) bool {
	return a.VideoCodec == b.VideoCodec && a.VideoCodecTag == b.VideoCodecTag &&
		a.VideoProfile == b.VideoProfile && a.VideoLevel == b.VideoLevel &&
		a.VideoPixelFormat == b.VideoPixelFormat && a.VideoFPSNum == b.VideoFPSNum &&
		a.VideoFPSDen == b.VideoFPSDen && a.VideoTimeBaseNum == b.VideoTimeBaseNum &&
		a.VideoTimeBaseDen == b.VideoTimeBaseDen && a.VideoBFrames == b.VideoBFrames &&
		a.VideoExtradataHash == b.VideoExtradataHash
}

func writeSliceConcatList(path string, plan *contract.CompiledRenderPlanV2, bindings runtimeassets.Bindings) error {
	var b strings.Builder
	b.WriteString("ffconcat version 1.0\n")
	for _, segment := range plan.VideoTracks[0].Segments {
		binding, ok := bindings[segment.AssetID]
		if !ok || !binding.Verified || strings.TrimSpace(binding.Path) == "" {
			return fmt.Errorf("source %q is not ready for stream-copy concat", segment.AssetID)
		}
		abs, err := filepath.Abs(binding.Path)
		if err != nil {
			return err
		}
		// The concat demuxer uses shell-like single-quoted paths; embedded
		// quotes are represented by closing, escaping, and reopening quotes.
		quoted := strings.ReplaceAll(abs, "'", "'\\''")
		frameDurationUS := segment.FrameCount * int64(plan.Output.FPSDen) * 1_000_000 / int64(plan.Output.FPSNum)
		fmt.Fprintf(&b, "file '%s'\ninpoint 0\noutpoint %s\n", quoted, seconds(frameDurationUS))
	}
	return os.WriteFile(path, []byte(b.String()), 0o600)
}

func sliceConcatMuxArgs(listPath, audioPath, output string, plan *contract.CompiledRenderPlanV2) ([]string, error) {
	if strings.TrimSpace(audioPath) == "" {
		return nil, errors.New("final audio path is empty")
	}
	return []string{"-nostdin", "-hide_banner", "-loglevel", "error", "-y",
		"-f", "concat", "-safe", "0", "-i", listPath,
		"-threads", "1", "-i", audioPath,
		"-map", "0:v:0", "-map", "1:a:0", "-c:v", "copy", "-c:a", "copy",
		"-frames:v", strconv.FormatInt(slicePlanFrameCount(plan), 10),
		"-t", seconds(plan.DurationUS), "-video_track_timescale", strconv.Itoa(plan.Output.TimeBaseDen),
		"-movflags", "+faststart", output}, nil
}

func slicePlanFrameCount(plan *contract.CompiledRenderPlanV2) int64 {
	var frames int64
	for _, segment := range plan.VideoTracks[0].Segments {
		frames += segment.FrameCount
	}
	return frames
}

func seconds(microseconds int64) string {
	return strconv.FormatFloat(float64(microseconds)/1_000_000, 'f', 6, 64)
}
