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
	"velox-worker-agent/internal/publisher"
	"velox-worker-agent/internal/runtimeassets"
	"velox-worker-agent/pkg/storage"
	"velox-worker-agent/pkg/video/ffmpegrunner"
)

const (
	VideoAssembleSliceID      = "video.assemble.slice.v1"
	VideoAssembleSliceVersion = 1
	maxSliceBatchSources      = 8
)

// videoAssembleSliceExecutor renders source windows on the selected worker.
// It downloads/certifies full provider sources via the common asset resolver,
// then makes one FFmpeg pass directly from those sources to the final mux.
// No Producer-side fragments or worker-side intermediate video files exist.
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
	hasDrive := false
	for _, asset := range plan.Assets {
		assets[asset.AssetID] = asset
		driveSource := asset.Kind == "video" && isSliceDriveSource(asset, runtimeDriveIDs)
		hasDrive = hasDrive || driveSource
	}
	if !hasDrive {
		return errors.New("video.assemble.slice.v1: plan must contain at least one Drive video source")
	}
	if len(plan.VideoTracks) != 1 || len(plan.VideoTracks[0].Segments) == 0 {
		return errors.New("video.assemble.slice.v1: exactly one non-empty video track is required")
	}
	var nextFrame int64
	for _, segment := range plan.VideoTracks[0].Segments {
		asset, ok := assets[segment.AssetID]
		if !ok || (asset.Kind != "video" && asset.Kind != "prepared_video_fragment") {
			return fmt.Errorf("video.assemble.slice.v1: segment %q must reference a video asset", segment.SegmentID)
		}
		if segment.TimelineStartFrame != nextFrame {
			return fmt.Errorf("video.assemble.slice.v1: segment %q leaves a timeline gap or overlap at frame %d", segment.SegmentID, nextFrame)
		}
		nextFrame += segment.FrameCount
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
	profile, err := contract.KnownCanonicalVideoProfileV1(plan.Output.ProfileID)
	if err != nil {
		return fail("DRIVE_SLICE_PLAN_INVALID", err)
	}
	if err := e.verifyDriveSources(ctx, plan, bindings, spec.Payload["runtime_assets"]); err != nil {
		return fail("DRIVE_SOURCE_VALIDATION_FAILED", err)
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
	// Keep intermediate batch videos on the worker's disk-backed ATTEMPT_TEMP
	// volume. Putting hundreds of decoders and all intermediate output in the
	// 4 GiB container memory limit caused the previous single-pass render to OOM.
	workRoot := filepath.Dir(outputPath)
	if resolver := storageResolverFromExecutionContext(execCtx); resolver != nil {
		workRoot = resolver.Config().TempDir
	}
	if strings.TrimSpace(workRoot) == "" {
		return fail("DRIVE_SLICE_STORAGE_INVALID", errors.New("disk-backed slice work directory is not configured"))
	}
	tempDir, err := os.MkdirTemp(workRoot, "drive-slice-"+jobID+"-")
	if err != nil {
		return fail("DRIVE_SLICE_STORAGE_INVALID", err)
	}
	defer os.RemoveAll(tempDir)
	segments := plan.VideoTracks[0].Segments
	batches := groupSliceSegments(segments, maxSliceBatchSources)
	var processWall time.Duration
	batchPaths := make([]string, 0, len(batches))
	for i, batch := range batches {
		batchPath := filepath.Join(tempDir, fmt.Sprintf("batch-%04d.mp4", i))
		args, argsErr := sliceVideoRenderArgs(plan, bindings, batchPath, profile, spec.Payload["runtime_assets"], batch)
		if argsErr != nil {
			return fail("DRIVE_SLICE_ASSETS_INVALID", argsErr)
		}
		metrics, runErr := e.runner.Run(ctx, ffmpegrunner.FFmpegRequest{Operation: ffmpegrunner.OperationEncode, Args: args})
		if runErr != nil {
			return fail("DRIVE_SLICE_RENDER_FAILED", fmt.Errorf("batch %d/%d: %w", i+1, len(batches), runErr))
		}
		processWall += time.Duration(metrics.ProcessWallMS) * time.Millisecond
		batchPaths = append(batchPaths, batchPath)
	}
	listPath := filepath.Join(tempDir, "batches.ffconcat")
	if err := writeFFConcatList(listPath, batchPaths); err != nil {
		return fail("DRIVE_SLICE_STORAGE_INVALID", err)
	}
	finalArgs, err := sliceConcatMuxArgs(listPath, audio.Path, outputPath, plan)
	if err != nil {
		return fail("DRIVE_SLICE_ASSETS_INVALID", err)
	}
	metrics, renderErr := e.runner.Run(ctx, ffmpegrunner.FFmpegRequest{Operation: ffmpegrunner.OperationEncode, Args: finalArgs})
	if renderErr != nil {
		return fail("DRIVE_SLICE_RENDER_FAILED", fmt.Errorf("final stream-copy concat/mux: %w", renderErr))
	}
	processWall += time.Duration(metrics.ProcessWallMS) * time.Millisecond
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
	raw.EncodePasses = int32(len(batches))
	raw.ConcatMode = "batched_reencode"
	raw.FinalConcatStreamCopy = true
	raw.MediaDurationSeconds = float64(plan.DurationUS) / 1_000_000
	raw.WallClockSeconds = processWall.Seconds()
	return executor.ExecutionResult{
		Status: "succeeded", Outputs: []executor.ArtifactRef{artifact}, RawMetrics: raw,
		StartedAt: started, CompletedAt: time.Now().UTC(),
	}, nil
}

func (e *videoAssembleSliceExecutor) verifyDriveSources(ctx context.Context, plan *contract.CompiledRenderPlanV2, bindings runtimeassets.Bindings, runtimeAssets interface{}) error {
	assets := make(map[string]contract.AssetRefV2, len(plan.Assets))
	runtimeDriveIDs := sliceRuntimeDriveFileIDs(runtimeAssets)
	for _, asset := range plan.Assets {
		assets[asset.AssetID] = asset
	}
	seen := make(map[string]struct{})
	for _, segment := range plan.VideoTracks[0].Segments {
		asset := assets[segment.AssetID]
		if asset.Kind != "video" || !isSliceDriveSource(asset, runtimeDriveIDs) {
			continue
		}
		if _, done := seen[asset.AssetID]; done {
			continue
		}
		seen[asset.AssetID] = struct{}{}
		binding := bindings[asset.AssetID]
		probe, err := e.prober.ProbeMediaDetails(ctx, binding.Path)
		if err != nil {
			return fmt.Errorf("probe Drive source %q: %w", asset.AssetID, err)
		}
		if !probe.HasVideo || probe.DurationSec <= 0 {
			return fmt.Errorf("Drive source %q has no valid video stream/duration", asset.AssetID)
		}
		actualUS := int64(probe.DurationSec * 1_000_000)
		// For a source sliced by the Worker, AssetRefV2.DurationUS can be the
		// requested materialized window, not the full provider file duration.
		// The plan's source window is checked against the probed media below.
		for _, requested := range plan.VideoTracks[0].Segments {
			if requested.AssetID != asset.AssetID {
				continue
			}
			windowEnd := requested.SourceInUS + requested.SourceDurationUS
			frameUS := int64(1_000_000 * plan.Output.FPSDen / plan.Output.FPSNum)
			if windowEnd > actualUS+frameUS {
				return fmt.Errorf("Drive source %q window ends at %d us beyond probed duration %d us", asset.AssetID, windowEnd, actualUS)
			}
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
	wantDuration := float64(plan.DurationUS) / 1_000_000
	frameTolerance := float64(plan.Output.FPSDen)/float64(plan.Output.FPSNum) + 0.1
	if probe.DurationSec <= 0 || probe.DurationSec-wantDuration > frameTolerance || wantDuration-probe.DurationSec > frameTolerance {
		return fmt.Errorf("assembled output duration %.3fs does not match plan %.3fs (tolerance %.3fs)", probe.DurationSec, wantDuration, frameTolerance)
	}
	return nil
}

func groupSliceSegments(segments []contract.VideoSegmentV2, maxSources int) [][]contract.VideoSegmentV2 {
	if maxSources < 1 {
		maxSources = 1
	}
	var batches [][]contract.VideoSegmentV2
	seen := make(map[string]struct{})
	for _, segment := range segments {
		if len(batches) == 0 {
			batches = append(batches, nil)
		}
		if _, ok := seen[segment.AssetID]; !ok && len(seen) >= maxSources {
			batches = append(batches, nil)
			seen = make(map[string]struct{})
		}
		batches[len(batches)-1] = append(batches[len(batches)-1], segment)
		seen[segment.AssetID] = struct{}{}
	}
	return batches
}

func writeFFConcatList(path string, files []string) error {
	var b strings.Builder
	b.WriteString("ffconcat version 1.0\n")
	for _, file := range files {
		abs, err := filepath.Abs(file)
		if err != nil {
			return err
		}
		// The concat demuxer uses shell-like single-quoted paths; embedded
		// quotes are represented by closing, escaping, and reopening quotes.
		quoted := strings.ReplaceAll(abs, "'", "'\\''")
		fmt.Fprintf(&b, "file '%s'\n", quoted)
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
		"-t", seconds(plan.DurationUS), "-movflags", "+faststart", output}, nil
}

func sliceVideoRenderArgs(plan *contract.CompiledRenderPlanV2, bindings runtimeassets.Bindings, output string, profile contract.CanonicalVideoProfileV1, runtimeAssets interface{}, segments []contract.VideoSegmentV2) ([]string, error) {
	if len(segments) == 0 {
		return nil, errors.New("video batch has no segments")
	}
	assets := make(map[string]contract.AssetRefV2, len(plan.Assets))
	runtimeDriveIDs := sliceRuntimeDriveFileIDs(runtimeAssets)
	for _, asset := range plan.Assets {
		assets[asset.AssetID] = asset
	}
	inputs := make([]string, 0, maxSliceBatchSources)
	inputIndex := make(map[string]int, maxSliceBatchSources)
	for _, segment := range segments {
		if _, exists := inputIndex[segment.AssetID]; exists {
			continue
		}
		binding, ok := bindings[segment.AssetID]
		if !ok || !binding.Verified || strings.TrimSpace(binding.Path) == "" || binding.Size <= 0 || strings.TrimSpace(binding.SHA256) == "" {
			return nil, fmt.Errorf("source %q is not downloaded and integrity-verified", segment.AssetID)
		}
		asset := assets[segment.AssetID]
		if asset.SHA256 != "" && binding.SHA256 != asset.SHA256 {
			return nil, fmt.Errorf("source %q SHA-256 does not match the plan", segment.AssetID)
		}
		if asset.SizeBytes > 0 && binding.Size != asset.SizeBytes {
			return nil, fmt.Errorf("source %q size does not match the plan", segment.AssetID)
		}
		info, err := os.Stat(binding.Path)
		if err != nil || !info.Mode().IsRegular() || info.Size() != binding.Size {
			return nil, fmt.Errorf("source %q local file identity is invalid", segment.AssetID)
		}
		inputIndex[segment.AssetID] = len(inputs)
		inputs = append(inputs, binding.Path)
	}
	fps := strconv.Itoa(plan.Output.FPSNum) + "/" + strconv.Itoa(plan.Output.FPSDen)
	filters := make([]string, 0, len(segments)+1)
	labels := make([]string, 0, len(segments))
	var batchFrames int64
	for i, segment := range segments {
		asset := assets[segment.AssetID]
		windowEnd := segment.SourceInUS + segment.SourceDurationUS
		if segment.SourceInUS < 0 || segment.SourceDurationUS <= 0 || (!isSliceDriveSource(asset, runtimeDriveIDs) && windowEnd > asset.DurationUS) {
			return nil, fmt.Errorf("segment %q source window exceeds declared duration", segment.SegmentID)
		}
		label := fmt.Sprintf("v%d", i)
		filters = append(filters, fmt.Sprintf("[%d:v:0]trim=start=%s:duration=%s,setpts=PTS-STARTPTS,fps=%s,scale=%d:%d:force_original_aspect_ratio=decrease,pad=%d:%d:(ow-iw)/2:(oh-ih)/2,setsar=1,format=%s,trim=end_frame=%d,setpts=N/(%s*TB)[%s]",
			inputIndex[segment.AssetID], seconds(segment.SourceInUS), seconds(segment.SourceDurationUS), fps,
			plan.Output.Width, plan.Output.Height, plan.Output.Width, plan.Output.Height, profile.PixelFormat, segment.FrameCount, fps, label))
		labels = append(labels, "["+label+"]")
		batchFrames += segment.FrameCount
	}
	filters = append(filters, strings.Join(labels, "")+fmt.Sprintf("concat=n=%d:v=1:a=0[vout]", len(labels)))
	// A Drive render can reference hundreds of separate source files. FFmpeg's
	// automatic decoder/filter thread counts multiply across those inputs and
	// can exhaust the worker container's PID limit before rendering starts.
	// Bound decoder, filter-graph, and encoder concurrency for predictable use.
	args := []string{"-nostdin", "-hide_banner", "-loglevel", "error", "-filter_complex_threads", "1", "-y"}
	for _, input := range inputs {
		args = append(args, "-threads", "1", "-i", input)
	}
	batchDurationUS := batchFrames * int64(plan.Output.FPSDen) * 1_000_000 / int64(plan.Output.FPSNum)
	args = append(args, "-filter_complex", strings.Join(filters, ";"), "-map", "[vout]",
		"-t", seconds(batchDurationUS), "-c:v", "libx264", "-threads:v", "2", "-preset", "veryfast", "-profile:v", profile.CodecProfile,
		"-level:v", profile.CodecLevel, "-pix_fmt", profile.PixelFormat, "-r", fps, "-g", strconv.Itoa(profile.GOPSize),
		"-keyint_min", strconv.Itoa(profile.GOPSize), "-sc_threshold", "0", "-flags", "+cgop",
		"-bf", strconv.Itoa(profile.BFrames), "-an", "-movflags", "+faststart", output)
	return args, nil
}

func seconds(microseconds int64) string {
	return strconv.FormatFloat(float64(microseconds)/1_000_000, 'f', 6, 64)
}
