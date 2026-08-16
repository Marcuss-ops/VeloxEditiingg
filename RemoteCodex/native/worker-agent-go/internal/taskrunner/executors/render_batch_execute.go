// Package executors — render_batch@1 execution path.
//
// render_batch_execute.go owns Execute (the two-phase video packet-copy +
// stream-copy mux orchestration) and its runCommand helper. The executor surface lives in
// render_batch_executor.go.
package executors

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/runtimeassets"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/storage"
	"velox-worker-agent/pkg/video/ffmpegrunner"
)

// Execute assembles complete, compatible video clips with packet-copy only,
// then muxes the one already-finalized audio asset. It never calls AudioMix,
// never encodes video, and never encodes the final audio stream. Any trim,
// gap, filter or stream-identity mismatch is rejected before FFmpeg starts.
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
	obs.timelineRevision = plan.TimelineRevision
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
	audioResolveStarted := time.Now()
	audioErr := validateMediaFile(e.probe, ctx, bindings[plan.FinalAudio.AssetID].Path, "final audio", plan.DurationUS, false, true, &plan.FinalAudio)
	obs.metrics.Set("final_audio_resolve_ms", time.Since(audioResolveStarted).Milliseconds())
	if audioErr != nil {
		obs.finish(assetResolution, telemetry.StatusFailed, "FINAL_AUDIO_INVALID", audioErr)
		return obs.failure(started, "FINAL_AUDIO_INVALID", audioErr), nil
	}
	videoPaths, videoErr := validatePacketCopySources(plan, bindings, e.probe, ctx)
	if videoErr != nil {
		obs.finish(assetResolution, telemetry.StatusFailed, "COPY_ONLY_VIDEO_INCOMPATIBLE", videoErr)
		return obs.failure(started, "COPY_ONLY_VIDEO_INCOMPATIBLE", videoErr), nil
	}
	obs.finish(assetResolution, telemetry.StatusOK, "", nil)

	if err := os.MkdirAll(e.outputRoot, 0o750); err != nil {
		obs.logFailure("output_directory", "output_directory", err)
		return obs.failure(started, "output_directory", err), nil
	}
	videoOnlyPath := filepath.Join(e.outputRoot, jobID+".video-only.mp4")
	concatListPath := filepath.Join(e.outputRoot, jobID+".concat.txt")
	finalPath := e.resolveFinalOutputPath(execCtx, jobID, plan.DurationUS)
	defer os.Remove(videoOnlyPath)
	defer os.Remove(concatListPath)
	if err := writeConcatList(concatListPath, videoPaths); err != nil {
		obs.logFailure("video_packet_copy", "COPY_ONLY_VIDEO_INCOMPATIBLE", err)
		return obs.failure(started, "COPY_ONLY_VIDEO_INCOMPATIBLE", err), nil
	}

	visual := obs.begin("visual_render", "engine", "render")
	visualArgs := buildVideoOnlyPacketCopyArgs(concatListPath, videoOnlyPath)
	visualArtifact, visualProfile, visualRaw, err := e.runCommand(ctx, execCtx, ffmpegrunner.OperationCompose, visualArgs, videoOnlyPath, "video-only")
	obs.mergeRawMetrics(visualRaw)
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
	finalArtifact, muxProfile, muxRaw, err := e.runCommand(ctx, execCtx, ffmpegrunner.OperationEncode, muxArgs, finalPath, "final-mux")
	obs.mergeRawMetrics(muxRaw)
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
	metrics.Set("compiled_asset_count", int64(len(plan.Assets)))
	metrics.Set("audio_mix_count", int64(0))
	metrics.Set("audio_encode_count", int64(0))
	metrics.Set("final_audio_copy", int64(1))
	metrics.Set("video_packet_copy", int64(1))
	metrics.Set("video_encode_count", int64(0))
	metrics.Set("video_only_bytes", visualArtifact.SizeBytes)
	metrics.Set("final_output_bytes", finalArtifact.SizeBytes)
	metrics.Set("ffmpeg_visual_profile", visualProfile)
	metrics.Set("ffmpeg_mux_profile", muxProfile)
	obs.info("render_batch.succeeded", map[string]interface{}{"compiled_asset_count": int64(len(plan.Assets)), "final_audio_copy": int64(1), "video_packet_copy": int64(1)})

	obs.ensureRawMetrics()
	obs.rawMetrics.OutputBytes = finalArtifact.SizeBytes
	obs.rawMetrics.OutputFileSize = finalArtifact.SizeBytes
	obs.rawMetrics.OutputSha256 = finalArtifact.Hash
	obs.rawMetrics.MediaDurationSeconds = float64(plan.DurationUS) / 1_000_000
	obs.rawMetrics.WallClockSeconds = time.Since(started).Seconds()
	obs.rawMetrics.FinalConcatStreamCopy = true
	obs.rawMetrics.ConcatMode = "stream_copy"
	return executor.ExecutionResult{
		Status: "succeeded", Outputs: []executor.ArtifactRef{finalArtifact},
		RawMetrics: obs.rawMetrics, Metrics: metrics.Map(), StartedAt: started, CompletedAt: time.Now().UTC(),
	}, nil
}

// resolveFinalOutputPath routes the final artifact through the canonical
// StorageResolver (ARTIFACT_STAGING: tmpfs-with-reservation → NVMe fallback)
// when one is present in the ExecutionContext, mirroring scene_composite.
// Without a resolver the legacy outputRoot root is used, so headless/test
// harnesses that never wire storage keep working unchanged.
func (e *renderBatchExecutor) resolveFinalOutputPath(execCtx executor.ExecutionContext, jobID string, durationUS int64) string {
	if resolver := storageResolverFromExecutionContext(execCtx); resolver != nil {
		if placement, err := resolver.Place(storage.ArtifactStaging, jobID+".mp4", estimateOutputBytesFromDuration(durationUS)); err == nil {
			return placement.Path
		}
	}
	return filepath.Join(e.outputRoot, jobID+".mp4")
}

func (e *renderBatchExecutor) runCommand(ctx context.Context, execCtx executor.ExecutionContext, operation ffmpegrunner.OperationType, args []string, outputPath, outputType string) (executor.ArtifactRef, map[string]interface{}, *telemetry.RawExecutionMetrics, error) {
	profileResult, runErr := e.runner.Run(ctx, ffmpegrunner.FFmpegRequest{Operation: operation, Args: args})
	if sink, ok := execCtx.(interface {
		FFmpegProfiles() *ffmpegrunner.Aggregator
	}); ok && sink.FFmpegProfiles() != nil {
		sink.FFmpegProfiles().Add(profileResult)
	}
	profile := ffmpegProfileMetadata(profileResult)
	rawMetrics := rawMetricsFromFFmpegResult(profileResult)
	if runErr != nil {
		return executor.ArtifactRef{}, profile, rawMetrics, fmt.Errorf("render_batch@1 %s: %w (exit_code=%d)", outputType, runErr, profileResult.ExitCode)
	}
	artifact, err := artifactFromFile("video/mp4", outputPath)
	if err != nil {
		return executor.ArtifactRef{}, profile, rawMetrics, fmt.Errorf("render_batch@1 %s artifact: %w", outputType, err)
	}
	return artifact, profile, rawMetrics, nil
}
