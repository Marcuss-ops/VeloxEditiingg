// Package executors — scene.composite.v1 execution path.
//
// scene_composite_execute.go owns Execute and its direct helpers
// (compiledPlanClipCount, rawMetricsFromPipeline, renderErrorCode,
// resolveOutputPath, PayloadOutputPath). The executor surface (type,
// constructor, Descriptor, Validate) lives in scene_composite.go.
package executors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"velox-shared/contract"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/performance"
	"velox-worker-agent/pkg/storage"
	"velox-worker-agent/pkg/video/pipeline"
)

// Execute performs the canonical work. It delegates to the existing
// pipeline.Runner using the explicit payload `pipeline_id` (the legacy
// hybrid.v1 fallback was retired).
//
// CAVEAT: the C++ engine runs as a synchronous subprocess; context
// cancellation propagates only AFTER the engine finishes. The
// descriptor's TemporalMode=global + Deterministic=true advertise this
// property to the master scheduler.
func (s *SceneComposite) Execute(ctx context.Context, execCtx executor.ExecutionContext, spec executor.TaskSpec) (executor.ExecutionResult, error) {
	startedAt := time.Now().UTC()
	timer := jobPhaseTimerFromExecutionContext(execCtx)
	gpuTracker := gpuTransferTrackerFromExecutionContext(execCtx)
	// Legacy compatibility projection for pre-typed report consumers. The
	// canonical producer output is rawMetrics below; this map is retired
	// incrementally as downstream consumers adopt RawMetrics.
	metrics := make(map[string]interface{})
	rec := recorderFromExecutionContext(execCtx)
	planHandle := rec.Begin(telemetry.EventSpec{Origin: telemetry.OriginWorker, Scope: telemetry.ScopeTask, Component: "worker.plan", Action: "compile"})
	if planHandle != nil {
		planHandle.SetMetadata("pipeline_id", resolvePipelineID(spec.Payload))
	}
	planCompleted := false
	defer func() {
		if planHandle == nil || planCompleted {
			return
		}
		if execCtx.Err() != nil {
			planHandle.Abort("context_cancelled", execCtx.Err().Error())
			return
		}
		planHandle.Abort("execution_failed", "scene composite exited before plan completion")
	}()

	outputPath, err := s.resolveOutputPath(execCtx, spec)
	if err != nil {
		return executor.ExecutionResult{
			Status:      "failed",
			ErrorCode:   "output_path_invalid",
			ErrorDetail: err.Error(),
			StartedAt:   startedAt,
			CompletedAt: time.Now().UTC(),
		}, nil
	}

	// Phase: pipeline run. We use RunWithMetrics so callers get
	// per-phase pipeline timings AND the native C++ engine sidecar
	// counters (frames, speed_x, encode_passes, temp_bytes,
	// duration_seconds) merged into the final task-scoped metrics map.
	pipelineID := resolvePipelineID(spec.Payload)
	pipelineStart := time.Now()

	// Fine-grained phase timer: wrap the pipeline run for the top-level video
	// phases. Sub-phase timings are populated from the engine's own sidecar
	// detailed phases after the run completes.
	var spanDecode, spanSubtitle, spanBlur, spanWatermark, spanFilter, spanComposite, spanEncode string
	if timer != nil {
		spanDecode = timer.Begin(telemetry.PhaseVideoDecode)
		spanSubtitle = timer.Begin(telemetry.PhaseVideoSubtitle)
		spanBlur = timer.Begin(telemetry.PhaseVideoBlur)
		spanWatermark = timer.Begin(telemetry.PhaseVideoWatermark)
		spanFilter = timer.Begin(telemetry.PhaseVideoFilter)
		spanComposite = timer.Begin(telemetry.PhaseVideoComposite)
		spanEncode = timer.Begin(telemetry.PhaseVideoEncode)
	}

	runMetrics, err := s.pipelineRunner.RunWithMetrics(ctx, pipelineID, spec.JobID, spec.Payload, outputPath)

	// Populate fine-grained phase timings from the C++ engine's detailed
	// phase ledger. Each engine component/action is mapped to a fine-grained
	// phase and accumulated into the shared timer.
	if timer != nil {
		// Close all spans (the engine run is synchronous).
		timer.End(spanDecode)
		timer.End(spanSubtitle)
		timer.End(spanBlur)
		timer.End(spanWatermark)
		timer.End(spanFilter)
		timer.End(spanComposite)
		timer.End(spanEncode)
		// Map engine detailed phases into fine-grained timer data.
		populateTimerFromEnginePhases(timer, runMetrics.RenderMetrics.DetailedPhases)
		// Populate per-scene breakdown from engine segments and detailed phases.
		populateSceneTimingsFromEngine(timer, runMetrics.RenderMetrics.Segments, runMetrics.RenderMetrics.DetailedPhases)
	}
	// Feed GPU transfer tracker from engine detailed phases.
	if gpuTracker != nil {
		ingestGPUTransferPhases(gpuTracker, runMetrics.RenderMetrics.DetailedPhases)
	}

	// Materialize native telemetry before handling the render error. The
	// engine may have emitted useful completed phases and segment timings
	// even when the process fails or is cancelled.
	clipCount := compiledPlanClipCount(spec.Payload)
	derivedIO, derivedCPU := projectRunMetrics(metrics, pipelineID, pipelineStart, runMetrics, clipCount)
	emitEngineProcessTelemetry(rec, runMetrics)
	segments := projectSegments(runMetrics.RenderMetrics)
	detailedPhases := projectDetailedPhases(runMetrics.RenderMetrics)
	rawMetrics := rawMetricsFromPipeline(runMetrics, derivedIO, derivedCPU)

	if err != nil {
		// A cancelled render is not a renderer failure. The native client
		// already terminates its process group; remove any partial output
		// before returning the cancellation sentinel to TaskRunner.
		_ = os.Remove(outputPath)
		_ = os.Remove(outputPath + ".progress.json")
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return executor.ExecutionResult{
				Status:    "failed",
				ErrorCode: "cancelled", ErrorDetail: err.Error(),
				RawMetrics:     rawMetrics,
				Metrics:        metrics,
				Segments:       segments,
				DetailedPhases: detailedPhases,
				StartedAt:      startedAt,
				CompletedAt:    time.Now().UTC(),
			}, err
		}
		return executor.ExecutionResult{
			Status:         "failed",
			ErrorCode:      renderErrorCode(err),
			ErrorDetail:    fmt.Sprintf("pipeline.Runner.RunWithMetrics(%s): %v", pipelineID, err),
			RawMetrics:     rawMetrics,
			Metrics:        metrics,
			Segments:       segments,
			DetailedPhases: detailedPhases,
			StartedAt:      startedAt,
			CompletedAt:    time.Now().UTC(),
		}, nil
	}
	// Fail-closed artifact boundary: the primary output and its progress
	// receipt must both exist and have a real manifest before success.
	outputs, outputManifest, failResult := verifyAndBuildOutputs(ctx, outputPath, startedAt, metrics, rawMetrics, runMetrics, clipCount, planHandle, rec)
	if failResult != nil {
		return *failResult, nil
	}

	planHandle.CompleteWith(0, outputManifest.SizeBytes, runMetrics.RenderMetrics.Frames, telemetry.StatusOK, "", "")
	planCompleted = true
	if rec != nil {
		status := telemetry.StatusOK
		if !outputManifest.FfprobeValid {
			status = telemetry.StatusFailed
		}
		rec.Emit(telemetry.EventSpec{Origin: telemetry.OriginValidation, Scope: telemetry.ScopeAttempt, Component: "quality", Action: "ffprobe"}, status, outputManifest.FfprobeErr, "")
	}
	return executor.ExecutionResult{
		Status:         "succeeded",
		Outputs:        outputs,
		RawMetrics:     rawMetrics,
		Metrics:        metrics,
		Segments:       segments,
		DetailedPhases: detailedPhases,
		StartedAt:      startedAt,
		CompletedAt:    time.Now().UTC(),
	}, nil
}

func compiledPlanClipCount(payload map[string]interface{}) int {
	raw, ok := payload[contract.PayloadKeyCompiledRenderPlanJSON].(string)
	if !ok || strings.TrimSpace(raw) == "" {
		return 0
	}
	plan, err := contract.DecodeCompiledRenderPlanV2([]byte(raw))
	if err != nil || plan == nil {
		return 0
	}
	count := 0
	for _, track := range plan.VideoTracks {
		count += len(track.Segments)
	}
	return count
}

func rawMetricsFromPipeline(run pipeline.RunMetrics, io performance.IOMetrics, cpu performance.CPUMetrics) *telemetry.RawExecutionMetrics {
	rm := run.RenderMetrics
	m := &telemetry.RawExecutionMetrics{
		InputBytes:           io.AssetBytesRead,
		OutputBytes:          io.FinalBytesWritten,
		CpuTimeMs:            cpu.CPUTotalMs,
		PeakRssBytes:         rm.PeakRSSBytes,
		FramesDecoded:        rm.FramesDecoded,
		FramesComposited:     rm.FramesComposited,
		FramesEncoded:        rm.Frames,
		FfmpegSpeedRatio:     rm.SpeedX,
		EncodePasses:         int32(rm.EncodePasses),
		ConcatMode:           rm.ConcatMode,
		TempBytesWritten:     io.TempBytesWritten,
		MediaDurationSeconds: rm.DurationSec,
		WallClockSeconds:     float64(run.TotalMs) / 1000,
		OutputFileSize:       io.FinalBytesWritten,
		DiskReadBytes:        io.TotalBytesRead,
		DiskWriteBytes:       io.TotalBytesWritten,

		// ── CPU attribution from engine sidecar ────────────────────
		CpuUserMs:   rm.CPUUserMs,
		CpuSystemMs: rm.CPUSystemMs,

		// ── Process spawn metrics from engine sidecar ──────────────
		// /proc sampler counts of external processes spawned by the engine.
		FfmpegExecCount:  rm.FfmpegExecCount,
		FfprobeExecCount: rm.FfprobeExecCount,
		// Engine-level spawn count (cmd.Start() succeeded) + timing.
		ProcessSpawnCount: rm.EngineSpawnCount,
		ProcessStartupMs:  rm.EngineSpawnMs,
		// Total time consumed by external ffmpeg/ffprobe processes.
		FfmpegProcessMs:  rm.ProcessWaitMs,
		FfprobeProcessMs: 0, // not separated in current /proc sampler; zero for clarity
	}
	// Derive segment statistics from the engine's per-segment sidecar.
	computePipelineSegmentStats(m, run)
	// Derive audio metrics from pipeline metadata.
	computePipelineAudioStats(m, run)
	// Derive cpu_percent_avg from total CPU time vs wall clock.
	if m.WallClockSeconds > 0 && m.CpuTimeMs > 0 {
		// cpu_percent_avg: average CPU utilization across the render window.
		// cpu_time_ms is accumulated across all cores; divide by wall clock
		// and by logical CPU count to get a 0-100 percentage.
		logicalCPUs := runtime.NumCPU()
		if logicalCPUs > 0 {
			m.CpuPercentAvg = (float64(m.CpuTimeMs) / (m.WallClockSeconds * 1000) / float64(logicalCPUs)) * 100
		}
	}
	return m
}

// computePipelineSegmentStats derives per-segment packet-copy/re-encode/composite
// breakdown from the engine sidecar. Classification rules:
//
//   - packet_copy: segment with FfmpegEncodeMS == 0 (stream/packet copy)
//   - reencoded:   segment with FfmpegEncodeMS > 0 and FramesComposited == 0
//   - composited:  segment with FramesComposited > 0
//
// Byte totals: packet_copy_bytes sums source bytes of copy-only segments;
// reencoded_bytes sums source bytes of re-encoded segments. Duration is
// the sum of FfmpegEncodeMS for each category.
func computePipelineSegmentStats(m *telemetry.RawExecutionMetrics, run pipeline.RunMetrics) {
	rm := run.RenderMetrics
	segments := rm.Segments
	m.SegmentsTotal = int32(len(segments))
	if m.SegmentsTotal == 0 {
		return
	}

	var packetCopyCount, reencodedCount, compositedCount int32
	var packetCopyBytes, reencodedBytes int64
	var packetCopyDurMs, reencodeDurMs int64

	for _, seg := range segments {
		encodeMs := int64(seg.FfmpegEncodeMS)
		isComposited := seg.FramesComposited > 0
		isPacketCopy := encodeMs == 0 && !isComposited

		if isPacketCopy {
			packetCopyCount++
			packetCopyBytes += seg.SourceBytes
			packetCopyDurMs += encodeMs // 0 for stream copy
		} else if isComposited {
			compositedCount++
			// Composited segments are also encoded, so source bytes
			// go to reencoded (filters change pixel data).
			reencodedBytes += seg.SourceBytes
			reencodeDurMs += encodeMs
		} else {
			reencodedCount++
			reencodedBytes += seg.SourceBytes
			reencodeDurMs += encodeMs
		}
	}

	m.SegmentsPacketCopy = packetCopyCount
	m.SegmentsReencoded = reencodedCount
	m.SegmentsComposited = compositedCount
	m.PacketCopyBytes = packetCopyBytes
	m.ReencodedBytes = reencodedBytes
	m.PacketCopyDurationMs = packetCopyDurMs
	m.ReencodeDurationMs = reencodeDurMs
	if m.SegmentsTotal > 0 {
		m.PacketCopyRatio = float64(packetCopyCount) / float64(m.SegmentsTotal) * 100
	}
}

// computePipelineAudioStats fills audio encode/copy metrics from pipeline
// metadata. Classification: when the engine concat mode indicates a stream
// copy for audio (AudioTracks == 0 or the Observability map has audio_copy),
// we count it as a copy; otherwise we treat the audio as re-encoded.
//
// The engine sidecar Observability map is the authoritative source when
// present; otherwise we fall back to AudioTracks heuristic.
func computePipelineAudioStats(m *telemetry.RawExecutionMetrics, run pipeline.RunMetrics) {
	rm := run.RenderMetrics
	obs := rm.Observability

	// Extract audio metrics from sidecar when available.
	// Observability keys are engine-defined: audio_copy_ms, audio_encode_ms,
	// audio_input_bytes, audio_output_bytes, audio_packet_copy_count,
	// audio_reencode_count.
	if obs != nil {
		if v, ok := intFromObs(obs, "audio_copy_ms"); ok {
			m.AudioCopyMs = v
		}
		if v, ok := intFromObs(obs, "audio_encode_ms"); ok {
			m.AudioEncodeMs = v
		}
		if v, ok := intFromObs(obs, "audio_input_bytes"); ok {
			m.AudioInputBytes = v
		}
		if v, ok := intFromObs(obs, "audio_output_bytes"); ok {
			m.AudioOutputBytes = v
		}
		if v, ok := intFromObs(obs, "audio_packet_copy_count"); ok {
			m.AudioPacketCopy = v
		}
		if v, ok := intFromObs(obs, "audio_reencode_count"); ok {
			m.AudioReencoded = v
		}
	}

	// Heuristic: when no observability data exists, infer from track count.
	// AudioTracks == 0 means audio was stream-copied (no engine processing).
	if m.AudioCopyMs == 0 && m.AudioEncodeMs == 0 {
		if run.AudioTracks == 0 {
			m.AudioPacketCopy = 1
			m.AudioReencoded = 0
		}
	}
}

// intFromObs extracts an int64 value from an engine observability map.
func intFromObs(obs map[string]interface{}, key string) (int64, bool) {
	v, ok := obs[key]
	if !ok {
		return 0, false
	}
	switch x := v.(type) {
	case float64:
		return int64(x), true
	case int64:
		return x, true
	case int:
		return int64(x), true
	case int32:
		return int64(x), true
	}
	return 0, false
}

func renderErrorCode(err error) string {
	if err != nil && strings.Contains(strings.ToLower(err.Error()), "copy_only media contract rejected") {
		return "COPY_ONLY_MEDIA_INCOMPATIBLE"
	}
	return "execute_failed"
}

// resolveOutputPath resolves where the final artifact is produced. When the
// task's ExecutionContext carries the canonical StorageResolver, the
// ARTIFACT_STAGING placement decision (tmpfs-with-reservation → NVMe
// fallback) owns the path; otherwise the executor falls back to its legacy
// <outputBase>/<jobID>.mp4 root. The payload's "output_path" is
// intentionally ignored — it refers to a path on the master and is not
// reachable inside the worker container. The worker always renders into its
// own storage and uploads the result to the master via the artifact API.
func (s *SceneComposite) resolveOutputPath(execCtx executor.ExecutionContext, spec executor.TaskSpec) (string, error) {
	if spec.JobID == "" {
		return "", fmt.Errorf("scene.composite.v1: missing JobID; cannot synthesize output path")
	}
	rel := spec.JobID + ".mp4"
	if resolver := storageResolverFromExecutionContext(execCtx); resolver != nil {
		placement, err := resolver.Place(storage.ArtifactStaging, rel, estimateOutputBytes(spec.Payload))
		if err == nil {
			return placement.Path, nil
		}
	}
	return filepath.Join(s.outputBase, rel), nil
}

// storageResolverFromExecutionContext extracts the canonical StorageResolver
// from the optional-interface seam exposed by taskrunner.runnerContext. It
// returns nil when the context does not provide one (legacy/headless
// executors, test stubs) so callers fall back to the outputBase root.
func storageResolverFromExecutionContext(execCtx executor.ExecutionContext) *storage.Resolver {
	if provider, ok := execCtx.(interface{ StorageResolver() *storage.Resolver }); ok && provider != nil {
		return provider.StorageResolver()
	}
	return nil
}

// Estimate the final artifact size for the tmpfs reservation. Over-reserving
// only costs a spurious NVMe fallback; under-reserving risks ENOSPC at 98%
// of the render. We therefore use a generous composite bitrate and a 20%
// margin. Unknown durations return -1, the resolver's "unknown size → NVMe"
// sentinel.
const (
	// outputEstimateConservativeBps is the assumed video+audio composite
	// bitrate for the reservation estimate.
	outputEstimateConservativeBps = 12_000_000
	// outputEstimateMargin inflates the byte estimate for container/mux
	// overhead.
	outputEstimateMargin = 1.20
)

func estimateOutputBytes(payload map[string]interface{}) int64 {
	dur := timelineDurationSeconds(payload)
	if dur <= 0 {
		return -1
	}
	return int64(float64(dur) * outputEstimateConservativeBps / 8 * outputEstimateMargin)
}

// estimateOutputBytesFromDuration estimates the final artifact size from a
// declared media duration in microseconds, using the same conservative
// composite bitrate and margin as estimateOutputBytes. Non-positive durations
// return -1, the resolver's "unknown size → NVMe" sentinel.
func estimateOutputBytesFromDuration(durationUS int64) int64 {
	if durationUS <= 0 {
		return -1
	}
	return int64(float64(durationUS) / 1_000_000 * outputEstimateConservativeBps / 8 * outputEstimateMargin)
}

// timelineDurationSeconds sums the payload's declared timeline duration,
// following the hybrid compiler's canonical precedence: items[] first, then
// scenes_json, then the top-level duration_seconds.
func timelineDurationSeconds(payload map[string]interface{}) float64 {
	if payload == nil {
		return 0
	}
	if items, ok := payload["items"].([]interface{}); ok {
		var total float64
		for _, it := range items {
			if im, ok := it.(map[string]interface{}); ok {
				total += durationSecondsOf(im)
			}
		}
		if total > 0 {
			return total
		}
	}
	if encoded, ok := payload["scenes_json"].(string); ok && strings.TrimSpace(encoded) != "" {
		var scenes []map[string]interface{}
		if err := json.Unmarshal([]byte(encoded), &scenes); err == nil {
			var total float64
			for _, sc := range scenes {
				total += durationSecondsOf(sc)
			}
			if total > 0 {
				return total
			}
		}
	}
	return durationSecondsOf(payload)
}

// durationSecondsOf reads "duration_seconds" then "duration" (items use the
// shorter key) from a map, accepting the numeric shapes Go's JSON decoder
// can produce.
func durationSecondsOf(m map[string]interface{}) float64 {
	for _, key := range []string{"duration_seconds", "duration"} {
		switch v := m[key].(type) {
		case float64:
			return v
		case int:
			return float64(v)
		case int64:
			return float64(v)
		case json.Number:
			if f, err := v.Float64(); err == nil {
				return f
			}
		}
	}
	return 0
}

// PayloadOutputPath returns the master-originated output_path stored in
// the spec payload (if any). Callers use this as the upload target
// filename, NOT as a local render path.
func PayloadOutputPath(spec executor.TaskSpec) string {
	p, _ := spec.Payload["output_path"].(string)
	return p
}

// populateSceneTimingsFromEngine maps per-segment engine data and per-segment
// detailed phases into the shared JobPhaseTimer's per-scene breakdown.
// This produces the TOP SLOWEST SCENES ranking in the performance report.
func populateSceneTimingsFromEngine(timer *telemetry.JobPhaseTimer, segments []pipeline.SegmentTiming, phases []pipeline.DetailedPhaseTiming) {
	if timer == nil {
		return
	}
	// Phase 1: ingest per-segment summary data (scene-level totals).
	for _, seg := range segments {
		sceneID := seg.SceneID
		if sceneID == "" {
			continue
		}
		// Add segment-level aggregate data.
		timer.AddSceneData(
			sceneID,
			int64(seg.InputDurationMS),
			int64(seg.OutputDurationMS),
			seg.SourceBytes,
			seg.OutputBytes,
			seg.FramesDecoded,
			seg.FramesEncoded,
			seg.FfmpegSpeedX,
		)
		// Add segment-level download and encode phases.
		if seg.AssetDownloadMS > 0 {
			timer.AddScenePhaseData(sceneID, telemetry.PhaseAssetDownload,
				seg.SourceBytes, 0, 0, 0, 0,
			)
		}
	}

	// Phase 2: ingest per-segment detailed phases for sub-phase breakdown.
	// Build a segment index → sceneID map for fast lookup.
	segScene := make(map[int32]string, len(segments))
	for _, seg := range segments {
		segScene[int32(seg.SegmentIndex)] = seg.SceneID
	}
	for _, phase := range phases {
		if phase.SegmentIndex < 0 {
			continue
		}
		sceneID, ok := segScene[phase.SegmentIndex]
		if !ok || sceneID == "" {
			continue
		}
		finePhase := mapEnginePhaseToFineGrained(phase.Component, phase.Action)
		if finePhase == "" {
			continue
		}
		timer.AddScenePhaseData(sceneID, finePhase,
			phase.BytesIn, phase.BytesOut,
			phase.FramesIn, phase.FramesOut,
			phase.CPUMS,
		)
	}
}

// ingestGPUTransferPhases feeds the engine's detailed phase stream into the
// GPU transfer tracker. Each phase is classified as GPU-side or CPU-side;
// transfers are inferred when frames cross the PCIe boundary.
func ingestGPUTransferPhases(tracker *telemetry.GPUTransferTracker, phases []pipeline.DetailedPhaseTiming) {
	if tracker == nil {
		return
	}
	ingests := make([]telemetry.PhaseIngest, 0, len(phases))
	for _, phase := range phases {
		ingests = append(ingests, telemetry.PhaseIngest{
			Component:  phase.Component,
			Action:     phase.Action,
			FramesIn:   phase.FramesIn,
			FramesOut:  phase.FramesOut,
			BytesIn:    phase.BytesIn,
			BytesOut:   phase.BytesOut,
			DurationMS: phase.DurationMS,
		})
	}
	tracker.IngestEnginePhases(ingests)
}

// populateTimerFromEnginePhases maps the C++ engine's detailed phase ledger
// onto the shared fine-grained JobPhaseTimer. Each engine component/action
// pair is mapped to the corresponding fine-grained phase; duration, bytes,
// frames, and CPU time are accumulated.
func populateTimerFromEnginePhases(timer *telemetry.JobPhaseTimer, phases []pipeline.DetailedPhaseTiming) {
	if timer == nil {
		return
	}
	for _, phase := range phases {
		finePhase := mapEnginePhaseToFineGrained(phase.Component, phase.Action)
		if finePhase == "" {
			continue
		}
		timer.AddPhaseData(finePhase,
			phase.BytesIn, phase.BytesOut,
			phase.FramesIn, phase.FramesOut,
			phase.CPUMS, phase.QueueWaitMS,
		)
	}
}

// mapEnginePhaseToFineGrained maps C++ engine component/action pairs to the
// canonical fine-grained phase name. Returns "" for unknown pairs.
func mapEnginePhaseToFineGrained(component, action string) string {
	// Normalize to a canonical key for matching: "component.action"
	key := component + "." + action

	// Direct matches first.
	switch key {
	case "engine.video.decode":
		return telemetry.PhaseVideoDecode
	case "engine.video.blur":
		return telemetry.PhaseVideoBlur
	case "engine.video.filter":
		return telemetry.PhaseVideoFilter
	case "engine.video.composite", "engine.composite":
		return telemetry.PhaseVideoComposite
	case "engine.encode.setup", "engine.encode.frame_submit", "engine.encode.flush":
		return telemetry.PhaseVideoEncode
	case "engine.subtitle.render", "engine.video.subtitle":
		return telemetry.PhaseVideoSubtitle
	case "engine.subtitle.raster", "engine.video.subtitle_raster":
		return telemetry.PhaseVideoSubtitleRaster
	case "engine.subtitle.composite", "engine.video.subtitle_composite":
		return telemetry.PhaseVideoSubtitleComposite
	case "engine.watermark.upload", "engine.video.watermark_upload":
		return telemetry.PhaseVideoWatermarkUpload
	case "engine.watermark.composite", "engine.video.watermark_composite":
		return telemetry.PhaseVideoWatermarkComposite
	case "engine.watermark.render", "engine.video.watermark":
		return telemetry.PhaseVideoWatermark
	case "engine.audio.mix":
		return telemetry.PhaseAudioPrepare
	case "engine.mux.audio", "engine.mux.finalize":
		return telemetry.PhaseAudioMux
	case "engine.concat":
		return telemetry.PhaseVideoConcat
	}

	// Fallback: match by component prefix for grouped phases.
	switch {
	case strings.HasPrefix(component, "engine.video"):
		switch action {
		case "decode":
			return telemetry.PhaseVideoDecode
		case "blur":
			return telemetry.PhaseVideoBlur
		case "filter":
			return telemetry.PhaseVideoFilter
		case "composite":
			return telemetry.PhaseVideoComposite
		case "subtitle":
			return telemetry.PhaseVideoSubtitle
		case "subtitle_raster":
			return telemetry.PhaseVideoSubtitleRaster
		case "subtitle_composite":
			return telemetry.PhaseVideoSubtitleComposite
		case "watermark":
			return telemetry.PhaseVideoWatermark
		case "watermark_upload":
			return telemetry.PhaseVideoWatermarkUpload
		case "watermark_composite":
			return telemetry.PhaseVideoWatermarkComposite
		}
	case strings.HasPrefix(component, "engine.encode"):
		return telemetry.PhaseVideoEncode
	case strings.HasPrefix(component, "engine.subtitle"):
		switch action {
		case "raster":
			return telemetry.PhaseVideoSubtitleRaster
		case "composite":
			return telemetry.PhaseVideoSubtitleComposite
		default:
			return telemetry.PhaseVideoSubtitle
		}
	case strings.HasPrefix(component, "engine.watermark"):
		switch action {
		case "upload":
			return telemetry.PhaseVideoWatermarkUpload
		case "composite":
			return telemetry.PhaseVideoWatermarkComposite
		default:
			return telemetry.PhaseVideoWatermark
		}
	case strings.HasPrefix(component, "engine.audio"):
		return telemetry.PhaseAudioPrepare
	case strings.HasPrefix(component, "engine.mux"):
		return telemetry.PhaseAudioMux
	case component == "engine" && action == "composite":
		return telemetry.PhaseVideoComposite
	case component == "engine" && action == "render":
		// Top-level engine render: span wraps sub-phases, not individually accumulated.
		return ""
	case component == "engine" && action == "concat":
		return telemetry.PhaseVideoConcat
	}
	return ""
}
