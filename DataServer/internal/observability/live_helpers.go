package observability

import "velox-server/internal/taskattempts"

// applyLiveAttemptOverlay copies only volatile execution fields onto a
// durable/non-terminal summary. Identity, status, errors, timestamps, and
// final metrics remain owned by the durable attempt; the live row is an
// explicitly temporary overlay.
func applyLiveAttemptOverlay(target *AttemptSummary, live *LiveAttempt) {
	if target == nil || live == nil {
		return
	}
	target.Live = true
	target.Phase = live.ProgressPhase
	target.ProgressPercent = live.ProgressPercent
	target.CurrentScene = live.CurrentScene
	target.TotalScenes = live.TotalScenes
	target.CurrentSegment = live.CurrentSegment
	target.TotalSegments = live.TotalSegments
	target.FramesEncoded = live.FramesEncoded
	target.FramesDecoded = live.FramesDecoded
	target.FramesComposited = live.FramesComposited
	target.FFmpegSpeedX = live.FFmpegSpeedX
	target.ElapsedMS = live.ElapsedMS
	if target.StartedAt == "" {
		target.StartedAt = live.StartedAt
	}
	target.LastProgressAt = live.LastProgressAt
	target.CumulativeMetrics = live.CumulativeMetrics
	target.CanonicalAttemptEvents = live.CanonicalAttemptEvents
}

func liveAttemptStatus(live *LiveAttempt) taskattempts.AttemptStatus {
	if live == nil {
		return taskattempts.AttemptStatusRunning
	}
	// Runtime phases such as UPLOADING and FINALIZING are deliberately
	// richer than the durable AttemptStatus enum. The admin summary keeps
	// the durable wire contract and reports every eligible non-terminal
	// runtime phase as RUNNING.
	return taskattempts.AttemptStatusRunning
}

func applyExecutionLiveOverlay(summary *ExecutionSummary, attempt *AttemptSummary) {
	if summary == nil || attempt == nil || !attempt.Live {
		return
	}
	summary.AttemptID = attempt.AttemptID
	summary.WorkerID = attempt.WorkerID
	summary.Phase = attempt.Phase
	summary.Progress = &ExecutionProgress{
		Percent: attempt.ProgressPercent, Scene: attempt.CurrentScene, ScenesTotal: attempt.TotalScenes,
		Segment: attempt.CurrentSegment, SegmentsTotal: attempt.TotalSegments,
	}
	summary.LiveMetrics = &ExecutionLiveMetrics{
		ElapsedMS: attempt.ElapsedMS, FramesEncoded: attempt.FramesEncoded,
		FramesDecoded: attempt.FramesDecoded, FramesComposited: attempt.FramesComposited,
		FFmpegSpeedX: attempt.FFmpegSpeedX, CumulativeMetrics: attempt.CumulativeMetrics,
	}
	summary.LastProgressAt = attempt.LastProgressAt
}

func liveAttemptMetrics(live *LiveAttempt) *taskattempts.AttemptMetrics {
	if live == nil {
		return nil
	}
	metrics := &taskattempts.AttemptMetrics{
		AttemptID:         live.AttemptID,
		FramesEncoded:     live.FramesEncoded,
		FramesDecoded:     live.FramesDecoded,
		FramesComposited:  live.FramesComposited,
		FFmpegSpeedRatio:  live.FFmpegSpeedX,
		SceneCount:        live.TotalScenes,
		SegmentCount:      live.TotalSegments,
		CompletedSegments: live.CurrentSegment,
		WallClockSeconds:  float64(live.ElapsedMS) / 1000,
	}
	for key, value := range live.CumulativeMetrics {
		switch key {
		case "input_bytes":
			metrics.InputBytes = int64Value(value)
		case "output_bytes":
			metrics.OutputBytes = int64Value(value)
		case "bytes_from_drive":
			metrics.BytesFromDrive = int64Value(value)
		case "bytes_from_blobstore":
			metrics.BytesFromBlobstore = int64Value(value)
		case "bytes_from_local_cache":
			metrics.BytesFromLocalCache = int64Value(value)
		case "cpu_time_ms":
			metrics.CPUTimeMS = int64Value(value)
		case "gpu_time_ms":
			metrics.GPUTimeMS = int64Value(value)
		case "peak_rss_bytes":
			metrics.PeakRSSBytes = int64Value(value)
		case "peak_vram_bytes":
			metrics.PeakVRAMBytes = int64Value(value)
		case "frames_encoded":
			metrics.FramesEncoded = int64Value(value)
		case "frames_decoded":
			metrics.FramesDecoded = int64Value(value)
		case "frames_composited":
			metrics.FramesComposited = int64Value(value)
		case "ffmpeg_speed_ratio", "ffmpeg_speed_x":
			metrics.FFmpegSpeedRatio = float64Value(value)
		case "pipeline_render_ms":
			metrics.PipelineRenderMs = int64Value(value)
		case "pipeline_total_ms":
			metrics.PipelineTotalMs = int64Value(value)
		}
	}
	return metrics
}

func int64Value(value any) int64 {
	switch number := value.(type) {
	case int:
		return int64(number)
	case int64:
		return number
	case float64:
		return int64(number)
	case float32:
		return int64(number)
	default:
		return 0
	}
}

func float64Value(value any) float64 {
	switch number := value.(type) {
	case int:
		return float64(number)
	case int64:
		return float64(number)
	case float64:
		return number
	case float32:
		return float64(number)
	default:
		return 0
	}
}
