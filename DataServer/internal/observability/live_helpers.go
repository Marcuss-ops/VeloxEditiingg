package observability

import "velox-server/internal/taskattempts"

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
