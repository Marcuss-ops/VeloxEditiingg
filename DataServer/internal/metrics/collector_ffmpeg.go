// Package metrics / collector_ffmpeg.go
//
// FFmpeg progress/exit families, sliced out of collector.go so the
// Collector struct definition stays focused on registration.
package metrics

// initFFmpegFamilies creates the FFmpeg -progress observation
// families. Called once from NewCollector at boot.
func (c *Collector) initFFmpegFamilies() {
	c.ffmpegFramesTotal = NewCounterFamily("velox_ffmpeg_frames_processed_total",
		"Total frames processed by FFmpeg as observed from -progress", []string{"executor_id"})
	c.ffmpegFps = NewGaugeFamily("velox_ffmpeg_fps",
		"Last-observed FFmpeg fps", []string{"executor_id"})
	c.ffmpegSpeed = NewGaugeFamily("velox_ffmpeg_speed_ratio",
		"Last-observed FFmpeg speed vs realtime (>1 faster)", []string{"executor_id"})
	c.ffmpegEncodeMs = NewHistogramFamily("velox_ffmpeg_encode_duration_seconds",
		"Encode duration as observed", []string{"executor_id"},
		[]float64{0.5, 1, 2.5, 5, 10, 30, 60, 300})
	c.ffmpegDecodeMs = NewHistogramFamily("velox_ffmpeg_decode_duration_seconds",
		"Decode duration as observed", []string{"executor_id"},
		[]float64{0.25, 0.5, 1, 2.5, 5, 10, 30, 60})
	c.ffmpegDropped = NewCounterFamily("velox_ffmpeg_dropped_frames_total",
		"Dropped frames as observed", []string{"executor_id"})
	c.ffmpegDuplicated = NewCounterFamily("velox_ffmpeg_duplicated_frames_total",
		"Duplicated frames as observed", []string{"executor_id"})
	c.ffmpegExits = NewCounterFamily("velox_ffmpeg_exit_total",
		"FFmpeg process exits by exit code", []string{"executor_id", "exit_code"})
	c.ffmpegRestarts = NewCounterFamily("velox_ffmpeg_restarts_total",
		"FFmpeg process restarts", []string{"executor_id"})
	c.ffmpegProcessesAct = NewGaugeFamily("velox_ffmpeg_processes_active",
		"Currently running FFmpeg processes", []string{"executor_id"})
}

// ffmpegFamilies returns the FFmpeg subset registered by NewCollector
// via allFamilies.
func (c *Collector) ffmpegFamilies() []*Family {
	return []*Family{
		c.ffmpegFramesTotal, c.ffmpegFps, c.ffmpegSpeed,
		c.ffmpegEncodeMs, c.ffmpegDecodeMs,
		c.ffmpegDropped, c.ffmpegDuplicated, c.ffmpegExits,
		c.ffmpegRestarts, c.ffmpegProcessesAct,
	}
}
