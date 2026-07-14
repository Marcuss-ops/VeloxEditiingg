// Package metrics / catalog_media.go
//
// Media family — metrics for the FFmpeg subprocess (ffmpeg.*) and the
// video-encoding path as a whole (video.*). FFmpeg entries come from
// the -progress parser; video entries are higher-level aggregates
// (encode passes, frames encoded, stream-copy vs re-encode).
package metrics

// mediaMetricDefinitions returns ffmpeg.* + video.* definitions.
// FFmpeg first (subprocess-level telemetry), then video (higher-level
// aggregates covering the whole encoding path).
func mediaMetricDefinitions() []MetricDefinition {
	return []MetricDefinition{
		// ── FFmpeg metrics ───────────────────────────────────────────────
		{
			Name: "ffmpeg.speed_ratio", Unit: "ratio", Component: CompFFmpeg, Kind: KindGauge,
			Description: "FFmpeg encoding speed vs realtime (>1 means faster than realtime)",
		},
		{
			Name: "ffmpeg.fps", Unit: "fps", Component: CompFFmpeg, Kind: KindGauge,
			Description: "Last-observed FFmpeg frames-per-second encoding rate",
		},
		{
			Name: "ffmpeg.frames_processed", Unit: "count", Component: CompFFmpeg, Kind: KindCounter,
			Description: "Total frames processed by FFmpeg as observed from -progress",
		},
		{
			Name: "ffmpeg.encode_duration_ms", Unit: "ms", Component: CompFFmpeg, Kind: KindHistogram,
			Description: "FFmpeg encode duration per segment or pass",
		},
		{
			Name: "ffmpeg.decode_duration_ms", Unit: "ms", Component: CompFFmpeg, Kind: KindHistogram,
			Description: "FFmpeg decode duration per segment",
		},
		{
			Name: "ffmpeg.dropped_frames", Unit: "count", Component: CompFFmpeg, Kind: KindCounter,
			Description: "Dropped frames as observed from FFmpeg -progress",
		},
		{
			Name: "ffmpeg.duplicated_frames", Unit: "count", Component: CompFFmpeg, Kind: KindCounter,
			Description: "Duplicated frames as observed from FFmpeg -progress",
		},
		{
			Name: "ffmpeg.exit_code", Unit: "code", Component: CompFFmpeg, Kind: KindCounter,
			Description: "FFmpeg process exit codes by value",
		},
		{
			Name: "ffmpeg.restarts", Unit: "count", Component: CompFFmpeg, Kind: KindCounter,
			Description: "FFmpeg process restart count",
		},
		{
			Name: "ffmpeg.processes_active", Unit: "count", Component: CompFFmpeg, Kind: KindGauge,
			Description: "Number of currently active FFmpeg processes",
		},
		// ── Video metrics ────────────────────────────────────────────────
		{
			Name: "video.encode_passes", Unit: "count", Component: CompVideo, Kind: KindCounter,
			Description: "Total encode passes performed (1 for single-pass, 2 for two-pass)",
		},
		{
			Name: "video.frames_encoded", Unit: "count", Component: CompVideo, Kind: KindCounter,
			Description: "Total frames encoded across all passes",
		},
		{
			Name: "video.output_frames", Unit: "count", Component: CompVideo, Kind: KindCounter,
			Description: "Output frames published (lower-bound dedup of frames_encoded)",
		},
		{
			Name: "video.stream_copy_operations", Unit: "count", Component: CompVideo, Kind: KindCounter,
			Description: "Stream-copy concat operations (cheap path, no re-encoding)",
		},
		{
			Name: "video.reencode_operations", Unit: "count", Component: CompVideo, Kind: KindCounter,
			Description: "Re-encode concat operations (expensive path, resolution mismatch etc.)",
		},
	}
}
