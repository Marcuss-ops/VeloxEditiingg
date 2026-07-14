// Package metrics / catalog_output.go
//
// Output family — every metric emitted after the render finishes, used
// to validate the produced file (size, hash, ffprobe pass/fail, audio
// sync offset, black-frame ratio).
package metrics

// outputMetricDefinitions returns output.* definitions in stable order:
// byte-level (size, hash) first, then stream-presence (ffprobe), then
// quality signals (sync offset, black-frame ratio).
func outputMetricDefinitions() []MetricDefinition {
	return []MetricDefinition{
		{
			Name: "output.bytes", Unit: "bytes", Component: CompOutput, Kind: KindCounter,
			Description: "Total output bytes produced by this attempt",
		},
		{
			Name: "output.hash_ms", Unit: "ms", Component: CompOutput, Kind: KindHistogram,
			Description: "Time spent computing SHA-256 hash of the output file",
		},
		{
			Name: "output.file_size", Unit: "bytes", Component: CompOutput, Kind: KindGauge,
			Description: "File size of the rendered output in bytes",
		},
		{
			Name: "output.ffprobe_valid", Unit: "boolean", Component: CompOutput, Kind: KindGauge,
			Description: "Whether ffprobe validation passed on the output file",
		},
		{
			Name: "output.has_video_stream", Unit: "boolean", Component: CompOutput, Kind: KindGauge,
			Description: "Whether the output contains a video stream",
		},
		{
			Name: "output.has_audio_stream", Unit: "boolean", Component: CompOutput, Kind: KindGauge,
			Description: "Whether the output contains an audio stream",
		},
		{
			Name: "output.duration_diff_sec", Unit: "seconds", Component: CompOutput, Kind: KindGauge,
			Description: "Absolute difference between expected and actual output duration",
		},
		{
			Name: "output.audio_sync_offset_ms", Unit: "ms", Component: CompOutput, Kind: KindGauge,
			Description: "Audio-video sync offset measured in the output",
		},
		{
			Name: "output.black_frame_ratio", Unit: "ratio", Component: CompOutput, Kind: KindGauge,
			Description: "Ratio of black frames detected in the output (quality check)",
		},
	}
}
