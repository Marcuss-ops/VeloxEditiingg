// Package metrics / catalog_input.go
//
// Input family — static attributes of the input task (scene count,
// segment count, resolution, fps, track/subtitle counts). These
// describe the *shape* of the input, not its processing.
package metrics

// inputMetricDefinitions returns input.* definitions in stable order:
// structure first (scenes/segments/duration), then format (resolution/fps),
// then tracks (audio/subtitle counts).
func inputMetricDefinitions() []MetricDefinition {
	return []MetricDefinition{
		// ── Task-level input context ─────────────────────────────────────
		{
			Name: "input.scene_count", Unit: "count", Component: CompInput, Kind: KindCounter,
			Description: "Number of scenes in the input timeline",
		},
		{
			Name: "input.segment_count", Unit: "count", Component: CompInput, Kind: KindCounter,
			Description: "Number of timeline segments in the render plan",
		},
		{
			Name: "input.total_duration_sec", Unit: "seconds", Component: CompInput, Kind: KindGauge,
			Description: "Total input media duration in seconds",
		},
		{
			Name: "input.resolution_width", Unit: "pixels", Component: CompInput, Kind: KindGauge,
			Description: "Output resolution width in pixels",
		},
		{
			Name: "input.resolution_height", Unit: "pixels", Component: CompInput, Kind: KindGauge,
			Description: "Output resolution height in pixels",
		},
		{
			Name: "input.fps", Unit: "fps", Component: CompInput, Kind: KindGauge,
			Description: "Output frames per second",
		},
		{
			Name: "input.audio_track_count", Unit: "tracks", Component: CompInput, Kind: KindCounter,
			Description: "Number of audio tracks in the input",
		},
		{
			Name: "input.subtitle_count", Unit: "count", Component: CompInput, Kind: KindCounter,
			Description: "Number of subtitle tracks in the input",
		},
	}
}
