// Package metrics / catalog_engine.go
//
// Engine family — every metric emitted by the C++ engine sidecar.
// These are the canonical "where did the time go" / "what was the
// encoding state" metrics for the rendering hot path.
//
// Naming: "engine.<phase>_<unit>" for phase durations,
// "engine.<telemetry>" for instantaneous gauges/counters.
package metrics

// engineMetricDefinitions returns every engine.* MetricDefinition in a
// stable order (phase durations first, then encode telemetry, then
// concat/stream metadata). The order is not semantically required —
// the central assembler folds them into a map — but it keeps diffs
// reviewable when entries are added or modified.
func engineMetricDefinitions() []MetricDefinition {
	return []MetricDefinition{
		// ── Engine phases (C++ sidecar) ──────────────────────────────────
		{
			Name: "engine.asset_download_ms", Unit: "ms", Component: CompEngine, Kind: KindHistogram,
			Description: "Time spent downloading assets (clips, images, audio) in the C++ engine",
		},
		{
			Name: "engine.segment_build_ms", Unit: "ms", Component: CompEngine, Kind: KindHistogram,
			Description: "Time spent building video segments (FFmpeg encode per segment) in the C++ engine",
		},
		{
			Name: "engine.concat_ms", Unit: "ms", Component: CompEngine, Kind: KindHistogram,
			Description: "Time spent concatenating segments into the final output",
		},
		{
			Name: "engine.mux_audio_ms", Unit: "ms", Component: CompEngine, Kind: KindHistogram,
			Description: "Time spent muxing audio tracks into the final output",
		},
		{
			Name: "engine.copy_final_ms", Unit: "ms", Component: CompEngine, Kind: KindHistogram,
			Description: "Time spent copying the final rendered file to the output destination",
		},
		{
			Name: "engine.audio_download_ms", Unit: "ms", Component: CompEngine, Kind: KindHistogram,
			Description: "Time spent downloading audio-only assets",
		},
		{
			Name: "engine.segment_duration_ms", Unit: "ms", Component: CompEngine, Kind: KindHistogram,
			Description: "Per-segment duration from the C++ engine sidecar segments[] array",
		},
		// ── Encoding telemetry ───────────────────────────────────────────
		{
			Name: "engine.frames", Unit: "count", Component: CompEngine, Kind: KindCounter,
			Description: "Total frames processed by the C++ engine in this attempt",
		},
		{
			Name: "engine.fps", Unit: "fps", Component: CompEngine, Kind: KindGauge,
			Description: "Last-observed frames-per-second from the C++ engine",
		},
		{
			Name: "engine.speed_x", Unit: "ratio", Component: CompEngine, Kind: KindGauge,
			Description: "Render speed multiplier vs realtime (>1 means faster than realtime)",
		},
		{
			Name: "engine.encode_passes", Unit: "count", Component: CompEngine, Kind: KindCounter,
			Description: "Number of encode passes performed by the C++ engine",
		},
		{
			Name: "engine.temp_bytes", Unit: "bytes", Component: CompEngine, Kind: KindGauge,
			Description: "Temporary bytes written by the C++ engine during rendering",
		},
		{
			Name: "engine.duration_seconds", Unit: "seconds", Component: CompEngine, Kind: KindGauge,
			Description: "Total media duration processed by the C++ engine",
		},
		{
			Name: "engine.bitrate", Unit: "bps", Component: CompEngine, Kind: KindGauge,
			Description: "Output bitrate from the C++ engine",
		},
		{
			Name: "engine.dup_frames", Unit: "count", Component: CompEngine, Kind: KindCounter,
			Description: "Duplicate frames detected during encoding",
		},
		{
			Name: "engine.drop_frames", Unit: "count", Component: CompEngine, Kind: KindCounter,
			Description: "Dropped frames during encoding",
		},
		// ── Concat / stream metadata ─────────────────────────────────────
		{
			Name: "engine.concat_mode", Unit: "string", Component: CompEngine, Kind: KindGauge,
			Description: "Concat mode used (stream_copy, reencode, or n/a)",
		},
	}
}
