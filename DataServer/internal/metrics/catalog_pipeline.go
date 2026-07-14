// Package metrics / catalog_pipeline.go
//
// Pipeline family — metrics for the Go-side pipeline orchestrator
// (pipeline.*) plus the C++ native subprocess wrapper (native.*).
// Pipeline metrics cover the resolve/validate/compile/render phases
// of the Go orchestrator; native metrics cover the subprocess
// lifecycle (plan-write, process wait, total).
package metrics

// pipelineMetricDefinitions returns pipeline.* + native.* definitions.
// Pipeline first (orchestrator phases), then native (subprocess timing).
func pipelineMetricDefinitions() []MetricDefinition {
	return []MetricDefinition{
		// ── Pipeline phases (Go pipeline runner) ─────────────────────────
		{
			Name: "pipeline.id", Unit: "string", Component: CompPipeline, Kind: KindGauge,
			Description: "Pipeline identifier selected for this task (e.g. hybrid.v1)",
		},
		{
			Name: "pipeline.resolve_ms", Unit: "ms", Component: CompPipeline, Kind: KindHistogram,
			Description: "Time spent resolving the pipeline for the given task spec",
		},
		{
			Name: "pipeline.validate_ms", Unit: "ms", Component: CompPipeline, Kind: KindHistogram,
			Description: "Time spent validating pipeline input parameters",
		},
		{
			Name: "pipeline.compile_ms", Unit: "ms", Component: CompPipeline, Kind: KindHistogram,
			Description: "Time spent compiling the render plan from the pipeline spec",
		},
		{
			Name: "pipeline.render_ms", Unit: "ms", Component: CompPipeline, Kind: KindHistogram,
			Description: "Time spent in the render phase (orchestrating the C++ engine)",
		},
		{
			Name: "pipeline.total_ms", Unit: "ms", Component: CompPipeline, Kind: KindHistogram,
			Description: "Total wall-clock time spent in the pipeline (resolve + compile + render)",
		},
		{
			Name: "pipeline.timeline_items", Unit: "items", Component: CompPipeline, Kind: KindCounter,
			Description: "Number of timeline items (clips, images, entities) in the render plan",
		},
		{
			Name: "pipeline.audio_tracks", Unit: "tracks", Component: CompPipeline, Kind: KindCounter,
			Description: "Number of audio tracks processed in the pipeline",
		},
		// ── Native process (C++ engine subprocess) ───────────────────────
		{
			Name: "native.total_ms", Unit: "ms", Component: CompNative, Kind: KindHistogram,
			Description: "Total time the native C++ engine process was running",
		},
		{
			Name: "native.plan_write_ms", Unit: "ms", Component: CompNative, Kind: KindHistogram,
			Description: "Time spent writing the render plan to disk for the native process",
		},
		{
			Name: "native.process_wait_ms", Unit: "ms", Component: CompNative, Kind: KindHistogram,
			Description: "Time spent waiting for the native C++ engine process to exit",
		},
	}
}
