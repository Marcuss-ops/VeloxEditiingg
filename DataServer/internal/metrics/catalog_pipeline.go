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
		// ── Creator intake adoption (Blocco 5 step #4) ────────────────────
		// Bounded label set: only "path" (values "creator_push",
		// "creator_forwarder", and "remote_engine_legacy"). High-cardinality
		// labels (source_provider, source_job_id) belong in structured logs,
		// not in metrics.
		{
			Name: "pipeline.creator_intake_accepted_total", Unit: "count", Component: CompPipeline, Kind: KindCounter,
			Description: "Total number of creator payloads accepted by the master, split by intake path. Label cardinality is bounded — only 'creator_push' (HTTP endpoint /api/v1/creator/jobs) and 'creator_forwarder' (async CreatorForwardingRunner) are valid values. The 'remote_engine_legacy' label was retired when /api/remote/pipeline was fully removed from main (see docs/CREATOR-PUSH.md §Removal).",
		},
		{
			Name: "pipeline.compat_alias_reads_total", Unit: "count", Component: CompPipeline, Kind: KindCounter,
			Description: "Reads of registered legacy payload aliases, labeled by alias and canonical key. Values are bounded by the shared compatibility registry.",
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
