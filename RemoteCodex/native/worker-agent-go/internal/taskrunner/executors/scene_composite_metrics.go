package executors

import (
	"encoding/json"
	"strings"
	"time"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/video/pipeline"
)

// scene_composite_metrics.go owns the observability projection
// helpers used by SceneComposite.Execute: the category-summary phase
// rollups (appendObservabilitySummaryPhases), the dotted-metric
// flattener (flattenObservabilityMetric) and the pipeline selector
// (resolvePipelineID). The executor itself lives in scene_composite.go.

// appendObservabilitySummaryPhases projects category rollups into the same
// detailed phase stream used by TaskResult. This preserves audio, subtitle,
// I/O, quality, retry, and waste summaries even though the protobuf has no
// generic map field. The raw category values remain in Metrics as well.
func appendObservabilitySummaryPhases(phases *[]executor.DetailedPhaseTiming, values map[string]interface{}) {
	if len(values) == 0 {
		return
	}
	nextEventIndex := int64(0)
	for _, phase := range *phases {
		if phase.Origin == telemetry.OriginValidation && phase.EventIndex >= nextEventIndex {
			nextEventIndex = phase.EventIndex + 1
		}
	}
	for _, category := range []string{"audio", "subtitle", "io", "quality", "retry", "waste"} {
		value, ok := values[category]
		if !ok {
			continue
		}
		metadata, err := json.Marshal(value)
		if err != nil {
			continue
		}
		*phases = append(*phases, executor.DetailedPhaseTiming{
			Origin:       telemetry.OriginValidation,
			Scope:        telemetry.ScopeAttempt,
			Component:    category,
			Action:       "summary",
			Phase:        category,
			EventType:    "summary",
			EventName:    category,
			EventIndex:   nextEventIndex,
			Status:       telemetry.StatusOK,
			MetadataJSON: string(metadata),
		})
		nextEventIndex++
	}
}

// projectRenderProfile exposes a stable, low-cardinality timing vocabulary
// over the existing pipeline/native counters. It intentionally preserves the
// native distinction that audio mix and AAC encoding are one FFmpeg command
// today; emitting a made-up AAC-only duration would make the profile less
// truthful. The dotted keys are carried through TaskExecutionReport metrics
// and are suitable for before/after benchmark aggregation.
func projectRenderProfile(dst map[string]interface{}, run pipeline.RunMetrics, artifactStarted time.Time, artifactSHAms int64) {
	if dst == nil {
		return
	}
	dst["render_profile.compile_plan_ms"] = run.CompileMs
	dst["render_profile.render_ms"] = run.RenderMs
	dst["render_profile.native_total_ms"] = run.RenderMetrics.TotalMs
	dst["render_profile.artifact_sha_ms"] = artifactSHAms
	dst["render_profile.artifact_total_ms"] = time.Since(artifactStarted).Milliseconds()

	for _, field := range []struct {
		name string
		key  string
	}{
		{name: "asset_download_ms", key: "asset_download_ms"},
		{name: "audio_download_ms", key: "audio_download_ms"},
		{name: "audio_prepare_ms", key: "audio_prepare_ms"},
		{name: "audio_mix_encode_ms", key: "mix_audio_ms"},
		{name: "mux_ms", key: "mux_audio_ms"},
	} {
		if value, ok := run.RenderMetrics.PhaseMS[field.key]; ok {
			dst["render_profile."+field.name] = value
		}
	}
}

// flattenObservabilityMetric keeps category summaries in the legacy dotted
// metric map so TaskRunner can project them into typed execution_metrics.
// Nested JSON objects are flattened without losing audio/subtitle/I/O/quality
// category names; scalar values retain their JSON-decoded types.
func flattenObservabilityMetric(dst map[string]interface{}, prefix string, value interface{}) {
	if prefix == "" {
		return
	}
	if nested, ok := value.(map[string]interface{}); ok {
		for key, child := range nested {
			name := key
			if prefix != "" {
				name = prefix + "." + key
			}
			flattenObservabilityMetric(dst, name, child)
		}
		return
	}
	dst[prefix] = value
}

func resolvePipelineID(payload map[string]interface{}) string {
	if payload != nil {
		if pipelineID, _ := payload["pipeline_id"].(string); strings.TrimSpace(pipelineID) != "" {
			return strings.TrimSpace(pipelineID)
		}
	}
	return "hybrid.v1"
}
