package executors

import (
	"encoding/json"
	"strings"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/telemetry"
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
