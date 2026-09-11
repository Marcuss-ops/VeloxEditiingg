package executors

import (
	"encoding/json"
	"strings"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/telemetry"
)

// appendObservabilitySummaryPhases projects category rollups into the typed
// detailed phase stream. The protobuf has no generic map field, so this keeps
// the summary visible without maintaining a second metrics projection.
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
			Origin: telemetry.OriginValidation, Scope: telemetry.ScopeAttempt,
			Component: category, Action: "summary", Phase: category,
			EventType: "summary", EventName: category, EventIndex: nextEventIndex,
			Status: telemetry.StatusOK, MetadataJSON: string(metadata),
		})
		nextEventIndex++
	}
}

func resolvePipelineID(payload map[string]interface{}) string {
	if payload != nil {
		if pipelineID, _ := payload["pipeline_id"].(string); strings.TrimSpace(pipelineID) != "" {
			return strings.TrimSpace(pipelineID)
		}
	}
	return ""
}
