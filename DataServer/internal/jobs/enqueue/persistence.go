// Package enqueue — persistence.go (Stage 3 of the enqueue split).
//
// compileSceneVideoJob is the third phase of the Enqueuer's pipeline:
// it converts a *normalized* scene-video payload into the canonical
// (*jobs.Job, *taskgraph.TaskSpec, priority) triple the orchestrator
// (enqueue.go) hands to the atomic-creation path. The caller still
// owns the insert; this file just owns canonical-entity construction.
//
// Filename note: "persistence.go" is the Stage-3 routing label per the
// §15.6 + §16.7 forward-map re-routing. It is NOT a claim that
// compileSceneVideoJob performs I/O — see docs/metrics/loc-baseline.md
// §17 for the R2 delta. Pairs with the §15.7 "smaller atomic step" rule.
//
// Imports mirror the EXACT set compileSceneVideoJob references, derived
// from the canonical pre-extraction body (git HEAD's lines 234-278 of
// enqueue.go). The import block is also a defence against drift: if the
// body ever grows new symbol references, the build will surface an
// unimported-symbol error before the import block silently rots.
package enqueue

import (
	"context"
	"encoding/json"
	"fmt"

	"velox-server/internal/costmodel"
	"velox-server/internal/jobs"
	"velox-server/internal/taskgraph"
	"velox-server/internal/telemetry"
	"velox-shared/contract"
	"velox-shared/contract/deliveryplan"
	"velox-shared/payload"
)

// compileSceneVideoJobContext builds a canonical *jobs.Job and
// *taskgraph.TaskSpec from a normalized scene-video payload. The caller
// owns the atomic creation.
func compileSceneVideoJobContext(ctx context.Context, normalized map[string]interface{}, req costmodel.JobRequirements) (*jobs.Job, *taskgraph.TaskSpec, int, error) {
	jobID, _ := normalized["job_id"].(string)
	videoName, _ := normalized["video_name"].(string)
	projectID, _ := normalized["project_id"].(string)
	jobRunID, _ := normalized["job_run_id"].(string)
	if jobRunID == "" {
		jobRunID, _ = normalized["run_id"].(string)
	}
	jobType, _ := normalized["job_type"].(string)
	if jobType == "" {
		jobType = "process_video"
	}
	priority := payload.EnsureInt(normalized["priority"], 5)

	publicationSpecs := normalizedPublicationSpecs(normalized)

	rendererPayload, err := contract.RenderOnlyPayload(normalized)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("marshal renderer payload: %w", err)
	}
	telemetry.RecordEnqueueJSONMarshal(ctx)
	raw, err := json.Marshal(rendererPayload)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("marshal renderer payload: %w", err)
	}

	job := &jobs.Job{
		ID:        jobID,
		Type:      jobType,
		Status:    jobs.StatusPending,
		VideoName: videoName,
		ProjectID: projectID,
		RunID:     jobRunID,
		// MaxRetries is set by extractPlanMaxRetry (single writer on
		// the insert path). Left at 0 here so the owner is explicit.
		MaxRetries:   0,
		Payload:      string(raw),
		Requirements: req,
	}

	executorID := "scene.composite.v1"
	if resolved := resolveInternalExecutorID(normalized); resolved != "" {
		executorID = resolved
	}

	spec := &taskgraph.TaskSpec{
		Version:    taskgraph.SpecVersion,
		JobID:      jobID,
		ExecutorID: executorID,
		Payload:    rendererPayload,
		// DeliveryPlan and PublicationSpecs are control-plane data. Capture
		// them before the renderer projection removes routing/publication
		// keys from Payload.
		DeliveryPlan:         deliveryplan.ExtractEnvelope(normalized),
		PublicationSpecs:     publicationSpecs,
		RequiredCapabilities: resolveRequiredCapabilities(executorID),
	}

	return job, spec, priority, nil
}

// normalizedPublicationSpecs extracts publication intents into the
// control-plane TaskSpec field. Publication metadata never belongs in the
// renderer payload, even when a producer supplied it in the temporary
// normalized envelope.
func normalizedPublicationSpecs(normalized map[string]interface{}) []map[string]interface{} {
	if normalized == nil {
		return nil
	}
	for _, key := range []string{"publication_specs", "publications"} {
		raw, ok := normalized[key]
		if !ok || raw == nil {
			continue
		}
		var specs []map[string]interface{}
		switch values := raw.(type) {
		case []map[string]interface{}:
			specs = values
		case []interface{}:
			for _, value := range values {
				if spec, ok := value.(map[string]interface{}); ok {
					specs = append(specs, spec)
				}
			}
		case map[string]interface{}:
			specs = []map[string]interface{}{values}
		}
		if len(specs) > 0 {
			return clonePublicationSpecMaps(specs)
		}
	}
	return nil
}

func clonePublicationSpecMaps(specs []map[string]interface{}) []map[string]interface{} {
	out := make([]map[string]interface{}, len(specs))
	for index, spec := range specs {
		out[index] = clonePublicationSpecValue(spec).(map[string]interface{})
	}
	return out
}

func clonePublicationSpecValue(value interface{}) interface{} {
	switch typed := value.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(typed))
		for key, child := range typed {
			out[key] = clonePublicationSpecValue(child)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(typed))
		for index, child := range typed {
			out[index] = clonePublicationSpecValue(child)
		}
		return out
	case []map[string]interface{}:
		out := make([]map[string]interface{}, len(typed))
		for index, child := range typed {
			out[index] = clonePublicationSpecValue(child).(map[string]interface{})
		}
		return out
	default:
		return value
	}
}

// cloneRendererPayload creates the final worker-facing map from the
// normalized canonical map. Delivery routing and publication metadata are
// consumed by the control plane and are deliberately absent here.
func cloneRendererPayload(normalized map[string]interface{}) (map[string]interface{}, error) {
	return contract.RenderOnlyPayload(normalized)
}
