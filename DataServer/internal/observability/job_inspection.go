package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// InspectJob composes the canonical job aggregate with the execution
// waterfall and the persistence-backed event/artifact/delivery projections.
// It is deliberately read-only and bounded by the adapter-provided limits.
func (s *Service) InspectJob(ctx context.Context, jobID string) (*JobInspection, error) {
	if s == nil || s.jobs == nil {
		return nil, fmt.Errorf("observability: job reader not configured")
	}
	if jobID == "" {
		return nil, fmt.Errorf("observability: job id is required")
	}
	job, err := s.jobs.Get(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("observability: get job: %w", err)
	}
	if job == nil {
		return nil, fmt.Errorf("observability: job %s not found", jobID)
	}
	execution, err := s.SummarizeJob(ctx, jobID)
	if err != nil {
		// Some legacy jobs predate the task graph. Keep their lifecycle,
		// events, artifacts, and deliveries inspectable even when the
		// execution waterfall does not exist.
		if !strings.Contains(err.Error(), "no task for job") {
			return nil, err
		}
	}
	result := &JobInspection{
		Job: job, Execution: execution,
		Events: make([]JobEvent, 0), Artifacts: make([]ArtifactSnapshot, 0),
		Deliveries: make([]DeliverySnapshot, 0),
	}
	if s.jobInspection == nil {
		return result, nil
	}
	if events, eventErr := s.jobInspection.ListJobEvents(ctx, jobID, 200); eventErr == nil {
		result.Events = events
		sort.SliceStable(result.Events, func(i, j int) bool {
			return result.Events[i].Timestamp < result.Events[j].Timestamp
		})
	} else {
		return nil, fmt.Errorf("observability: list job events: %w", eventErr)
	}
	if artifacts, artifactErr := s.jobInspection.ListArtifacts(ctx, jobID, 100); artifactErr == nil {
		result.Artifacts = artifacts
	} else {
		return nil, fmt.Errorf("observability: list artifacts: %w", artifactErr)
	}
	if deliveries, deliveryErr := s.jobInspection.ListDeliveries(ctx, jobID); deliveryErr == nil {
		result.Deliveries = deliveries
	} else {
		return nil, fmt.Errorf("observability: list deliveries: %w", deliveryErr)
	}
	return result, nil
}

// DecodeJobEventPayload is shared by the composition-root adapter and tests.
// Invalid legacy payloads remain observable instead of being silently lost.
func DecodeJobEventPayload(raw string) map[string]any {
	if raw == "" {
		return nil
	}
	var payload map[string]any
	if json.Unmarshal([]byte(raw), &payload) != nil {
		return map[string]any{"_raw": raw}
	}
	return payload
}
