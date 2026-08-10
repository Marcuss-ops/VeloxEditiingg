package instaedit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"velox-server/internal/store"
	"velox-shared/contract"
)

// buildCreateJobPayload applies destination resolution and builds the
// canonical payload consumed by either creatorflow or enqueue.
func (s *Service) buildCreateJobPayload(ctx context.Context, cmd CreateJobCmd, renderSpec map[string]any) (map[string]any, error) {
	if _, present := renderSpec["scenes_json"]; !present {
		if scenes, present := renderSpec["scenes"]; present {
			encoded, err := json.Marshal(scenes)
			if err != nil {
				return nil, fmt.Errorf("%w: invalid scenes: %v", ErrInvalidPayload, err)
			}
			renderSpec["scenes_json"] = string(encoded)
		}
	}

	deliveryPlan := make([]map[string]any, 0, len(cmd.Destinations))
	for i, d := range cmd.Destinations {
		externalID := strings.TrimSpace(d.ExternalDestinationID)
		if externalID == "" {
			return nil, fmt.Errorf("%w: destination[%d].external_destination_id is required", ErrInvalidPayload, i)
		}
		dest, err := s.jobs.GetDeliveryDestinationByExternalID(ctx, externalID)
		if err != nil {
			if errors.Is(err, store.ErrDeliveryNoRow) {
				return nil, fmt.Errorf("%w: %s", ErrDestinationUnknown, externalID)
			}
			return nil, err
		}
		if dest == nil {
			return nil, fmt.Errorf("%w: %s", ErrDestinationUnknown, externalID)
		}
		if !dest.Enabled {
			return nil, fmt.Errorf("%w: %s", ErrDestinationDisabled, externalID)
		}

		metadata := map[string]any{}
		if len(d.Metadata) > 0 {
			if err := json.Unmarshal(d.Metadata, &metadata); err != nil {
				return nil, fmt.Errorf("%w: invalid metadata for destination[%d]: %v", ErrInvalidPayload, i, err)
			}
		}

		deliveryPlan = append(deliveryPlan, map[string]any{
			"destination_id": dest.DestinationID,
			"priority":       i,
			"retry_budget":   contract.DefaultDeliveryRetryBudget,
			"metadata":       metadata,
		})
	}

	if _, ok := renderSpec["video_name"]; !ok {
		renderSpec["video_name"] = cmd.ProjectID
	}
	renderSpec["delivery_plan"] = deliveryPlan
	if cmd.RenderOnly {
		renderSpec["render_only"] = true
	}

	typedPayload := contract.NewJobPayloadV2(renderSpec)
	payload, err := typedPayload.ToMap()
	if err != nil {
		return nil, fmt.Errorf("build canonical payload: %w", err)
	}
	payload["project_id"] = cmd.ProjectID
	if cmd.RenderOnly {
		// JobPayloadV2 intentionally omits control-plane render_only from its
		// typed projection; preserve it explicitly for enqueue/creatorflow.
		payload["render_only"] = true
	}
	// This adapter submits a fully assembled render request (unlike the
	// remote-engine polling path), so mark the canonical handoff complete
	// for the resolver's completion gate. The enqueue normalizer still owns
	// the persisted worker lifecycle status.
	// `status=completed` is the historical wire value for a fully assembled
	// input handoff, not a Job completion. Keep the wire key/value unchanged,
	// but make the semantic domain explicit at this boundary.
	if !typedPayload.SetInputAssemblyStatus(contract.InputAssemblyCompleted) {
		return nil, fmt.Errorf("set input assembly status: %w", ErrInvalidPayload)
	}
	payload, err = typedPayload.ToMap()
	if err != nil {
		return nil, fmt.Errorf("project input assembly payload: %w", err)
	}
	// These control-plane fields are intentionally outside JobPayloadV2's
	// renderer-owned projection, so restore them after the typed projection.
	payload["project_id"] = cmd.ProjectID
	if cmd.RenderOnly {
		payload["render_only"] = true
	}
	return payload, nil
}
