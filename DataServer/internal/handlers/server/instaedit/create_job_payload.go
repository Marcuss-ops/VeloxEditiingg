package instaedit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"velox-server/internal/storecore"
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

	destinations := cmd.Destinations
	if len(cmd.Publications) > 0 && !cmd.RenderOnly {
		publicationDestinations, err := destinationsFromPublications(cmd.Publications)
		if err != nil {
			return nil, err
		}
		// Publications are the authoritative fan-out contract. This is
		// important when a compatibility client also sends a legacy top-level
		// delivery_plan: using that list would collapse multiple languages
		// targeting the same channel into one delivery.
		destinations = publicationDestinations
	}
	deliveryPlan := make([]map[string]any, 0, len(destinations))
	for i, d := range destinations {
		externalID := strings.TrimSpace(d.ExternalDestinationID)
		if externalID == "" {
			return nil, fmt.Errorf("%w: destination[%d].external_destination_id is required", ErrInvalidPayload, i)
		}
		dest, err := s.jobs.GetDeliveryDestinationByExternalID(ctx, externalID)
		if err != nil {
			if errors.Is(err, storecore.ErrDeliveryNoRow) {
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
		if cmd.PublishAt != "" {
			metadata["publish_at"] = cmd.PublishAt
		}
		if cmd.Target != nil {
			metadata["target_type"] = cmd.Target.Type
			if cmd.Target.ChannelID != "" {
				metadata["channel_id"] = cmd.Target.ChannelID
			}
			if cmd.Target.ChannelName != "" {
				metadata["channel_name"] = cmd.Target.ChannelName
			}
			if cmd.Target.GroupID != 0 {
				metadata["group_id"] = cmd.Target.GroupID
			}
			if cmd.Target.GroupName != "" {
				metadata["group_name"] = cmd.Target.GroupName
			}
		}
		if variantID := strings.TrimSpace(d.VariantID); variantID != "" {
			metadata["output_variant_id"] = variantID
		}

		deliveryPlan = append(deliveryPlan, map[string]any{
			"destination_id": dest.DestinationID,
			"publication_id": strings.TrimSpace(d.PublicationID),
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

	typedPayload, err := contract.NewJobPayloadV2Checked(renderSpec)
	if err != nil {
		return nil, fmt.Errorf("build canonical payload: %w", err)
	}
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
	if len(cmd.Publications) > 0 {
		var publications []any
		if err := json.Unmarshal(cmd.Publications, &publications); err != nil || len(publications) == 0 {
			return nil, fmt.Errorf("%w: publications must be a non-empty JSON array", ErrInvalidPayload)
		}
		// Publications are control-plane data. The enqueue/creatorflow
		// normalizer persists them as publication specs and removes them from
		// the renderer-owned payload before execution.
		payload["publications"] = publications
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
	if len(cmd.Publications) > 0 {
		var publications []any
		if err := json.Unmarshal(cmd.Publications, &publications); err != nil || len(publications) == 0 {
			return nil, fmt.Errorf("%w: publications must be a non-empty JSON array", ErrInvalidPayload)
		}
		// Restore control-plane publication intents after the renderer-owned
		// typed projection has been rebuilt.
		payload["publications"] = publications
	}
	return payload, nil
}

// destinationsFromPublications lets a language bundle be submitted with one
// source of truth: publications[].destinations[].destination_id. The legacy
// top-level delivery_plan remains accepted only when publications are absent.
func destinationsFromPublications(raw json.RawMessage) ([]CreateDestinationCmd, error) {
	var publications []struct {
		PublicationID string `json:"publication_id"`
		Language      string `json:"language"`
		OutputRef     struct {
			VariantID    string `json:"variant_id"`
			ArtifactRole string `json:"artifact_role"`
		} `json:"output_ref"`
		Metadata     json.RawMessage `json:"metadata"`
		Destinations []struct {
			DestinationID    string          `json:"destination_id"`
			MetadataOverride json.RawMessage `json:"metadata_override"`
		} `json:"destinations"`
	}
	if err := json.Unmarshal(raw, &publications); err != nil || len(publications) == 0 {
		return nil, fmt.Errorf("%w: publications must be a non-empty JSON array", ErrInvalidPayload)
	}
	seen := make(map[string]struct{})
	out := make([]CreateDestinationCmd, 0)
	for publicationIndex, publication := range publications {
		publicationID := strings.TrimSpace(publication.PublicationID)
		for destinationIndex, destination := range publication.Destinations {
			externalID := strings.TrimSpace(destination.DestinationID)
			if externalID == "" {
				return nil, fmt.Errorf("%w: publications[%d].destinations[%d].destination_id is required", ErrInvalidPayload, publicationIndex, destinationIndex)
			}
			pairKey := publicationID + "\x00" + externalID
			if _, exists := seen[pairKey]; exists {
				continue
			}
			seen[pairKey] = struct{}{}
			metadataMap := make(map[string]any)
			if len(publication.Metadata) > 0 && string(publication.Metadata) != "null" {
				if err := json.Unmarshal(publication.Metadata, &metadataMap); err != nil {
					return nil, fmt.Errorf("%w: publications[%d].metadata must be a JSON object", ErrInvalidPayload, publicationIndex)
				}
			}
			if len(destination.MetadataOverride) > 0 && string(destination.MetadataOverride) != "null" {
				override := make(map[string]any)
				if err := json.Unmarshal(destination.MetadataOverride, &override); err != nil {
					return nil, fmt.Errorf("%w: publications[%d].destinations[%d].metadata_override must be a JSON object", ErrInvalidPayload, publicationIndex, destinationIndex)
				}
				for key, value := range override {
					metadataMap[key] = value
				}
			}
			if publicationID != "" {
				metadataMap["publication_id"] = publicationID
			}
			if language := strings.TrimSpace(publication.Language); language != "" {
				metadataMap["language"] = language
			}
			if variantID := strings.TrimSpace(publication.OutputRef.VariantID); variantID != "" {
				metadataMap["output_variant_id"] = variantID
			} else if artifactRole := strings.TrimSpace(publication.OutputRef.ArtifactRole); artifactRole != "" {
				metadataMap["output_artifact_role"] = artifactRole
			}
			metadata, err := json.Marshal(metadataMap)
			if err != nil {
				return nil, fmt.Errorf("%w: publications[%d] metadata cannot be serialized", ErrInvalidPayload, publicationIndex)
			}
			out = append(out, CreateDestinationCmd{
				ExternalDestinationID: externalID,
				PublicationID:         publicationID,
				VariantID:             strings.TrimSpace(publication.OutputRef.VariantID),
				Metadata:              metadata,
			})
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: publications must contain at least one destination", ErrInvalidPayload)
	}
	return out, nil
}
