package instaedit

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"velox-shared/contract"
)

// validateCreateJobCommand validates the request fields and returns the
// strictly validated render specification for payload construction.
func validateCreateJobCommand(cmd CreateJobCmd) (map[string]any, error) {
	if strings.TrimSpace(cmd.ProjectID) == "" {
		return nil, fmt.Errorf("%w: project_id is required", ErrInvalidPayload)
	}
	if !cmd.RenderOnly && len(cmd.Destinations) == 0 && len(cmd.Publications) == 0 {
		return nil, fmt.Errorf("%w: delivery_plan.destinations or publications is required unless render_only=true", ErrInvalidPayload)
	}
	if strings.TrimSpace(cmd.PublishAt) != "" {
		publishAt, err := time.Parse(time.RFC3339, strings.TrimSpace(cmd.PublishAt))
		if err != nil {
			return nil, fmt.Errorf("%w: publish_at must be RFC3339", ErrInvalidPayload)
		}
		if !publishAt.After(time.Now().UTC()) {
			return nil, fmt.Errorf("%w: publish_at must be in the future", ErrInvalidPayload)
		}
	}
	if cmd.Target != nil {
		if err := validatePublicationTarget(*cmd.Target); err != nil {
			return nil, err
		}
	}
	if len(cmd.Publications) > 0 {
		if err := validatePublicationBundle(cmd.Publications); err != nil {
			return nil, err
		}
	}

	var renderSpec map[string]any
	if len(cmd.RenderSpec) > 0 {
		if err := json.Unmarshal(cmd.RenderSpec, &renderSpec); err != nil {
			return nil, fmt.Errorf("%w: invalid render_spec JSON: %v", ErrBadRequest, err)
		}
	} else {
		renderSpec = map[string]any{}
	}

	if err := contract.StrictValidatePayload(renderSpec); err != nil {
		return nil, fmt.Errorf("%w: invalid render_spec: %v", ErrInvalidPayload, err)
	}
	return renderSpec, nil
}

// validatePublicationBundle validates the control-plane fan-out contract
// before it is flattened into delivery_plan entries. Keeping this validation
// here prevents a malformed language bundle from becoming a partially
// routable job.
func validatePublicationBundle(raw json.RawMessage) error {
	type publicationDestination struct {
		DestinationID    string          `json:"destination_id"`
		MetadataOverride json.RawMessage `json:"metadata_override"`
	}
	type publication struct {
		PublicationID string `json:"publication_id"`
		OutputRef     struct {
			VariantID    string `json:"variant_id"`
			ArtifactRole string `json:"artifact_role"`
		} `json:"output_ref"`
		Metadata     json.RawMessage          `json:"metadata"`
		Destinations []publicationDestination `json:"destinations"`
	}
	var publications []publication
	if err := json.Unmarshal(raw, &publications); err != nil || len(publications) == 0 {
		return fmt.Errorf("%w: publications must be a non-empty JSON array", ErrInvalidPayload)
	}
	seenPublications := make(map[string]struct{}, len(publications))
	now := time.Now().UTC()
	for publicationIndex, item := range publications {
		prefix := fmt.Sprintf("publications[%d]", publicationIndex)
		publicationID := strings.TrimSpace(item.PublicationID)
		if publicationID == "" {
			return fmt.Errorf("%w: %s.publication_id is required", ErrInvalidPayload, prefix)
		}
		if _, exists := seenPublications[publicationID]; exists {
			return fmt.Errorf("%w: duplicate %s.publication_id %q", ErrInvalidPayload, prefix, publicationID)
		}
		seenPublications[publicationID] = struct{}{}
		variantID := strings.TrimSpace(item.OutputRef.VariantID)
		artifactRole := strings.TrimSpace(item.OutputRef.ArtifactRole)
		if (variantID == "") == (artifactRole == "") {
			return fmt.Errorf("%w: %s.output_ref requires exactly one of variant_id or artifact_role", ErrInvalidPayload, prefix)
		}
		if len(item.Destinations) == 0 {
			return fmt.Errorf("%w: %s.destinations must contain at least one destination", ErrInvalidPayload, prefix)
		}
		if err := validateMetadataObject(item.Metadata, prefix+".metadata"); err != nil {
			return err
		}
		if publishAt := metadataPublishAt(item.Metadata); publishAt != "" {
			if err := validateFuturePublishAt(publishAt, now, prefix+".metadata.publish_at"); err != nil {
				return err
			}
		}
		seenDestinations := make(map[string]struct{}, len(item.Destinations))
		for destinationIndex, destination := range item.Destinations {
			destinationID := strings.TrimSpace(destination.DestinationID)
			destinationPrefix := fmt.Sprintf("%s.destinations[%d]", prefix, destinationIndex)
			if destinationID == "" {
				return fmt.Errorf("%w: %s.destination_id is required", ErrInvalidPayload, destinationPrefix)
			}
			if _, exists := seenDestinations[destinationID]; exists {
				return fmt.Errorf("%w: duplicate %s.destination_id %q", ErrInvalidPayload, destinationPrefix, destinationID)
			}
			seenDestinations[destinationID] = struct{}{}
			if err := validateMetadataObject(destination.MetadataOverride, destinationPrefix+".metadata_override"); err != nil {
				return err
			}
			if publishAt := metadataPublishAt(destination.MetadataOverride); publishAt != "" {
				if err := validateFuturePublishAt(publishAt, now, destinationPrefix+".metadata_override.publish_at"); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func validateMetadataObject(raw json.RawMessage, field string) error {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var metadata map[string]any
	if err := json.Unmarshal(raw, &metadata); err != nil {
		return fmt.Errorf("%w: %s must be a JSON object", ErrInvalidPayload, field)
	}
	return nil
}

func metadataPublishAt(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var metadata struct {
		PublishAt string `json:"publish_at"`
	}
	if err := json.Unmarshal(raw, &metadata); err != nil {
		return ""
	}
	return strings.TrimSpace(metadata.PublishAt)
}

func validateFuturePublishAt(value string, now time.Time, field string) error {
	publishAt, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return fmt.Errorf("%w: %s must be RFC3339", ErrInvalidPayload, field)
	}
	if !publishAt.After(now) {
		return fmt.Errorf("%w: %s must be in the future", ErrInvalidPayload, field)
	}
	return nil
}

func validatePublicationTarget(target PublicationTargetCmd) error {
	switch strings.TrimSpace(target.Type) {
	case "channel":
		if strings.TrimSpace(target.ChannelID) == "" && strings.TrimSpace(target.ChannelName) == "" {
			return fmt.Errorf("%w: target channel requires channel_id or channel_name", ErrInvalidPayload)
		}
		if target.GroupID != 0 || strings.TrimSpace(target.GroupName) != "" {
			return fmt.Errorf("%w: channel target cannot include group fields", ErrInvalidPayload)
		}
	case "group":
		if target.GroupID <= 0 && strings.TrimSpace(target.GroupName) == "" {
			return fmt.Errorf("%w: target group requires group_id or group_name", ErrInvalidPayload)
		}
		if strings.TrimSpace(target.ChannelID) != "" || strings.TrimSpace(target.ChannelName) != "" {
			return fmt.Errorf("%w: group target cannot include channel fields", ErrInvalidPayload)
		}
	default:
		return fmt.Errorf("%w: target.type must be channel or group", ErrInvalidPayload)
	}
	return nil
}
