// Package enqueue - video metadata validation and delivery metadata projection.
package enqueue

import (
	"fmt"
	"strings"
	"time"

	"velox-shared/contract/deliveryplan"
)

func attachVideoMetadataToDeliveryPlan(payloadMap map[string]interface{}) error {
	metadata, ok := payloadMap["video_metadata"].(map[string]interface{})
	if !ok || len(metadata) == 0 {
		return nil
	}
	rawPlan, ok := payloadMap["delivery_plan"]
	if !ok || rawPlan == nil {
		return nil
	}
	attach := func(entry map[string]interface{}) {
		entryMetadata, ok := entry["metadata"].(map[string]interface{})
		if !ok {
			entryMetadata = map[string]interface{}{}
			entry["metadata"] = entryMetadata
		}
		if _, exists := entryMetadata["video_metadata"]; !exists {
			entryMetadata["video_metadata"] = cloneMetadataMap(metadata)
		}
	}
	switch plan := rawPlan.(type) {
	case []interface{}:
		for i, item := range plan {
			entry, ok := item.(map[string]interface{})
			if !ok {
				return deliveryplan.NewValidationError(
					fmt.Sprintf("delivery_plan.%d", i),
					"must be an object",
				)
			}
			attach(entry)
		}
	case []map[string]interface{}:
		for _, entry := range plan {
			attach(entry)
		}
	case map[string]interface{}:
		attach(plan)
	}
	return nil
}
func validateVideoMetadata(metadata map[string]interface{}) error {
	if title, ok := metadata["title"]; ok {
		if value, ok := title.(string); !ok || strings.TrimSpace(value) == "" {
			return deliveryplan.NewValidationError("video_metadata.title", "must be a non-empty string")
		}
	}
	if description, ok := metadata["description"]; ok {
		if _, ok := description.(string); !ok {
			return deliveryplan.NewValidationError("video_metadata.description", "must be a string")
		}
	}
	if tags, ok := metadata["tags"]; ok && tags != nil {
		switch values := tags.(type) {
		case []interface{}:
			for i, value := range values {
				if _, ok := value.(string); !ok {
					return deliveryplan.NewValidationError(
						fmt.Sprintf("video_metadata.tags[%d]", i),
						"must be a string",
					)
				}
			}
		case []string:
		default:
			return deliveryplan.NewValidationError("video_metadata.tags", "must be an array of strings")
		}
	}
	if privacy, ok := metadata["privacy_status"]; ok {
		value, ok := privacy.(string)
		if !ok || !isValidPrivacyStatus(value) {
			return deliveryplan.NewValidationError("video_metadata.privacy_status", "must be private, unlisted, or public")
		}
	}
	if publishAt, ok := metadata["publish_at"]; ok && publishAt != nil {
		value, ok := publishAt.(string)
		if !ok || strings.TrimSpace(value) == "" {
			return deliveryplan.NewValidationError("video_metadata.publish_at", "must be an RFC3339 timestamp")
		}
		if _, err := time.Parse(time.RFC3339, value); err != nil {
			return deliveryplan.NewValidationError("video_metadata.publish_at", "must be an RFC3339 timestamp")
		}
	}
	return nil
}
func isValidPrivacyStatus(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "private", "unlisted", "public":
		return true
	default:
		return false
	}
}
func cloneMetadataMap(metadata map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(metadata))
	for key, value := range metadata {
		out[key] = value
	}
	return out
}
