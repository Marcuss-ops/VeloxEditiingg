// Package enqueue - video metadata validation and renderer-safe projection.
package enqueue

import (
	"fmt"
	"strings"
	"time"

	"velox-shared/contract/deliveryplan"
)

// attachVideoMetadataToDeliveryPlan is retained as a compatibility helper for
// callers that invoke the normalizer directly. Publication metadata is never
// copied into delivery_plan or the renderer payload. Only technical render
// options understood by the renderer are retained in video_metadata.
func attachVideoMetadataToDeliveryPlan(payloadMap map[string]interface{}) error {
	if payloadMap == nil {
		return nil
	}
	metadata, ok := payloadMap["video_metadata"].(map[string]interface{})
	if !ok || len(metadata) == 0 {
		return nil
	}
	filtered := rendererVideoMetadata(metadata)
	if len(filtered) == 0 {
		delete(payloadMap, "video_metadata")
	} else {
		payloadMap["video_metadata"] = filtered
	}
	return nil
}

// rendererVideoMetadata allowlists only technical render settings. Editorial
// publication fields (title, description, tags, privacy, scheduling,
// localizations, and provider overrides) are deliberately excluded.
func rendererVideoMetadata(metadata map[string]interface{}) map[string]interface{} {
	allowed := map[string]struct{}{
		"width":             {},
		"height":            {},
		"fps_num":           {},
		"fps_den":           {},
		"pixel_format":      {},
		"sample_rate":       {},
		"audio_sample_rate": {},
		"audio_channels":    {},
		"video_codec":       {},
		"audio_codec":       {},
	}
	filtered := make(map[string]interface{})
	for key, value := range metadata {
		if _, ok := allowed[key]; ok {
			filtered[key] = value
		}
	}
	return filtered
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
