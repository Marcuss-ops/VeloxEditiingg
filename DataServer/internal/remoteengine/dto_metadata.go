package remoteengine

import (
	"strings"

	"velox-shared/payload"
)

func extractMetadataDTO(flat map[string]interface{}) VideoMetadata {
	var meta VideoMetadata

	if rawMeta, ok := flat["video_metadata"].(map[string]interface{}); ok {
		meta.Title = payload.FirstString(rawMeta, "title", "name")
		meta.Description = payload.FirstString(rawMeta, "description")
		if tags, ok := rawMeta["tags"].([]interface{}); ok {
			for _, t := range tags {
				if s, ok := t.(string); ok && strings.TrimSpace(s) != "" {
					meta.Tags = append(meta.Tags, strings.TrimSpace(s))
				}
			}
		}
		meta.PrivacyStatus = payload.FirstString(rawMeta, "privacy_status")
	}

	// Fallback: metadata array (legacy remote engine shape).
	if meta.Title == "" {
		if metadata, ok := flat["metadata"]; ok {
			switch v := metadata.(type) {
			case []interface{}:
				for _, item := range v {
					if m, ok := item.(map[string]interface{}); ok {
						if title := payload.FirstString(m, "title", "name"); title != "" {
							meta.Title = title
							break
						}
					}
				}
			case []map[string]interface{}:
				for _, item := range v {
					if title := payload.FirstString(item, "title", "name"); title != "" {
						meta.Title = title
						break
					}
				}
			}
		}
	}

	return meta
}

// extractAssetsDTO extracts asset references from the flat map.
// Currently the remote engine does not have a dedicated assets field,
// but scene image_link / clip_link values are collected as assets.
