// Package store / delivery_plan_payload.go
//
// Canonical delivery-plan parser shared by the atomic Job+Task creator
// (internal/store/atomic_job_task.go::CreateJobWithTaskTx) and the
// finalize-side resolver (internal/deliveries/plan_resolver.go).
//
// Canonical rename note (YouTube → Delivery, PR-15.8):
//
//	YouTubeGroup       → DestinationGroupID   (was: youtube_group_id)
//	YouTubeChannelID   → ExternalDestinationID (was: youtube_channel; column on delivery_destinations)
//	YouTubeVideoID     → RemoteMediaID        (was: youtube_video_id; persisted on job_deliveries.remote_id)
//	YouTubeURL         → RemoteURL            (was: youtube_published_url)
//	YouTubeStatus      → DeliveryStatus       (was: youtube_publish_status)
//
// All five YouTube-prefixed field names are absent from active Go
// runtime code at this revision (verified by commit 7f8d3a4 +
// post-PR-15.8 grep): the per-row model owns `destination_id` +
// `metadata_json` and the durable delivery row owns `remote_id` +
// `last_remote_status`. Velox no longer `SELECT`s `youtube_channels`,
// `youtube_oauth_tokens`, or `youtube_groups` (those tables are
// dropped in migration 090 and the social_repo is the authoritative
// owner of platform metadata). Any future contributor reintroducing a
// YouTube-prefixed field into this struct must replace it with the
// canonical Destination-prefixed equivalent above AND open a new
// migration; do NOT add it as an additional name.
package store

import (
	"encoding/json"
	"fmt"
	"strings"
)

type deliveryPlanEntry struct {
	DestinationID string
	Priority      int
	RetryBudget   int
	MetadataJSON  string
}

func parseDeliveryPlanPayload(payload map[string]interface{}) ([]deliveryPlanEntry, error) {
	if payload == nil {
		return nil, nil
	}

	var entries []deliveryPlanEntry
	if raw, ok := payload["delivery_plan"]; ok && raw != nil {
		switch value := raw.(type) {
		case []interface{}:
			for i, item := range value {
				entryMap, ok := item.(map[string]interface{})
				if !ok {
					return nil, fmt.Errorf("delivery_plan[%d] must be an object", i)
				}
				entry, err := deliveryPlanEntryFromMap(entryMap, i)
				if err != nil {
					return nil, err
				}
				entries = append(entries, entry)
			}
		case []map[string]interface{}:
			for i, entryMap := range value {
				entry, err := deliveryPlanEntryFromMap(entryMap, i)
				if err != nil {
					return nil, err
				}
				entries = append(entries, entry)
			}
		case map[string]interface{}:
			entry, err := deliveryPlanEntryFromMap(value, 0)
			if err != nil {
				return nil, err
			}
			entries = append(entries, entry)
		default:
			return nil, fmt.Errorf("delivery_plan must be an object or array of objects")
		}
	}

	if len(entries) == 0 {
		ids, err := deliveryDestinationIDs(payload)
		if err != nil {
			return nil, err
		}
		for i, id := range ids {
			entries = append(entries, deliveryPlanEntry{
				DestinationID: id,
				Priority:      i,
				RetryBudget:   5,
				MetadataJSON:  "{}",
			})
		}
	}

	seen := make(map[string]struct{}, len(entries))
	for i := range entries {
		id := strings.TrimSpace(entries[i].DestinationID)
		if id == "" {
			return nil, fmt.Errorf("delivery plan entry %d: destination_id is required", i)
		}
		if _, exists := seen[id]; exists {
			return nil, fmt.Errorf("delivery plan entry %d: duplicate destination_id %q", i, id)
		}
		seen[id] = struct{}{}
		entries[i].DestinationID = id
	}
	return entries, nil
}

func deliveryPlanEntryFromMap(value map[string]interface{}, index int) (deliveryPlanEntry, error) {
	id := deliveryPlanFirstString(value, "destination_id", "id")
	if id == "" {
		return deliveryPlanEntry{}, fmt.Errorf("delivery_plan[%d].destination_id is required", index)
	}
	if enabled, ok := deliveryPlanBoolFromMap(value, "enabled"); ok && !enabled {
		return deliveryPlanEntry{}, fmt.Errorf("delivery_plan[%d] is disabled; omit it instead of creating a non-routable plan", index)
	}

	priority := deliveryPlanIntFromMap(value, "priority", index)
	if priority < 0 {
		return deliveryPlanEntry{}, fmt.Errorf("delivery_plan[%d].priority must be >= 0", index)
	}
	retryBudget := deliveryPlanIntFromMap(value, "retry_budget", 5)
	if retryBudget <= 0 {
		return deliveryPlanEntry{}, fmt.Errorf("delivery_plan[%d].retry_budget must be > 0", index)
	}

	metadataJSON := "{}"
	if metadata, ok := value["metadata"]; ok && metadata != nil {
		data, err := json.Marshal(metadata)
		if err != nil {
			return deliveryPlanEntry{}, fmt.Errorf("delivery_plan[%d].metadata: %w", index, err)
		}
		metadataJSON = string(data)
	}
	return deliveryPlanEntry{
		DestinationID: id,
		Priority:      priority,
		RetryBudget:   retryBudget,
		MetadataJSON:  metadataJSON,
	}, nil
}

func deliveryDestinationIDs(payload map[string]interface{}) ([]string, error) {
	for _, key := range []string{"delivery_destination_ids", "destination_ids"} {
		if raw, exists := payload[key]; exists && raw != nil {
			switch values := raw.(type) {
			case []string:
				return normalizeDeliveryDestinationIDs(values)
			case []interface{}:
				ids := make([]string, 0, len(values))
				for i, item := range values {
					id, ok := item.(string)
					if !ok || strings.TrimSpace(id) == "" {
						return nil, fmt.Errorf("%s[%d] must be a non-empty string", key, i)
					}
					ids = append(ids, id)
				}
				return normalizeDeliveryDestinationIDs(ids)
			default:
				return nil, fmt.Errorf("%s must be an array of strings", key)
			}
		}
	}
	if id := deliveryPlanFirstString(payload, "delivery_destination_id", "destination_id"); id != "" {
		return []string{id}, nil
	}
	return nil, nil
}

func normalizeDeliveryDestinationIDs(values []string) ([]string, error) {
	out := make([]string, 0, len(values))
	for i, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return nil, fmt.Errorf("delivery destination id at index %d is empty", i)
		}
		out = append(out, value)
	}
	return out, nil
}
