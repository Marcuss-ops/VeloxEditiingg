package observability

import (
	"fmt"
	"sort"
	"strings"
)

// PrefetchTimeline is the canonical operator projection for one future asset.
// Missing fields mean that the corresponding lifecycle event has not arrived;
// the projection never fabricates timing from the job waterfall.
type PrefetchTimeline struct {
	AssetKey            string `json:"asset_key"`
	JobID               string `json:"job_id"`
	TaskID              string `json:"task_id,omitempty"`
	WorkerID            string `json:"worker_id,omitempty"`
	PlanID              string `json:"plan_id,omitempty"`
	PlanVersion         int64  `json:"plan_version,omitempty"`
	ReservationID       string `json:"reservation_id,omitempty"`
	Distance            int    `json:"distance,omitempty"`
	PlanSentAt          string `json:"plan_sent_at,omitempty"`
	DownloadStartedAt   string `json:"download_started_at,omitempty"`
	AssetReadyAt        string `json:"asset_ready_at,omitempty"`
	JobStartedAt        string `json:"job_started_at,omitempty"`
	PrefetchReadyLeadMS int64  `json:"prefetch_ready_lead_ms,omitempty"`
	CacheHit            bool   `json:"cache_hit,omitempty"`
	Origin              string `json:"origin,omitempty"`
}

// BuildPrefetchTimeline groups the durable prefetch events for a job by the
// canonical asset key. It is intentionally tolerant of older event payloads.
func BuildPrefetchTimeline(events []JobEvent) []PrefetchTimeline {
	byKey := make(map[string]*PrefetchTimeline)
	for _, event := range events {
		if !strings.HasPrefix(event.Event, "prefetch.") {
			continue
		}
		payload := event.Payload
		keys := []string{stringValue(payload, "asset_key")}
		if event.Event == "prefetch.future_plan_sent" {
			keys = append(keys, stringSlice(payload, "asset_keys")...)
		}
		for _, key := range keys {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			entry := byKey[key]
			if entry == nil {
				entry = &PrefetchTimeline{AssetKey: key, JobID: event.JobID}
				byKey[key] = entry
			}
			mergePrefetchEvent(entry, event)
		}
	}
	result := make([]PrefetchTimeline, 0, len(byKey))
	for _, entry := range byKey {
		result = append(result, *entry)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Distance != result[j].Distance {
			return result[i].Distance < result[j].Distance
		}
		return result[i].AssetKey < result[j].AssetKey
	})
	return result
}

func mergePrefetchEvent(entry *PrefetchTimeline, event JobEvent) {
	p := event.Payload
	entry.TaskID = firstNonEmpty(entry.TaskID, stringValue(p, "task_id"))
	entry.WorkerID = firstNonEmpty(entry.WorkerID, stringValue(p, "worker_id"))
	entry.PlanID = firstNonEmpty(entry.PlanID, stringValue(p, "plan_id"))
	entry.ReservationID = firstNonEmpty(entry.ReservationID, stringValue(p, "reservation_id"))
	if entry.PlanVersion == 0 {
		entry.PlanVersion = intValue(p, "plan_version")
	}
	if entry.Distance == 0 {
		entry.Distance = int(intValue(p, "distance"))
	}
	switch event.Event {
	case "prefetch.future_plan_sent":
		entry.PlanSentAt = firstNonEmpty(entry.PlanSentAt, stringValue(p, "plan_sent_at"))
	case "prefetch.download_started":
		entry.DownloadStartedAt = firstNonEmpty(entry.DownloadStartedAt, stringValue(p, "download_started_at"))
		entry.Origin = firstNonEmpty(entry.Origin, stringValue(p, "origin"))
	case "prefetch.asset_ready":
		entry.DownloadStartedAt = firstNonEmpty(entry.DownloadStartedAt, stringValue(p, "download_started_at"))
		entry.AssetReadyAt = firstNonEmpty(entry.AssetReadyAt, stringValue(p, "asset_ready_at"))
		entry.CacheHit = boolValue(p, "cache_hit")
		entry.Origin = firstNonEmpty(entry.Origin, stringValue(p, "origin"))
	case "prefetch.prefetch_ready_lead":
		entry.AssetReadyAt = firstNonEmpty(entry.AssetReadyAt, stringValue(p, "asset_ready_at"))
		entry.JobStartedAt = firstNonEmpty(entry.JobStartedAt, stringValue(p, "job_started_at"))
		entry.PrefetchReadyLeadMS = intValue(p, "prefetch_ready_lead_ms")
		entry.Origin = firstNonEmpty(entry.Origin, stringValue(p, "origin"))
	}
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func stringValue(payload map[string]any, key string) string {
	if value, ok := payload[key]; ok {
		return fmt.Sprint(value)
	}
	return ""
}

func stringSlice(payload map[string]any, key string) []string {
	values, ok := payload[key].([]any)
	if !ok {
		return nil
	}
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value != nil {
			result = append(result, fmt.Sprint(value))
		}
	}
	return result
}

func intValue(payload map[string]any, key string) int64 {
	switch value := payload[key].(type) {
	case float64:
		return int64(value)
	case int:
		return int64(value)
	case int64:
		return value
	}
	return 0
}

func boolValue(payload map[string]any, key string) bool {
	value, _ := payload[key].(bool)
	return value
}
