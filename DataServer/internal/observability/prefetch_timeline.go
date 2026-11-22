package observability

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

var (
	prefetchURLPattern    = regexp.MustCompile(`(?i)https?://[^\s"'<>]+`)
	prefetchBearerPattern = regexp.MustCompile(`(?i)bearer\s+[a-z0-9._~+/-]+=*`)
	prefetchSecretPattern = regexp.MustCompile(`(?i)(token|sig|signature|credential|key)=([^&\s]+)`)
)

// PrefetchTimeline is the canonical operator projection for one future asset.
// Missing fields mean that the corresponding lifecycle event has not arrived;
// the projection never fabricates timing from the job waterfall.
type PrefetchTimeline struct {
	AssetKey            string `json:"asset_key"`
	AssetID             string `json:"asset_id,omitempty"`
	JobID               string `json:"job_id"`
	Phase               string `json:"phase,omitempty"`
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
	Attempts            int    `json:"attempts,omitempty"`
	LastHTTPStatus      int    `json:"last_http_status,omitempty"`
	BytesReceived       int64  `json:"bytes_received,omitempty"`
	BytesExpected       int64  `json:"bytes_expected,omitempty"`
	SHA256              string `json:"sha256,omitempty"`
	HashVerified        bool   `json:"hash_verified"`
	LastErrorCode       string `json:"last_error_code,omitempty"`
	LastError           string `json:"last_error,omitempty"`
	LastProgressAt      string `json:"last_progress_at,omitempty"`
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
				entry = &PrefetchTimeline{AssetKey: key, JobID: event.JobID, Phase: "PLANNED"}
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

// mergePrefetchAssetProgress joins the planned asset keys from the event
// journal with the worker's latest transfer checkpoints. The response contains
// only opaque asset keys and transfer metadata; signed source URLs never enter
// the operator read model.
func mergePrefetchAssetProgress(planned []PrefetchTimeline, progress []AssetProgressView, jobID string) []PrefetchTimeline {
	byKey := make(map[string]int, len(planned)+len(progress))
	for i := range planned {
		byKey[planned[i].AssetKey] = i
	}
	for _, asset := range progress {
		if asset.AssetKey == "" {
			continue
		}
		index, exists := byKey[asset.AssetKey]
		if !exists {
			planned = append(planned, PrefetchTimeline{AssetKey: asset.AssetKey, JobID: jobID})
			index = len(planned) - 1
			byKey[asset.AssetKey] = index
		}
		entry := &planned[index]
		entry.Phase = asset.State
		entry.AssetID = asset.AssetID
		entry.WorkerID = firstNonEmpty(asset.WorkerID, entry.WorkerID)
		entry.TaskID = firstNonEmpty(asset.TaskID, entry.TaskID)
		entry.Attempts = asset.Attempt
		entry.LastHTTPStatus = asset.HTTPStatus
		entry.BytesReceived = asset.BytesDownloaded
		entry.BytesExpected = asset.BytesTotal
		entry.SHA256 = asset.SHA256
		entry.HashVerified = asset.HashVerified
		entry.LastErrorCode = asset.ErrorCode
		entry.LastError = sanitizePrefetchError(asset.ErrorDetail)
		entry.LastProgressAt = asset.UpdatedAt
		if asset.CacheHit {
			entry.CacheHit = true
			entry.Phase = "CACHE_HIT"
		}
	}
	sort.Slice(planned, func(i, j int) bool {
		if planned[i].Distance != planned[j].Distance {
			return planned[i].Distance < planned[j].Distance
		}
		return planned[i].AssetKey < planned[j].AssetKey
	})
	return planned
}

func sanitizePrefetchError(message string) string {
	message = prefetchURLPattern.ReplaceAllString(message, "[source URL redacted]")
	message = prefetchBearerPattern.ReplaceAllString(message, "Bearer [redacted]")
	return prefetchSecretPattern.ReplaceAllString(message, "$1=[redacted]")
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
		entry.Phase = "PLANNED"
		entry.PlanSentAt = firstNonEmpty(entry.PlanSentAt, stringValue(p, "plan_sent_at"))
	case "prefetch.download_started":
		entry.Phase = "DOWNLOADING"
		entry.DownloadStartedAt = firstNonEmpty(entry.DownloadStartedAt, stringValue(p, "download_started_at"))
		entry.Origin = firstNonEmpty(entry.Origin, stringValue(p, "origin"))
	case "prefetch.asset_ready":
		entry.Phase = "READY"
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
	if strings.Contains(event.Event, "failed") || strings.Contains(event.Event, "error") {
		entry.Phase = "FAILED"
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
