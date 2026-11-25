package pipeline

import (
	"encoding/json"
	"net/url"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/store"
)

type queueAssetMetadata struct {
	ID     string
	Kind   string
	Source string
	SHA256 string
	Size   int64
}

// buildQueueTelemetry composes only durable Master and worker observations.
// Missing boundaries stay null; the read model does not infer a queue phase
// from the job's total wall time.
func buildQueueTelemetry(status string, runtimeAssetsPending bool, taskPayload map[string]interface{}, events []store.JobEvent, progress []store.AssetDownloadProgressView, attempt *store.TaskAttemptSnapshot) gin.H {
	assets := map[string]*queueAssetMetadata{}
	for _, meta := range runtimeAssetMetadata(taskPayload) {
		for _, key := range []string{meta.ID, meta.SHA256} {
			if key = strings.TrimSpace(key); key != "" {
				copyMeta := meta
				assets[key] = &copyMeta
			}
		}
	}
	planned := map[string]bool{}
	states := map[string]string{}
	cacheHits := map[string]bool{}
	var enqueuedAt, startedAt, completedAt, offeredAt, acceptedAt string
	reservationCreated, planSent, prefetchPrepared := false, false, false
	targetTaskID := ""
	if attempt != nil {
		targetTaskID = strings.TrimSpace(attempt.TaskID)
	}
	assetErrors := map[string]gin.H{}
	for _, event := range events {
		var p map[string]interface{}
		if err := json.Unmarshal([]byte(event.RawJSON), &p); err != nil {
			continue
		}
		if targetTaskID != "" && queueString(p, "task_id") != "" && queueString(p, "task_id") != targetTaskID {
			continue
		}
		switch event.Event {
		case "prefetch.reservation_created":
			reservationCreated = true
		case "prefetch.future_plan_sent":
			planSent = true
			enqueuedAt = firstQueueTimestamp(enqueuedAt, queueString(p, "plan_sent_at"), event.Timestamp)
			for _, key := range queueStringSlice(p["asset_keys"]) {
				planned[key] = true
			}
		case "prefetch.future_plan_received":
			startedAt = firstQueueTimestamp(startedAt, event.Timestamp)
		case "prefetch.download_started":
			startedAt = firstQueueTimestamp(startedAt, queueString(p, "download_started_at"), event.Timestamp)
			key := queueString(p, "asset_key")
			if key != "" {
				states[key] = "DOWNLOADING"
			}
		case "prefetch.asset_ready":
			key := queueString(p, "asset_key")
			if key != "" {
				if reason := queueString(p, "error_reason"); reason != "" {
					states[key] = "FAILED"
					assetErrors[key] = gin.H{"code": "ASSET_PREPARATION_FAILED", "message": sanitizeQueueError(reason)}
				} else {
					states[key] = "READY"
					cacheHits[key] = queueBool(p, "cache_hit")
				}
			}
		case "prefetch.asset_failed", "prefetch.prefetch_failed", "prefetch.prefetch_error":
			key := queueString(p, "asset_key")
			if key != "" {
				states[key] = "FAILED"
				assetErrors[key] = gin.H{"code": firstQueueString(queueString(p, "error_code"), "ASSET_PREPARATION_FAILED"), "message": sanitizeQueueError(queueString(p, "error_reason"))}
			}
		case "prefetch.prefetch_prepared":
			prefetchPrepared = true
			completedAt = firstQueueString(queueString(p, "prepared_at"), event.Timestamp)
		case "lease.offered":
			offeredAt = firstQueueString(queueString(p, "offered_at"), event.Timestamp)
		case "lease.accepted":
			acceptedAt = firstQueueString(queueString(p, "accepted_at"), event.Timestamp)
		}
	}
	progressByKey := make(map[string]store.AssetDownloadProgressView, len(progress))
	for _, item := range progress {
		key := strings.TrimSpace(item.AssetKey)
		if key == "" {
			continue
		}
		if targetTaskID != "" && item.TaskID != "" && item.TaskID != targetTaskID {
			continue
		}
		// The store orders progress newest first. Keep the most recent record
		// when one shared asset has evidence from more than one worker.
		if _, exists := progressByKey[key]; exists {
			continue
		}
		progressByKey[key] = item
		planned[key] = true
		if item.State != "" {
			states[key] = strings.ToUpper(item.State)
		}
		cacheHits[key] = item.CacheHit
		if assets[key] == nil {
			assets[key] = &queueAssetMetadata{ID: firstQueueString(item.AssetID, key), Kind: item.Role, SHA256: item.SHA256}
		}
		if assets[key].SHA256 == "" {
			assets[key].SHA256 = item.SHA256
		}
	}
	if attempt != nil {
		offeredAt = firstQueueString(attempt.LeasedAt, offeredAt)
		acceptedAt = firstQueueString(attempt.StartedAt, acceptedAt)
	}

	assetKeys := make([]string, 0, len(planned))
	for key := range planned {
		assetKeys = append(assetKeys, key)
	}
	sort.Strings(assetKeys)
	assetDetails := make([]gin.H, 0, len(assetKeys))
	ready, failed := 0, 0
	var bytesTotal int64
	for _, key := range assetKeys {
		meta := assets[key]
		if meta == nil {
			meta = &queueAssetMetadata{ID: key}
		}
		state := states[key]
		if state == "READY" || state == "CACHE_HIT" {
			ready++
		}
		if state == "FAILED" || state == "CANCELLED" {
			failed++
		}
		item, observed := progressByKey[key]
		if observed {
			bytesTotal += item.BytesTotal
		} else if meta.Size > 0 {
			bytesTotal += meta.Size
		}
		detail := gin.H{
			"asset_id":  firstQueueString(meta.ID, key),
			"kind":      meta.Kind,
			"source":    meta.Source,
			"sha256":    firstQueueString(item.SHA256, meta.SHA256),
			"cache_hit": cacheHits[key],
			"state":     state,
		}
		if observed {
			detail["download_ms"] = item.DownloadWorkMS
			detail["verify_ms"] = item.HashVerifyMS
			if item.ErrorCode != "" {
				detail["error"] = gin.H{"code": item.ErrorCode, "message": sanitizeQueueError(item.ErrorDetail)}
			}
		} else if assetErrors[key] != nil {
			detail["error"] = assetErrors[key]
		}
		assetDetails = append(assetDetails, detail)
	}
	queueReason := queueReason(status, runtimeAssetsPending, planSent, prefetchPrepared, reservationCreated, offeredAt, acceptedAt)
	return gin.H{
		"prefetch_enqueued_at":  nullableQueueTimestamp(enqueuedAt),
		"prefetch_started_at":   nullableQueueTimestamp(startedAt),
		"prefetch_completed_at": nullableQueueTimestamp(completedAt),
		"lease_offered_at":      nullableQueueTimestamp(offeredAt),
		"lease_accepted_at":     nullableQueueTimestamp(acceptedAt),
		"assets_total":          len(assetKeys),
		"assets_ready":          ready,
		"assets_failed":         failed,
		"assets_bytes":          bytesTotal,
		"queue_reason":          queueReason,
		"assets":                assetDetails,
	}
}

func queueReason(status string, runtimeAssetsPending, planSent, prefetchPrepared, reservationCreated bool, offeredAt, acceptedAt string) string {
	if queueTerminal(status) {
		return "none"
	}
	if runtimeAssetsPending {
		return "validation"
	}
	if planSent && !prefetchPrepared {
		return "prefetch_wait"
	}
	if acceptedAt != "" {
		return "none"
	}
	if prefetchPrepared && offeredAt == "" {
		return "pin_wait"
	}
	if offeredAt != "" || prefetchPrepared {
		return "worker_unavailable"
	}
	return "unknown"
}

func queueTerminal(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "SUCCEEDED", "FAILED", "CANCELLED", "CANCELED", "REJECTED", "TIMED_OUT":
		return true
	default:
		return false
	}
}

func runtimeAssetMetadata(payload map[string]interface{}) []queueAssetMetadata {
	if payload == nil {
		return nil
	}
	var rawAssets []interface{}
	switch raw := payload["runtime_assets"].(type) {
	case []interface{}:
		rawAssets = raw
	case []map[string]interface{}:
		for _, item := range raw {
			rawAssets = append(rawAssets, item)
		}
	}
	out := make([]queueAssetMetadata, 0, len(rawAssets))
	for _, raw := range rawAssets {
		asset, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		meta := queueAssetMetadata{
			ID:     firstQueueString(queueString(asset, "asset_id"), queueString(asset, "asset_key"), queueString(asset, "drive_file_id")),
			Kind:   firstQueueString(queueString(asset, "kind"), queueString(asset, "role")),
			SHA256: firstQueueString(queueString(asset, "sha256"), queueString(asset, "content_sha256")),
			Size:   queueInt64(asset["size_bytes"]),
		}
		meta.Source = queueAssetSource(asset)
		out = append(out, meta)
	}
	return out
}

func queueAssetSource(asset map[string]interface{}) string {
	if strings.TrimSpace(queueString(asset, "drive_file_id")) != "" || strings.HasPrefix(strings.ToLower(queueString(asset, "url")), "velox-drive:") {
		return "drive"
	}
	source := strings.ToLower(firstQueueString(queueString(asset, "source"), queueString(asset, "provider"), queueString(asset, "storage_provider")))
	if strings.Contains(source, "object") || strings.Contains(strings.ToLower(queueString(asset, "url")), "/objects/") {
		return "objectstore"
	}
	rawURL := firstQueueString(queueString(asset, "source_url"), queueString(asset, "url"))
	if parsed, err := url.Parse(rawURL); err == nil && parsed.Scheme == "https" {
		return "https"
	}
	if source == "drive" || source == "https" || source == "objectstore" {
		return source
	}
	return "unknown"
}

func queueString(payload map[string]interface{}, key string) string {
	value, _ := payload[key].(string)
	return strings.TrimSpace(value)
}

func queueStringSlice(value interface{}) []string {
	items, _ := value.([]interface{})
	out := make([]string, 0, len(items))
	for _, item := range items {
		if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
			out = append(out, strings.TrimSpace(s))
		}
	}
	return out
}

func queueBool(payload map[string]interface{}, key string) bool {
	value, _ := payload[key].(bool)
	return value
}

func queueInt64(value interface{}) int64 {
	switch v := value.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	default:
		return 0
	}
}

func firstQueueString(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func firstQueueTimestamp(values ...string) string { return firstQueueString(values...) }

func nullableQueueTimestamp(raw string) interface{} {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	return nullableJobTimestamp(raw)
}

func sanitizeQueueError(raw string) string {
	if strings.Contains(strings.ToLower(raw), "https://") || strings.Contains(strings.ToLower(raw), "http://") {
		return "asset transfer failed; inspect the Master error log for the sanitized cause"
	}
	return raw
}
