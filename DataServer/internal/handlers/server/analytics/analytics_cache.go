package analytics

import (
	"encoding/json"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func toFloat(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int64:
		return float64(t)
	case json.Number:
		n, _ := t.Float64()
		return n
	case string:
		n, _ := strconv.ParseFloat(strings.TrimSpace(t), 64)
		return n
	default:
		return 0
	}
}

func toInt(v any) int {
	return int(toFloat(v))
}

func toStr(v any) string {
	if v == nil {
		return ""
	}
	s, ok := v.(string)
	if ok {
		return s
	}
	return ""
}

func loadAnalyticsCache(period string) map[string]any {
	if analyticsStore != nil {
		if data, err := analyticsStore.GetAnalyticsCache(period); err == nil && len(data) > 0 {
			return data
		}
	}
	if analyticsState.DataDir == "" {
		return map[string]any{}
	}
	path := filepath.Join(analyticsState.DataDir, "analytics", "analytics_cache.json")
	var raw map[string]any
	if err := readJSONFile(path, &raw); err != nil {
		return map[string]any{}
	}
	entry, ok := raw[period].(map[string]any)
	if !ok {
		entry = map[string]any{}
	}
	data, ok := entry["data"].(map[string]any)
	if ok {
		if analyticsStore != nil {
			if b, err := json.Marshal(data); err == nil {
				ts := toFloat(entry["ts"])
				if ts == 0 {
					ts = float64(time.Now().Unix())
				}
				if err := analyticsStore.UpsertAnalyticsCache(period, ts, b); err != nil {
					log.Printf("sqlite upsert analytics cache failed: %v", err)
				}
			}
		}
		return data
	}
	return map[string]any{}
}

func loadRealtimeCache() map[string]any {
	if analyticsStore != nil {
		if data, err := analyticsStore.GetAnalyticsCache("realtime"); err == nil && len(data) > 0 {
			return data
		}
	}
	if analyticsState.DataDir == "" {
		return map[string]any{}
	}
	path := filepath.Join(analyticsState.DataDir, "analytics", "analytics_realtime_cache.json")
	var raw map[string]any
	if err := readJSONFile(path, &raw); err != nil {
		return map[string]any{}
	}
	if data, ok := raw["data"].(map[string]any); ok {
		if analyticsStore != nil {
			if b, err := json.Marshal(data); err == nil {
				if err := analyticsStore.UpsertAnalyticsCache("realtime", float64(time.Now().Unix()), b); err != nil {
					log.Printf("sqlite upsert realtime cache failed: %v", err)
				}
			}
		}
		return data
	}
	return map[string]any{}
}
