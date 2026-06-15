package analytics

import (
	"encoding/json"
	"strconv"
	"strings"
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
	return map[string]any{}
}

func loadRealtimeCache() map[string]any {
	if analyticsStore != nil {
		if data, err := analyticsStore.GetAnalyticsCache("realtime"); err == nil && len(data) > 0 {
			return data
		}
	}
	return map[string]any{}
}
