package store

import (
	"encoding/json"
	"strings"
)

func deliveryPlanFirstString(values map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if value, ok := values[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func deliveryPlanBoolFromMap(values map[string]interface{}, key string) (bool, bool) {
	switch value := values[key].(type) {
	case bool:
		return value, true
	case float64:
		return value != 0, true
	case int:
		return value != 0, true
	case string:
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "true", "1", "yes", "on":
			return true, true
		case "false", "0", "no", "off":
			return false, true
		}
	}
	return false, false
}

func deliveryPlanIntFromMap(values map[string]interface{}, key string, fallback int) int {
	switch value := values[key].(type) {
	case int:
		return value
	case int32:
		return int(value)
	case int64:
		return int(value)
	case float64:
		return int(value)
	case json.Number:
		if parsed, err := value.Int64(); err == nil {
			return int(parsed)
		}
	}
	return fallback
}
