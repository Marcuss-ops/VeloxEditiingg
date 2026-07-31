package grpcserver

// Helpers for translating the JSON-shaped Hello capabilities map into the
// scalar fields of an immutable worker runtime snapshot. Capabilities may
// expose host facts either at the top level or under the canonical "host"
// object, and structpb.AsMap normalizes numbers to float64.

func snapshotValue(caps map[string]interface{}, key string) (interface{}, bool) {
	if caps == nil {
		return nil, false
	}
	if value, ok := caps[key]; ok {
		return value, true
	}
	if host, ok := caps["host"].(map[string]interface{}); ok {
		value, ok := host[key]
		return value, ok
	}
	return nil, false
}

func snapshotHostValue(caps map[string]interface{}, key string) (interface{}, bool) {
	if caps == nil {
		return nil, false
	}
	host, ok := caps["host"].(map[string]interface{})
	if !ok {
		return nil, false
	}
	value, ok := host[key]
	return value, ok
}

func snapshotString(caps map[string]interface{}, key, fallback string) string {
	value, ok := snapshotValue(caps, key)
	if !ok {
		return fallback
	}
	if text, ok := value.(string); ok {
		return text
	}
	return fallback
}

func snapshotFloat(caps map[string]interface{}, key string, fallback float64) float64 {
	value, ok := snapshotValue(caps, key)
	if !ok {
		return fallback
	}
	switch number := value.(type) {
	case float64:
		return number
	case float32:
		return float64(number)
	case int:
		return float64(number)
	case int32:
		return float64(number)
	case int64:
		return float64(number)
	case uint:
		return float64(number)
	case uint32:
		return float64(number)
	case uint64:
		return float64(number)
	default:
		return fallback
	}
}

func snapshotInt(caps map[string]interface{}, key string, fallback int) int {
	return int(snapshotFloat(caps, key, float64(fallback)))
}

func snapshotInt64(caps map[string]interface{}, key string, fallback int64) int64 {
	return int64(snapshotFloat(caps, key, float64(fallback)))
}

func snapshotHostInt(caps map[string]interface{}, key string) int {
	value, ok := snapshotHostValue(caps, key)
	if !ok {
		return 0
	}
	switch number := value.(type) {
	case float64:
		return int(number)
	case int:
		return number
	case int32:
		return int(number)
	case int64:
		return int(number)
	default:
		return 0
	}
}

func snapshotHostInt64(caps map[string]interface{}, key string) int64 {
	value, ok := snapshotHostValue(caps, key)
	if !ok {
		return 0
	}
	switch number := value.(type) {
	case float64:
		return int64(number)
	case int:
		return int64(number)
	case int32:
		return int64(number)
	case int64:
		return number
	default:
		return 0
	}
}
