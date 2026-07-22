package api

import "encoding/json"

// toInt64 extracts a signed integer from a JSON-unmarshalled value.
func toInt64(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case float64:
		return int64(n), true
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return i, true
		}
	}
	return 0, false
}

// toFloat64 extracts a float from a JSON-unmarshalled value.
func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case json.Number:
		if f, err := n.Float64(); err == nil {
			return f, true
		}
	}
	return 0, false
}

// toString extracts a string value from a JSON-unmarshalled value.
func toString(v interface{}) string {
	s, _ := v.(string)
	return s
}

// toInt64Zero extracts a signed integer, defaulting to 0.
func toInt64Zero(v interface{}) int64 {
	n, _ := toInt64(v)
	return n
}
