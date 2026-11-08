package pipeline

import (
	"encoding/json"
	"strings"
)

func stringField(m map[string]interface{}, key string) string {
	if m == nil {
		return ""
	}
	switch v := m[key].(type) {
	case string:
		return strings.TrimSpace(v)
	case json.Number:
		return v.String()
	default:
		return ""
	}
}

func cloneJSONMap(in map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(in))
	for k, v := range in {
		out[k] = cloneJSONValue(v)
	}
	return out
}

func cloneJSONValue(v interface{}) interface{} {
	switch x := v.(type) {
	case map[string]interface{}:
		return cloneJSONMap(x)
	case []interface{}:
		out := make([]interface{}, len(x))
		for i := range x {
			out[i] = cloneJSONValue(x[i])
		}
		return out
	default:
		return x
	}
}
