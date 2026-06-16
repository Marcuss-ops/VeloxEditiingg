package uploads

import (
	"regexp"
	"strings"
)

func slugify(s string) string {
	reg := regexp.MustCompile(`[^a-zA-Z0-9]+`)
	return strings.ToLower(reg.ReplaceAllString(s, "-"))
}

func firstNonEmptyString(values ...string) string {
	for _, v := range values {
		if s := strings.TrimSpace(v); s != "" {
			return s
		}
	}
	return ""
}

func asString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func asStringFromSlot(job map[string]interface{}, key string) string {
	if slot, ok := job["slot_data"].(map[string]interface{}); ok {
		return asString(slot[key])
	}
	return ""
}

func asStringSlice(v interface{}) []string {
	switch val := v.(type) {
	case []string:
		return append([]string(nil), val...)
	case []interface{}:
		out := make([]string, 0, len(val))
		for _, item := range val {
			if s := strings.TrimSpace(asString(item)); s != "" {
				out = append(out, s)
			}
		}
		return out
	case string:
		parts := strings.Split(val, ",")
		out := make([]string, 0, len(parts))
		for _, part := range parts {
			if s := strings.TrimSpace(part); s != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func asStringSliceFromSlot(job map[string]interface{}, key string) []string {
	if slot, ok := job["slot_data"].(map[string]interface{}); ok {
		return asStringSlice(slot[key])
	}
	return nil
}

func mergeStringSlices(lists ...[]string) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0)
	for _, list := range lists {
		for _, item := range list {
			s := strings.TrimSpace(item)
			if s == "" {
				continue
			}
			if _, ok := seen[s]; ok {
				continue
			}
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}
