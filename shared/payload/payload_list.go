// Package payload / payload_list.go
//
// List normalization helpers for map[string]interface{} (deserialized
// JSON). Split out of payload.go; the package doc lives in payload.go.
package payload

import "strings"

func NormalizeStringList(source map[string]interface{}, keys ...string) []string {
	if source == nil {
		return nil
	}
	var values []string
	for _, key := range keys {
		v, ok := source[key]
		if !ok {
			continue
		}
		switch vv := v.(type) {
		case []string:
			values = append(values, vv...)
		case []interface{}:
			for _, item := range vv {
				if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
					values = append(values, strings.TrimSpace(s))
				}
			}
		case string:
			for _, line := range strings.Split(vv, "\n") {
				if s := strings.TrimSpace(line); s != "" {
					values = append(values, s)
				}
			}
		}
	}
	return DedupeStrings(values)
}

func NormalizeToStrings(v interface{}) []string {
	switch val := v.(type) {
	case []string:
		return val
	case []interface{}:
		out := make([]string, 0, len(val))
		for _, item := range val {
			if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
				out = append(out, strings.TrimSpace(s))
			}
		}
		return out
	case string:
		s := strings.TrimSpace(val)
		if s == "" {
			return nil
		}
		if strings.Contains(s, "\n") {
			lines := strings.Split(s, "\n")
			out := make([]string, 0, len(lines))
			for _, line := range lines {
				if trimmed := strings.TrimSpace(line); trimmed != "" {
					out = append(out, trimmed)
				}
			}
			return out
		}
		return []string{s}
	default:
		return nil
	}
}

func DedupeStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

// NormalizeList normalizza un valore che può essere string o []interface{} in una stringa
// con elementi separati da newline. Utile per campi job come source_text, image_links, etc.
func NormalizeList(val interface{}) string {
	switch v := val.(type) {
	case []interface{}:
		var parts []string
		for _, item := range v {
			if s, ok := item.(string); ok {
				parts = append(parts, strings.TrimSpace(s))
			}
		}
		return strings.Join(parts, "\n")
	case string:
		return strings.TrimSpace(v)
	default:
		return ""
	}
}

// NormalizeListToArray normalizza un valore (string o []interface{}) in []string.
// Se è una stringa con newline, la divide in righe.
func NormalizeListToArray(val interface{}) []string {
	if val == nil {
		return nil
	}
	switch v := val.(type) {
	case []interface{}:
		var result []string
		for _, item := range v {
			if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
				result = append(result, strings.TrimSpace(s))
			}
		}
		return result
	case string:
		s := strings.TrimSpace(v)
		if s == "" {
			return nil
		}
		if strings.Contains(s, "\n") {
			var result []string
			for _, line := range strings.Split(s, "\n") {
				if trimmed := strings.TrimSpace(line); trimmed != "" {
					result = append(result, trimmed)
				}
			}
			return result
		}
		return []string{s}
	default:
		return nil
	}
}
