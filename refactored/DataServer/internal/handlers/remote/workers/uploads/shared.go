package uploads

import (
	"regexp"
	"strings"
)

// slugify sanitizes a string for use as a filename.
func slugify(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "untitled"
	}
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r + ('a' - 'A'))
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_':
			b.WriteRune(r)
		case r == ' ':
			b.WriteRune('_')
		default:
			continue
		}
	}
	out := strings.Trim(b.String(), "_-")
	if out == "" {
		return "untitled"
	}
	return out
}

// firstNonEmptyString returns the first non-empty string from the arguments.
func firstNonEmptyString(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
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
	slot, ok := job["slot_data"].(map[string]interface{})
	if !ok {
		return ""
	}
	if v, ok := slot[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func mergeStringSlices(slices ...[]string) []string {
	var result []string
	for _, s := range slices {
		result = append(result, s...)
	}
	return result
}

func asStringSlice(v interface{}) []string {
	switch val := v.(type) {
	case []string:
		return val
	case []interface{}:
		out := make([]string, 0, len(val))
		for _, item := range val {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func asStringSliceFromSlot(job map[string]interface{}, key string) []string {
	slot, ok := job["slot_data"].(map[string]interface{})
	if !ok {
		return nil
	}
	return asStringSlice(slot[key])
}

// sanitizeDriveFolderName sanitizes a video name for use as a Drive folder name.
func sanitizeDriveFolderName(name string) string {
	reg := regexp.MustCompile(`[^a-zA-Z0-9_\- ]+`)
	cleaned := reg.ReplaceAllString(name, "")
	cleaned = strings.TrimSpace(cleaned)
	if cleaned == "" {
		return ""
	}
	return cleaned
}
