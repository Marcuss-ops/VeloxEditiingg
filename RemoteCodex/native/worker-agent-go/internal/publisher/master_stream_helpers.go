package publisher

import "strings"

// isLowerHex64 reports whether s is a lowercase SHA-256 digest.
func isLowerHex64(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

// extractJSONString extracts one string from the flat completion response.
func extractJSONString(b []byte, key string) string {
	s := string(b)
	idx := strings.Index(s, key)
	if idx < 0 {
		return ""
	}
	rest := s[idx+len(key):]
	rest = strings.TrimLeft(rest, " \t:")
	if !strings.HasPrefix(rest, `"`) {
		return ""
	}
	rest = rest[1:]
	end := strings.IndexByte(rest, '"')
	if end < 0 {
		return ""
	}
	return rest[:end]
}
