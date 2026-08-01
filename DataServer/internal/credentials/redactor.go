package credentials

import (
	"encoding/json"
	"regexp"
	"strings"
)

var sensitiveKeyRE = regexp.MustCompile(`(?i)(access[_-]?token|refresh[_-]?token|client[_-]?secret|authorization|cookie|api[_-]?key|signed[_-]?url|signature|password|credential)`)
var bearerRE = regexp.MustCompile(`(?i)(bearer\s+)[^\s,;]+`)
var querySecretRE = regexp.MustCompile(`(?i)([?&](?:access_token|refresh_token|sig|signature|x-goog-signature|token|key)=)[^&\s]+`)

const Redacted = "[REDACTED]"

func String(value string) string {
	value = bearerRE.ReplaceAllString(value, "${1}"+Redacted)
	return querySecretRE.ReplaceAllString(value, "${1}"+Redacted)
}

func Map(input map[string]any) map[string]any {
	if input == nil {
		return nil
	}
	out := make(map[string]any, len(input))
	for key, value := range input {
		if sensitiveKeyRE.MatchString(key) {
			out[key] = Redacted
			continue
		}
		switch nested := value.(type) {
		case map[string]any:
			out[key] = Map(nested)
		case []any:
			out[key] = slice(nested)
		case string:
			out[key] = String(nested)
		default:
			out[key] = value
		}
	}
	return out
}

func slice(input []any) []any {
	out := make([]any, len(input))
	for i, value := range input {
		switch nested := value.(type) {
		case map[string]any:
			out[i] = Map(nested)
		case []any:
			out[i] = slice(nested)
		case string:
			out[i] = String(nested)
		default:
			out[i] = value
		}
	}
	return out
}

func JSON(raw string) string {
	var value any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return String(raw)
	}
	// Preserve the original bytes for reports that do not contain sensitive
	// material. Raw reports are immutable evidence and callers may rely on
	// their exact representation (including field order and whitespace).
	if !containsSensitiveValue(value) {
		return raw
	}
	redacted, err := json.Marshal(redactValue(value))
	if err != nil {
		return String(raw)
	}
	return string(redacted)
}

func containsSensitiveValue(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		for key, nested := range typed {
			if sensitiveKeyRE.MatchString(key) || containsSensitiveValue(nested) {
				return true
			}
		}
	case []any:
		for _, nested := range typed {
			if containsSensitiveValue(nested) {
				return true
			}
		}
	case string:
		return bearerRE.MatchString(typed) || querySecretRE.MatchString(typed)
	}
	return false
}

func redactValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return Map(typed)
	case []any:
		return slice(typed)
	case string:
		return String(typed)
	default:
		return value
	}
}

func ContainsPlaintext(raw string) bool {
	lower := strings.ToLower(raw)
	for _, marker := range []string{"access_token", "refresh_token", "client_secret", "authorization", "cookie", "api_key"} {
		if strings.Contains(lower, marker+"\":") || strings.Contains(lower, marker+"=") {
			return true
		}
	}
	return false
}
