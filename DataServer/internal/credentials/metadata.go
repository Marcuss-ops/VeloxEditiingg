package credentials

import (
	"encoding/json"
	"strings"
)

// ReferenceFromJSON extracts the only credential field accepted from
// delivery metadata. It returns an opaque generated reference and never
// accepts credential material in its place.
func ReferenceFromJSON(raw string) (string, error) {
	if strings.TrimSpace(raw) == "" || strings.TrimSpace(raw) == "{}" {
		return "", nil
	}
	var metadata map[string]any
	if err := json.Unmarshal([]byte(raw), &metadata); err != nil {
		return "", ErrInvalidReference
	}
	value, ok := metadata["credential_ref"]
	if !ok || value == nil {
		return "", nil
	}
	ref, ok := value.(string)
	if !ok || !ValidReference(ref) {
		return "", ErrInvalidReference
	}
	return strings.TrimSpace(ref), nil
}
