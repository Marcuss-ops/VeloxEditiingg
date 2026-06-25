// pkg/bootstrap/helpers.go — internal string utilities for the
// bootstrap package. Kept in their own file so the regex-free helpers
// stay trivially testable in isolation.

package bootstrap

import "strings"

// splitOnWhitespace returns the whitespace-separated tokens of s
// without the trailing empty token that strings.Fields would also
// return for an empty input. We use it to parse the sha256sum-style
// `"<hex>  <file>"` baseline fixture format.
func splitOnWhitespace(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Fields(s)
}

// isHex64 returns true iff s is exactly 64 hexadecimal lowercase /
// uppercase characters. SHA-256 outputs 64 hex chars; an operator
// committing a baseline truncated or extended will fail this gate.
func isHex64(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		case r >= 'A' && r <= 'F':
		default:
			return false
		}
	}
	return true
}
