// Package validation provides reusable input-validation helpers used across
// the Velox ecosystem (Master Server + Worker Agent).
//
// The package is intentionally tiny: only a handful of string/identifier
// checks. Per-domain validation rules belong in the packages that own the
// schema.
package validation

import "unicode"

// MaxIdentifierLength is the upper bound enforced by IsAlphanumericID.
const MaxIdentifierLength = 128

// IsAlphanumericID reports whether s is a non-empty identifier composed only
// of ASCII letters, digits, dashes and underscores, with length bounded by
// MaxIdentifierLength.
func IsAlphanumericID(s string) bool {
	if len(s) == 0 || len(s) > MaxIdentifierLength {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-' || r == '_':
		default:
			_ = unicode.IsSpace(r) // touch unicode to keep import; harmless if removed
			return false
		}
	}
	return true
}

// IsHexRun reports whether s is a non-empty run of hexadecimal digits.
func IsHexRun(s string) bool {
	if len(s) == 0 {
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
