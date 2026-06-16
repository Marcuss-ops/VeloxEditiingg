// Package validation: thin re-export of the canonical ID validators living
// in velox-shared/validation. Keeping the implementation in shared/ prevents
// drift between the worker agent and the master server.
package validation

import sharedvalidation "velox-shared/validation"

const MaxIdentifierLength = sharedvalidation.MaxIdentifierLength

// IsAlphanumericID delegates to velox-shared/validation.IsAlphanumericID.
func IsAlphanumericID(s string) bool {
	return sharedvalidation.IsAlphanumericID(s)
}

// IsHexRun delegates to velox-shared/validation.IsHexRun.
func IsHexRun(s string) bool {
	return sharedvalidation.IsHexRun(s)
}
