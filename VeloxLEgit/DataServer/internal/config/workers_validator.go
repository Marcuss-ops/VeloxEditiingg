package config

import "fmt"

// ValidateProductionWorkers enforces the canonical production worker
// allowlist invariants on VELOX_ALLOWED_WORKERS. The fleet size is NOT
// bounded — only the shape of the allowlist is checked.
//
// Rules (operations MUST comply with these):
//   - allowlist non-empty in production  → ids must contain at least one entry
//   - no wildcard '*'                    → no id may be '*'
//   - no blank IDs                       → parseCommaList already trims and drops empties
//   - unique IDs                         → no id may appear twice
//
// Single source of truth for the production allowlist shape. Replicated
// copies in the gRPC handler, Ansible playbooks, and HTTP layer are
// FORBIDDEN: drift from this rule opens the fleet to misconfiguration
// at exactly the layer we want centralised. If a caller needs to check
// the allowlist, it MUST call this function.
//
// Callers MUST pre-filter the input before invoking:
//   - drop empty tokens (parseCommaList already does this)
//   - reject any '*' wildcard entry (this validator defends in depth
//     so a future caller is still safe)
func ValidateProductionWorkers(ids []string) error {
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id == "*" {
			return fmt.Errorf(
				"VELOX_ALLOWED_WORKERS must not contain '*'; " +
					"configure an explicit comma-separated list of worker IDs",
			)
		}
		if id == "" {
			return fmt.Errorf(
				"VELOX_ALLOWED_WORKERS must not contain empty IDs; " +
					"configure an explicit comma-separated list of worker IDs",
			)
		}
		if _, dup := seen[id]; dup {
			return fmt.Errorf(
				"VELOX_ALLOWED_WORKERS contains duplicate worker ID %q", id,
			)
		}
		seen[id] = struct{}{}
	}
	if len(ids) == 0 {
		return fmt.Errorf(
			"VELOX_ALLOWED_WORKERS must not be empty in production; " +
				"set at least one worker ID (comma-separated)",
		)
	}
	return nil
}
