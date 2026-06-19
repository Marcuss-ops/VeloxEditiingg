package config

import "fmt"

// productionWorkerCount is the canonical worker count for a Velox
// production deployment. The constant is the operator-facing source of
// truth referenced by deploy/, ansible/, and the env templates, so
// changing this number requires updating those surfaces in lockstep.
const productionWorkerCount = 2

// ValidateProductionWorkers enforces the canonical production worker
// topology: exactly productionWorkerCount distinct worker IDs in the
// allowlist that backs VELOX_ALLOWED_WORKERS.
//
// Single source of truth for the production worker-count rule.
// Callers MUST pre-filter the input before invoking:
//
//   - drop empty tokens                 (parseCommaList already does this)
//   - reject any "*" wildcard entry     (Config.Validate does this)
//
// Replicated copies in the gRPC handler, Ansible playbooks, and HTTP
// layer are FORBIDDEN: drift from this rule opens the fleet to
// misconfiguration at exactly the layer we want centralised. If a
// caller needs worker-count semantics, it MUST call this function.
//
// Error semantics:
//   - len(ids) != productionWorkerCount → fail with count.
//   - ids[0] == ids[1]                  → fail (a duplicated ID passes a
//                                          naive union/containment check
//                                          but silently halves fleet
//                                          capacity and breaks
//                                          heartbeat accounting).
//   - the (ids[0] == "" && ids[1] == "") corner surfaces as "must be
//     unique" because "" == "" — that is intentional: an empty-allowlist
//     shipped to production is treated identically to a duplicated ID,
//     not as a "zero workers" config (which is forbidden by the count
//     check above).
func ValidateProductionWorkers(ids []string) error {
	// Defense-in-depth wildcard guard: the spec rules include "niente *",
	// but the canonical body below only checks count + uniqueness. A
	// future direct caller (test, new boot path) would silently admit
	// any worker under ["*", "valid"] because length==2 and the IDs are
	// distinct. Keeping the guard INSIDE the function rather than only
	// in the caller (Config.Validate) preserves the "non duplicata"
	// invariant — one canonical rule, enforced at the canonical point.
	for _, id := range ids {
		if id == "*" {
			return fmt.Errorf(
				"production worker IDs must not contain '*'; " +
					"configure the two canonical worker IDs in the allowlist",
			)
		}
	}

	if len(ids) != productionWorkerCount {
		return fmt.Errorf(
			"production requires exactly %d allowed workers, got %d",
			productionWorkerCount,
			len(ids),
		)
	}

	if ids[0] == ids[1] {
		return fmt.Errorf("production worker IDs must be unique")
	}

	return nil
}
