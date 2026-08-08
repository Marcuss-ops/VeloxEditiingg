// Package pipeline — publishing_error_codes.go
//
// Canonical error-code taxonomy for the publisher-side failure
// envelope §0.3.4 item 4 (External destination enabled). The
// split resolves the runtime ambiguity between two distinct
// failure modes that previously collapsed into the same operator
// diagnostic ("destination_id %q is globally disabled"):
//
//   - Velox-side globally-disabled destination row, detected at
//     POST /api/v1/jobs enqueue time. The destination_id the
//     producer received from InstaEdit is routable upstream, but
//     Velox's own delivery_destinations.enabled column flipped to 0
//     between opaque provisioning and job submission (for example,
//     an operator pauses one destination).
//
//   - Upstream-zero-match, determined by InstaEdit before a
//     publication is submitted. InstaEdit may report that one or
//     more channels exist but none satisfy its publishability
//     predicate; Velox does not calculate or mirror that catalog
//     verdict.
//
// This split was previously a NIT-2 follow-up on the original
// catalog-side code review. The diagnostic column in
// docs/SOCIAL_API_MIGRATION_RUNBOOK.md §0.2 item 4 now lists
// each code's failure-mode separately.
//
// Cross-repo contract:
//
//   - runtime: Velox surfaces these codes in JSON envelopes.
//
//   - sender: PipelineGen and any other trusted sender MUST
//     distinguish BLOCKED_VELOX_DISABLED from
//     BLOCKED_NO_PUBLISHABLE_CHANNEL because the operator-side
//     remediation differs:
//
//   - BLOCKED_VELOX_DISABLED: re-enable or reprovision the
//     opaque destination through the InstaEdit-owned lifecycle.
//
//   - BLOCKED_NO_PUBLISHABLE_CHANNEL: remediate the channel in
//     InstaEdit according to its own per-channel verdict.
//
// Stability: these are PUBLISHED codes. Adding new codes
// requires a cross-repo changelog; renaming or removing
// existing codes requires bumping
// `velox.instaedit.publish.v1` (the metadata contract_version
// stamped in delivery_plan[].metadata).
package pipeline

// BlockedCodeVeloxDisabled is the canonical error code surfaced
// by POST /api/v1/jobs (Velox-side enqueue-time gate) when the
// producer-selected destination_id exists in
// delivery_destinations but its `enabled` column is 0.
//
// Emitted under `details[].target_error_code` of the 422
// invalid_payload envelope returned by the
// `BatchDeliveryDestinationsStatus` pre-flight in job_submit.go.
//
// Subset of "Velox-side delivery envelope violations"; upstream
// reauth and publishability verdicts remain InstaEdit-owned.
const BlockedCodeVeloxDisabled = "BLOCKED_VELOX_DISABLED"

// BlockedCodeNoPublishableChannel is retained as a shared wire-code
// for upstream publication diagnostics. InstaEdit owns the catalog,
// computes channel publishability, and is responsible for surfacing
// this verdict to its callers; Velox must not expose a replacement
// catalog or derive this code from mirrored groups/channels.
const BlockedCodeNoPublishableChannel = "BLOCKED_NO_PUBLISHABLE_CHANNEL"
