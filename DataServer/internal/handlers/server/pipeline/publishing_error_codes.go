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
//     producer picked from /publishing/targets IS routable in
//     the catalog-the-producer-sees, but Velox's own
//     delivery_destinations.enabled column flipped to 0 between
//     catalog discovery and job submission (canonical example:
//     an operator toggles `UPDATE delivery_destinations
//     SET enabled = 0 WHERE destination_id = '...'` to incident-
//     pause a single channel without re-syncing the entire
//     catalog).
//
//   - Catalog-zero-match, detected at POST /api/v1/publishing/
//     targets discovery time. The InstaeditLogin catalog yielded
//     ONE OR MORE entries for the workspace/platform, but NONE
//     of them satisfied the publishable predicate
//     (can_post=true AND capabilities.upload_video=true). Every
//     row carries a target_error_code explaining the reason
//     (BLOCKED_AUTH / TARGET_NOT_AVAILABLE), but no row has
//     can_post=true, so the producer cannot select a destination.
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
//   - BLOCKED_VELOX_DISABLED: `UPDATE delivery_destinations
//     SET enabled = 1 WHERE destination_id = '...'` and/or
//     re-sync via POST /api/v1/admin/destinations/sync.
//
//   - BLOCKED_NO_PUBLISHABLE_CHANNEL: see the per-row
//     target_error_code on each catalog entry (BLOCKED_AUTH,
//     TARGET_NOT_AVAILABLE) and act per the §0.2 chart.
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
// Subset of "Velox-side delivery envelope violations" — does NOT
// include catalog-side reauth_required-blocked cases, which are
// surfaced as catalog row target_error_code=BLOCKED_AUTH in
// /publishing/targets (see publish-job-payload docs).
const BlockedCodeVeloxDisabled = "BLOCKED_VELOX_DISABLED"

// BlockedCodeNoPublishableChannel is the canonical error code
// surfaced by POST /api/v1/publishing/targets (Velox-side catalog
// reflection layer) when the underlying InstaeditLogin catalog
// yielded AT LEAST ONE entry but ZERO entries satisfied the
// publishable predicate (can_post=true AND
// capabilities.upload_video=true).
//
// Emitted at the TOP-LEVEL `error.code` of the 200 OK envelope
// (the catalog endpoint stays 200 because the catalog fetch
// itself succeeded; it's the FILTERING verdict that surfaces
// the diagnostic). The `targets` array remains an empty array
// for backward compat with existing senders that read .targets.
//
// Does NOT include InstaEdit-side catalog fetch failures (those
// are mapped to social_api_failure / social_api_auth_failed /
// social_api_rate_limited / social_target_catalog_rejected in
// publishing_targets.go - the catalog handler's error switch).
const BlockedCodeNoPublishableChannel = "BLOCKED_NO_PUBLISHABLE_CHANNEL"
