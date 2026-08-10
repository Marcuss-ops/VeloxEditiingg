package pipeline

import (
	"fmt"
	"regexp"

	"github.com/gin-gonic/gin"
)

// MaxVideoNameBytes is the byte-length cap on SubmitJobRequest.video_name.
// The cap protects log-line printers (some wire up to 4096-byte lines)
// and prevents accidental "paste the entire script in the title" classes
// of misuse. Picked to comfortably fit real video titles; raise only
// with operator approval.
//
// MUST mirror openapi.yaml:SubmitJobRequest.video_name.maxLength.
// Drift here silently breaks the contract: clients receive 422 for
// names the spec claimed were valid.
const MaxVideoNameBytes = 300

// MaxScenes is the upper bound on SubmitJobRequest.scenes count. The
// number is a defensive ceiling — a real-world video composition is
// rarely more than a few hundred scenes, so any larger count is
// almost certainly a misconfigured client. Anything above this bound
// is rejected with 422 invalid_payload to make the resource cost
// explicit (instead of letting it silently O(N) the resolver).
//
// MUST mirror openapi.yaml:SubmitJobRequest.scenes.maxItems.
// Drift here causes a silent acceptance/rejection asymmetry: clients
// sending a number of scenes spec-permitted but Go-rejected (or vice
// versa) get a confusing 422 on a structurally valid request.
const MaxScenes = 10000

// MinSceneDurationSeconds is the lower bound on SubmitScene.duration_seconds.
// Per the OpenAPI contract (and the validation helper below) sub-second
// durations are supported; zero or negative is rejected.
//
// MUST mirror openapi.yaml:SubmitScene.duration_seconds.minimum.
// Drift breaks sub-second scene cuts, an explicit feature for
// fine-grained montage work the Worker supports.
const MinSceneDurationSeconds = 0.1

// MaxSceneDurationSeconds is the upper bound, expressed in seconds.
// 86,400 s == 24 h, a generous ceiling that catches runaway defects
// (a client accidentally setting DurationSeconds = 1e10) without
// blocking legitimate full-day timelapses.
//
// MUST mirror openapi.yaml:SubmitScene.duration_seconds.maximum.
// Drift lets buggy clients silently inflate server-side resource
// budgets (a 1e10-second scene never finishes a paint cycle).
const MaxSceneDurationSeconds = 86400.0

// DefaultRetryBudget is the value stamped on a delivery_plan entry
// whose pointer is nil — i.e., the client did not declare a
// retry_budget and accepts the OpenAPI default. Mirrors
// openapi.yaml:SubmitDeliveryPlanEntry.retry_budget.default.
//
// MUST change in lockstep with the spec — if openapi.yaml's default
// changes, this constant changes. A divergence silently breaks
// the omitted-RetryBudget round-trip contract (clients that omit
// retry_budget get a different value than the spec advertises).
const DefaultRetryBudget = 3

// ManifestRefSchemaVersionV1 is the only manifest schema version the
// Master accepts today. Future versions MUST be added to the `oneof`
// list in apiwire.SubmitManifestRef.SchemaVersion AND to this
// constant set BEFORE the new resolver is shipped, so the spec and
// the implementation cannot drift.
//
// Closed enum today (one accepted value) but expressed as a list of
// constants so a future v2 addition is a one-line bump instead of a
// scattered refactor across the validator + the resolver.
var manifestRefSchemaVersions = []string{
	"velox.render-manifest.v1",
}

// MaxManifestRefURLBytes caps the wire-level byte length of
// manifest_ref.url at 2048 — a generous ceiling that fits every
// realistic https URL and velox-asset URI without truncating. The
// cap mirrors the apiwire.SubmitManifestRef.validate tag so a
// drift between the schema and the validator surfaces as a test
// failure rather than a silent acceptance asymmetry.
const MaxManifestRefURLBytes = 2048

// manifestRefURLRegexp matches a URL whose scheme is one of http,
// https, or velox-asset. The schemagen cannot emit a JSON Schema
// that distinguishes velox-asset:// from arbitrary URIs natively,
// so the regex is duplicated here (and in the apiwire validate
// tag) to keep the wire schema and the runtime validator in
// lockstep. Compile-once at package init so the validator's hot
// path doesn't pay the regex-compile cost on every request.
var manifestRefURLRegexp = regexp.MustCompile(`^(https?://|velox-asset://).+`)

// manifestRefSHA256Regexp matches a 64-character lowercase hex
// string. The hex-only check is intentionally strict (no
// uppercase, no `0x` prefix, no whitespace) so the byte-level
// rejection shape matches what the Master will compare against
// when it recomputes the SHA-256 of the downloaded manifest JSON.
var manifestRefSHA256Regexp = regexp.MustCompile(`^[0-9a-f]{64}$`)

// audioRoleValues is the closed set of accepted SubmitAudioTrack.Role values.
var audioRoleValues = []string{"voiceover", "scene_clip_audio", "background_music", "sfx"}

// containsString is a tiny slice-membership helper. Used by the
// manifest_ref.schema_version closed-enum check; inlined here so
// the validator has no third-party dependency on top of stdlib +
// gin (the project's existing dependency surface).
func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

// SubmitJobValidationError is the structured 4xx envelope returned by
// ValidateSubmitJobRequest when one (or more) of the cross-field
// constraints on SubmitJobRequest / SubmitScene / SubmitDeliveryPlanEntry
// fail. Designed for human-readable error reporting AND machine-
// readable code dispatch:
//
//   - Code is "invalid_payload" (always; the cross-field validators
//     are semantic, not byte-level).
//   - Reason is a short singular noun (e.g. "video_name_too_long",
//     "scenes_too_many", "scene_text_empty", "scene_duration_range",
//     "delivery_destination_empty").
//   - Details is the canonical {path, issue, ...} shape: path is the
//     JSON-pointer style dotted path to the offending field, issue
//     is a machine-readable short token.
//
// The handler maps every validation error to 422 with the error envelope
// (per openapi.yaml's ErrorEnvelope shape). One invalid scene out of
// 100 produces ONE error with details.path = "scenes.47.duration_seconds"
// — the handler emits 422 with all violations baked into details[] so
// the client sees the full picture in one round trip.
//
// Idempotency-key byte-level errors (e.g. too long, contains ":") are
// reported by ValidateIdempotencyKey separately (see
// idempotency_validation.go and SubmitJob() handler).
type SubmitJobValidationError struct {
	Code    string
	Reason  string
	Message string
	Details []gin.H
}

// Error implements the error interface so the helper is composable
// with `return err`.
func (e *SubmitJobValidationError) Error() string {
	return fmt.Sprintf("submit_job_validation: %s: %s (details=%d)", e.Code, e.Reason, len(e.Details))
}
