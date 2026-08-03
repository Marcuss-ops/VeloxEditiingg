// Package pipeline — intake_validation.go owns the intake limits,
// manifest-reference regexes, structured validation error, and
// ValidateSubmitJobRequest cross-field validator. Request DTOs live in
// intake_types.go so the wire contract stays separate from its rules.
//
// All bool-returned validation paths return FALSE on the happy path so
// handlers can write `if verr, bad := …; bad { ... }`. Cross-field failures
// are aggregated into a single 4xx envelope so a client can correct them
// in one round trip.
//
// Caller in this package: job_submit.go (thin composer).
package pipeline

import (
	"fmt"
	"regexp"
	"strings"

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

// ValidateSubmitJobRequest is the canonical programmatically-invoked
// validator for SubmitJobRequest. It runs AFTER the strict JSON decoder
// has populated the struct, so every field is present (or zero-value)
// by the time this runs. It is the single source of truth for the
// [P1] OpenAPI-aligned cross-field constraints; the OpenAPI Python
// validator script confirms the same set of constraints at the spec
// level.
//
// The function accumulates ALL field-level violations into a single
// SubmitJobValidationError so the client can correct them in one round
// trip ("fix these 4 fields and resubmit") rather than learning about
// them one at a time across multiple requests.
//
// All bool-returned-from-validation paths return false on the happy
// path so the handler can write `if verr, bad := …; bad { ... }`.
//
// ValidateIdempotencyKey runs separately at the byte level (it is
// invoked BEFORE this helper at the handler level so a cheap byte-level
// rejected request never has its body fully traversed here).
func ValidateSubmitJobRequest(req SubmitJobRequest) (*SubmitJobValidationError, bool) {
	var details []gin.H

	// VideoName byte-length cap. Trimmed to match canonical identity-
	// field trim policy in submitRequestToRawPayload (TrimSpace).
	if v := strings.TrimSpace(req.VideoName); len(v) > MaxVideoNameBytes {
		details = append(details, gin.H{
			"path":     "video_name",
			"issue":    "max_length",
			"max":      MaxVideoNameBytes,
			"observed": len(v),
		})
	}

	// Scenes count: at least 1, at most MaxScenes for inline bodies.
	// A manifest_ref-only submission is allowed to omit scenes because
	// RenderManifestResolver substitutes the manifest-derived scene list
	// before enqueue. If an inline scene list is supplied alongside
	// manifest_ref it is still validated here; the resolver later replaces
	// it with the manifest as the source of truth.
	if len(req.Scenes) == 0 {
		if req.ManifestRef == nil {
			details = append(details, gin.H{
				"path":  "scenes",
				"issue": "empty",
			})
		}
	} else if len(req.Scenes) > MaxScenes {
		details = append(details, gin.H{
			"path":     "scenes",
			"issue":    "max_items",
			"max":      MaxScenes,
			"observed": len(req.Scenes),
		})
	}

	// Per-scene validation: text non-empty, duration in [0.1, 86400].
	for i, s := range req.Scenes {
		pathPrefix := fmt.Sprintf("scenes.%d", i)
		if strings.TrimSpace(s.Text) == "" {
			details = append(details, gin.H{
				"path":  pathPrefix + ".text",
				"issue": "empty",
			})
		}
		if s.DurationSeconds < MinSceneDurationSeconds || s.DurationSeconds > MaxSceneDurationSeconds {
			details = append(details, gin.H{
				"path":     pathPrefix + ".duration_seconds",
				"issue":    "out_of_range",
				"min":      MinSceneDurationSeconds,
				"max":      MaxSceneDurationSeconds,
				"observed": s.DurationSeconds,
			})
		}
	}

	// Per-scene nested-asset validation. Runs ONLY when the scene
	// at index i has a non-nil Clip / Voiceover / Subtitles
	// pointer. A nil pointer is the canonical "scene carries no
	// clip/vo/sub" path and MUST pass silently — legacy clients
	// never sent nested objects, so every existing client is
	// unaffected by the new fields' presence.
	//
	// Shape rules (matching apiwire validate tags):
	//   - URL: must be non-empty after trim and must match the
	//     http(s) + velox-asset:// scheme allow-list (same regex
	//     used for manifest_ref.url; the SSRF blocklist layer
	//     downstream enforces the egress policy separately).
	//   - SHA256: must be exactly 64 lowercase hex chars.
	//   - Subtitles.format: closed enum (ass / srt / vtt).
	//   - Language: 2-byte ISO 639-1 (best-effort — not strictly
	//     validated against the full ISO list; an empty string is
	//     permitted because the worker can fall back to the
	//     project default).
	//
	// An empty-object nested (pointer non-nil with all fields
	// empty) is rejected with at least one violation per nested
	// object that has an empty URL — the canonical "client sent
	// {}" shape must not silently pass.
	for i, s := range req.Scenes {
		if s.Clip != nil {
			pathPrefix := fmt.Sprintf("scenes.%d.clip", i)
			if trimmed := strings.TrimSpace(s.Clip.URL); trimmed == "" {
				details = append(details, gin.H{
					"path":  pathPrefix + ".url",
					"issue": "empty",
				})
			} else if !manifestRefURLRegexp.MatchString(trimmed) {
				details = append(details, gin.H{
					"path":     pathPrefix + ".url",
					"issue":    "unsupported_scheme",
					"observed": trimmed,
					"allowed":  []string{"https://", "http://", "velox-asset://"},
				})
			}
			if s.Clip.SHA256 != "" && !manifestRefSHA256Regexp.MatchString(s.Clip.SHA256) {
				details = append(details, gin.H{
					"path":     pathPrefix + ".sha256",
					"issue":    "malformed",
					"observed": s.Clip.SHA256,
					"expected": "64 lowercase hex characters ([0-9a-f]{64})",
				})
			}
		}
		if s.Voiceover != nil {
			pathPrefix := fmt.Sprintf("scenes.%d.voiceover", i)
			if trimmed := strings.TrimSpace(s.Voiceover.URL); trimmed == "" {
				details = append(details, gin.H{
					"path":  pathPrefix + ".url",
					"issue": "empty",
				})
			} else if !manifestRefURLRegexp.MatchString(trimmed) {
				details = append(details, gin.H{
					"path":     pathPrefix + ".url",
					"issue":    "unsupported_scheme",
					"observed": trimmed,
					"allowed":  []string{"https://", "http://", "velox-asset://"},
				})
			}
			if s.Voiceover.SHA256 != "" && !manifestRefSHA256Regexp.MatchString(s.Voiceover.SHA256) {
				details = append(details, gin.H{
					"path":     pathPrefix + ".sha256",
					"issue":    "malformed",
					"observed": s.Voiceover.SHA256,
					"expected": "64 lowercase hex characters ([0-9a-f]{64})",
				})
			}
		}
		if s.Subtitles != nil {
			pathPrefix := fmt.Sprintf("scenes.%d.subtitles", i)
			if trimmed := strings.TrimSpace(s.Subtitles.URL); trimmed == "" {
				details = append(details, gin.H{
					"path":  pathPrefix + ".url",
					"issue": "empty",
				})
			} else if !manifestRefURLRegexp.MatchString(trimmed) {
				details = append(details, gin.H{
					"path":     pathPrefix + ".url",
					"issue":    "unsupported_scheme",
					"observed": trimmed,
					"allowed":  []string{"https://", "http://", "velox-asset://"},
				})
			}
			if s.Subtitles.SHA256 != "" && !manifestRefSHA256Regexp.MatchString(s.Subtitles.SHA256) {
				details = append(details, gin.H{
					"path":     pathPrefix + ".sha256",
					"issue":    "malformed",
					"observed": s.Subtitles.SHA256,
					"expected": "64 lowercase hex characters ([0-9a-f]{64})",
				})
			}
			if s.Subtitles.Format != "" && !containsString([]string{"ass", "srt", "vtt"}, s.Subtitles.Format) {
				details = append(details, gin.H{
					"path":     pathPrefix + ".format",
					"issue":    "unsupported_value",
					"observed": s.Subtitles.Format,
					"allowed":  []string{"ass", "srt", "vtt"},
				})
			}
		}
	}

	// Per-audio-track validation: at least one of source_url or asset_id
	// must be non-empty (the Master resolves asset_id → source_url before
	// the worker sees the payload). When source_url IS provided, it must
	// match the http(s) + velox-asset:// allow-list. Role must be in the
	// closed enum (when supplied). Volume in [0.0, 2.0].
	for i, track := range req.AudioTracks {
		pathPrefix := fmt.Sprintf("audio_tracks.%d", i)
		trimmedURL := strings.TrimSpace(track.SourceURL)
		trimmedAsset := strings.TrimSpace(track.AssetID)
		if trimmedURL == "" && trimmedAsset == "" {
			details = append(details, gin.H{
				"path":  pathPrefix,
				"issue": "empty",
				"hint":  "provide source_url or asset_id",
			})
		} else if trimmedURL != "" && !manifestRefURLRegexp.MatchString(trimmedURL) {
			details = append(details, gin.H{
				"path":     pathPrefix + ".source_url",
				"issue":    "unsupported_scheme",
				"observed": trimmedURL,
				"allowed":  []string{"https://", "http://", "velox-asset://"},
			})
		}
		if track.Role != "" && !containsString(audioRoleValues, track.Role) {
			details = append(details, gin.H{
				"path":     pathPrefix + ".role",
				"issue":    "unsupported_value",
				"observed": track.Role,
				"allowed":  audioRoleValues,
			})
		}
		if track.Volume < 0.0 || track.Volume > 2.0 {
			details = append(details, gin.H{
				"path":     pathPrefix + ".volume",
				"issue":    "out_of_range",
				"min":      0.0,
				"max":      2.0,
				"observed": track.Volume,
			})
		}
	}

	// publishing_target is an alternate server-side selector. A non-empty
	// delivery_plan and a selector are ambiguous, so reject the combination
	// before any catalog/store lookup. An explicitly empty delivery_plan is
	// treated as absent for backward compatibility with clients that always
	// serialize arrays.
	if req.PublishingTarget != nil {
		target := req.PublishingTarget
		path := "publishing_target"
		if target.WorkspaceID <= 0 {
			details = append(details, gin.H{"path": path + ".workspace_id", "issue": "must_be_positive"})
		}
		targetType := strings.TrimSpace(target.Type)
		if targetType != "channel" && targetType != "group" {
			details = append(details, gin.H{
				"path":    path + ".type",
				"issue":   "unsupported_value",
				"allowed": []string{"channel", "group"},
			})
		}
		if len(req.DeliveryPlan) > 0 {
			details = append(details, gin.H{
				"path":  path,
				"issue": "conflicts_with_delivery_plan",
			})
		}
		switch targetType {
		case "channel":
			if strings.TrimSpace(target.DestinationID) == "" {
				details = append(details, gin.H{"path": path + ".destination_id", "issue": "required_for_channel"})
			}
			if target.GroupID != 0 {
				details = append(details, gin.H{"path": path + ".group_id", "issue": "forbidden_for_channel"})
			}
		case "group":
			if target.GroupID <= 0 {
				details = append(details, gin.H{"path": path + ".group_id", "issue": "required_for_group"})
			}
			if strings.TrimSpace(target.DestinationID) != "" {
				details = append(details, gin.H{"path": path + ".destination_id", "issue": "forbidden_for_group"})
			}
		}
	}

	// Per-delivery-plan-entry validation: destination_id non-empty
	// after trim. RetryBudget has NO upper bound at the OpenAPI layer
	// (only "minimum: 0"); allowing 0 is the whole point of the *int
	// change so the explicit zero-round-trip contract holds.
	for i, d := range req.DeliveryPlan {
		pathPrefix := fmt.Sprintf("delivery_plan.%d", i)
		if strings.TrimSpace(d.DestinationID) == "" {
			details = append(details, gin.H{
				"path":  pathPrefix + ".destination_id",
				"issue": "empty",
			})
		}
	}

	// Publications are validated against the shared canonical contract.
	// This remains separate from the renderer payload and adds the
	// request-level uniqueness/provider-options checks that the shared
	// package cannot know about.
	details = append(details, validateSubmitPublications(req.Publications)...)

	// ManifestRef shape validation. Runs ONLY when the pointer is
	// non-nil — a nil pointer is the "client did not opt in" path
	// and MUST pass through this validator without complaint. When
	// the pointer is non-nil the body is treated as the canonical
	// shape contract: schema_version must be in the closed enum,
	// url must match the http(s) + velox-asset:// allow-list and
	// be 1..MaxManifestRefURLBytes after trim, sha256 must be
	// exactly 64 lowercase hex characters.
	//
	// The actual fetch + SHA-256 verification happens later in
	// ResolveRenderManifestRef; this layer is intentionally byte-level
	// only so the rejection paths are order-stable and a malformed
	// manifest_ref returns 422 invalid_payload BEFORE any downstream cost.
	if req.ManifestRef != nil {
		mr := req.ManifestRef
		mrPath := "manifest_ref"

		// schema_version must be in the closed enum. The allowed
		// list is the source of truth — changing it requires
		// bumping apiwire.SubmitManifestRef's `oneof` tag too, so
		// the wire schema and the runtime validator agree.
		if !containsString(manifestRefSchemaVersions, mr.SchemaVersion) {
			details = append(details, gin.H{
				"path":     mrPath + ".schema_version",
				"issue":    "unsupported_value",
				"observed": mr.SchemaVersion,
				"allowed":  manifestRefSchemaVersions,
			})
		}

		// url: 1..MaxManifestRefURLBytes after trim AND must match
		// the http(s) + velox-asset:// allow-list. The regex is
		// duplicated from the apiwire validate tag because the
		// schemagen cannot express the velox-asset:// scheme
		// natively; duplicating it here keeps the wire schema
		// and the runtime validator in lockstep.
		trimmedURL := strings.TrimSpace(mr.URL)
		if trimmedURL == "" {
			details = append(details, gin.H{
				"path":  mrPath + ".url",
				"issue": "empty",
			})
		} else if len(trimmedURL) > MaxManifestRefURLBytes {
			details = append(details, gin.H{
				"path":     mrPath + ".url",
				"issue":    "max_length",
				"max":      MaxManifestRefURLBytes,
				"observed": len(trimmedURL),
			})
		} else if !manifestRefURLRegexp.MatchString(trimmedURL) {
			details = append(details, gin.H{
				"path":     mrPath + ".url",
				"issue":    "unsupported_scheme",
				"observed": trimmedURL,
				"allowed":  []string{"https://", "http://", "velox-asset://"},
			})
		}

		// sha256: exactly 64 lowercase hex characters. The
		// hex-only check is intentionally strict (lowercase) so
		// a future drift to mixed case is caught at the wire
		// rather than silently producing a mismatch inside the
		// resolver.
		if !manifestRefSHA256Regexp.MatchString(mr.SHA256) {
			details = append(details, gin.H{
				"path":     mrPath + ".sha256",
				"issue":    "malformed",
				"observed": mr.SHA256,
				"expected": "64 lowercase hex characters ([0-9a-f]{64})",
			})
		}
	}

	if len(details) == 0 {
		return nil, false
	}

	return &SubmitJobValidationError{
		Code:    "invalid_payload",
		Reason:  "validation_failed",
		Message: fmt.Sprintf("request body has %d validation failure(s) (see details)", len(details)),
		Details: details,
	}, true
}

// SubmitJob handles POST /api/v1/jobs.
//
// This is the simplified, external-friendly entry point for job
// submission. It converts the flat request into the Creator-push
// format and delegates to the same resolver machinery.
//
// The identity tuple is derived from the idempotency_key with stable
// cardinality:
//   - source_provider:    ExternalAPISourceProvider ("external_api")
//   - source_job_id:      the (validated) idempotency_key
//   - target_executor_id: JobSubmitTargetExecutorID ("scene.composite.v1")
//
// Stable provider cardinality is a deliberate invariant: it lets
// dashboards aggregate "all external_api jobs" without scanning
// per-key labels, and keeps future M2M-auth client_ids additive
// rather than redefining the provider dimension.
//
// Decoding strategy: the handler uses json.NewDecoder(...).Decode
// with DisallowUnknownFields — NOT c.ShouldBindJSON. The strict
// decoder rejects any field name not on the struct, so a typo'd
// json blob (e.g. {"ideliverency_key": "...", ...}) fails with 400
// invalid_json BEFORE we touch downstream code. Gin's binding tag
// machinery is permissive for cross-field validation and silently
// accepts unknown fields, which is the wrong default for an
// external-API surface.
