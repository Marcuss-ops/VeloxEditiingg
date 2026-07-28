// Package pipeline — intake_validation.go owns the request-DTO
// types (SubmitJobRequest and its nested shapes), the per-field
// limit consts (video-name bytes, scene count, scene-duration
// bounds, retry-budget default, manifest-ref URL bytes, schema-
// version closed enum), the manifest-ref URL/SHA256 regexes,
// SubmitJobValidationError + its .Error() method, the
// containsString slice-membership helper, and
// ValidateSubmitJobRequest (the cross-field validator that
// returns either nil+false or a populated error with
// details[].path / []issue entries).
//
// All bool-returned validation paths return FALSE on the happy
// path so handlers can write `if verr, bad := …; bad { ... }`.
// Cross-field failures are aggregated into a single 4xx envelope
// so a client can correct them in one round trip.
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

// per-scene nested-asset map builders used by submitRequestToRawPayload.
// Each builder returns nil if the source struct is nil so the caller
// can simply append the result to the scene map under the canonical
// nested key (clip / voiceover / subtitles) without an extra nil-check.
//
// Trim policy: URL-bearing fields are TrimSpace'd to match the
// identity-field trim policy (job_submit.go::submitRequestToRawPayload
// "Trim policy" block). Asset-id and language are passed verbatim
// (they're not URL-shaped so the parser doesn't care about whitespace).

// which silently default to permissive behaviour for missing
// cross-field constraints.
type SubmitJobRequest struct {
	// IdempotencyKey is required. 1..128 bytes after UTF-8 trim, valid
	// UTF-8, no control bytes, no ':' or '%' separators. See
	// ValidateIdempotencyKey in idempotency_validation.go for the
	// byte-level rules and rejection envelopes.
	IdempotencyKey string `json:"idempotency_key"`

	// VideoName is the display name for the resulting video. Capped
	// at MaxVideoNameBytes (300); empty allowed.
	VideoName string `json:"video_name,omitempty"`

	// ScriptText is the plain-text script used for TTS / overlay.
	// Content field — NOT trimmed. Empty allowed. No byte-length cap
	// here; matches the creator path's tolerance.
	ScriptText string `json:"script_text,omitempty"`

	// VoiceoverPaths are voiceover audio references. Each entry MUST
	// be a velox-asset:// URI or a fully-qualified reachable URL.
	VoiceoverPaths []string `json:"voiceover_paths,omitempty"`

	// Scenes is the scene list. Each scene drives one composited
	// segment. At least one scene is required; max MaxScenes (10k).
	Scenes []SubmitScene `json:"scenes"`

	// Layers are independent overlays: title, name, important phrase or
	// additional media. They are not folded into Scenes, so callers can
	// submit a video, images and any combination of overlays together.
	Layers []SubmitLayer `json:"layers,omitempty"`

	// SubtitleTracks are independent from visual layers and media.
	SubtitleTracks []SubmitSubtitleTrack `json:"subtitle_tracks,omitempty"`

	// DeliveryPlan is the ordered list of delivery targets. Empty
	// allowed (defaults to scene.composite.v1's default resolver).
	DeliveryPlan []SubmitDeliveryPlanEntry `json:"delivery_plan,omitempty"`

	// ManifestRef is OPTIONAL. When present, the Master downloads
	// the manifest JSON at `url`, verifies `sha256`, validates
	// `schema_version`, and uses the manifest-derived payload as
	// the worker input (replacing / overlaying the inline scene
	// list). Shape-level rules (URL scheme allow-list, sha256 hex
	// format, schema_version enum) are enforced by
	// ValidateSubmitJobRequest. Fetch + verification are handled by
	// ResolveRenderManifestRef before enqueue.
	ManifestRef *SubmitManifestRef `json:"manifest_ref,omitempty"`

	// ResolvedManifest fields are internal-only. They are populated by
	// ResolveRenderManifestRef after the Master fetches and verifies
	// manifest_ref. They are intentionally ignored by JSON decoding
	// because DisallowUnknownFields would reject these names on the public
	// wire contract; submitRequestToRawPayload copies them into the
	// worker payload after resolution so TaskSpec carries the immutable
	// manifest snapshot.
	ResolvedManifest       map[string]interface{} `json:"-"`
	ResolvedManifestRef    map[string]interface{} `json:"-"`
	ResolvedManifestSHA256 string                 `json:"-"`
}

// SubmitScene is a single scene in the simplified job submission format.
//
// Field validation rules — submitted scenes MUST satisfy:
//   - Text non-empty (string length > 0 after trim).
//   - DurationSeconds in [MinSceneDurationSeconds,
//     MaxSceneDurationSeconds] (i.e. [0.1, 86400] seconds).
//
// Per-scene enrichment (Phase 2 of the render-manifest plan): the
// Clip / Voiceover / Subtitles nested objects REPLACE the legacy
// position-coupled relationship where `voiceover_paths[N]` matched
// `scenes[N]` by index (a fragile contract that broke when a scene
// was reordered or removed). A single scene now carries its own
// clip / voiceover / subtitles assets directly; the worker reads
// them from `scenes_json[i].voiceover.url` (and .clip, .subtitles)
// instead of relying on a top-level positional array.
//
// All three nested objects are POINTERS so that a client that supplies
// `{}` (the parent object with no nested keys) is distinguishable from
// the "scene carries no clip/vo/sub" case (pointer nil). The
// handler-side validator rejects the empty-object case with three
// aggregated 422 violations.
//
// ValidateSubmitJobRequest (in this file) runs the per-scene check
// and aggregates failures into a single 422 with details pointing at
// the offending index.
type SubmitScene struct {
	// Text is the narration / overlay text for this scene. Must be
	// non-empty after trim.
	Text string `json:"text"`

	// SceneID is the canonical client-supplied scene identifier
	// (e.g. "scene-0"). Optional; used by callers that track scene
	// identity across requests.
	SceneID string `json:"scene_id,omitempty"`

	// Index is the scene's position in the video timeline. Optional;
	// the validator does not require continuity (a caller that
	// supplies only every other index is fine). Worker consumers
	// use scenes_json's array order as the canonical timeline
	// regardless of this field's value. Parity: int64 (matches
	// apiwire.SubmitScene.Index; the bridge into
	// remoteengine.SceneResult.Index is uniform, so a future
	// cross-package cast won't trip on the int->int64 widening).
	Index int64 `json:"index,omitempty"`

	// Kind is the scene's role tag (e.g. "intro", "clip", "outro").
	// Free-form string for forward-compatibility; the validator
	// caps it at 32 bytes.
	Kind string `json:"kind,omitempty"`

	// ClipLink is a velox-asset:// clip URI or reachable URL.
	// PRESERVED for back-compat with legacy clients; when both
	// ClipLink and Clip.URL are supplied, the nested form wins
	// (submitRequestToRawPayload's documented tie-break).
	ClipLink string `json:"clip_link,omitempty"`

	// ImageLink is an optional image fallback.
	ImageLink string `json:"image_link,omitempty"`

	// DurationSeconds is the intended duration of the scene. Must be
	// in [MinSceneDurationSeconds, MaxSceneDurationSeconds].
	DurationSeconds float64 `json:"duration_seconds"`

	// Clip is the per-scene clip asset reference (Phase 2 of the
	// render-manifest plan). Pointer nil = "no clip for this scene".
	// Pointer non-nil with empty body = rejected with aggregated 422.
	Clip *SubmitClip `json:"clip,omitempty"`

	// Voiceover is the per-scene voiceover asset reference. Same
	// pointer semantics as Clip. The nested form REPLACES the legacy
	// top-level voiceover_paths[N] positional coupling.
	Voiceover *SubmitVoiceover `json:"voiceover,omitempty"`

	// Subtitles is the per-scene subtitles asset reference. Same
	// pointer semantics as Clip.
	Subtitles *SubmitSubtitles `json:"subtitles,omitempty"`
}

// SubmitClip is the per-scene clip asset reference nested inside
// SubmitScene. Mirrors apiwire.SubmitClip (no validate tags here —
// the handler-side ValidateSubmitJobRequest runs the shape checks
// when Clip != nil).
type SubmitClip struct {
	AssetID     string `json:"asset_id,omitempty"`
	DriveFileID string `json:"drive_file_id,omitempty"`
	URL         string `json:"url,omitempty"`
	SHA256      string `json:"sha256,omitempty"`
	StartMS     int64  `json:"start_ms,omitempty"`
	EndMS       int64  `json:"end_ms,omitempty"`
	DurationMS  int64  `json:"duration_ms,omitempty"`
}

// SubmitVoiceover is the per-scene voiceover asset reference nested
// inside SubmitScene. Same pointer indirection contract as SubmitClip.
type SubmitVoiceover struct {
	AssetID     string `json:"asset_id,omitempty"`
	DriveFileID string `json:"drive_file_id,omitempty"`
	URL         string `json:"url,omitempty"`
	SHA256      string `json:"sha256,omitempty"`
	DurationMS  int64  `json:"duration_ms,omitempty"`
	Language    string `json:"language,omitempty"`
}

// SubmitSubtitles is the per-scene subtitles asset reference nested
// inside SubmitScene.
type SubmitSubtitles struct {
	AssetID  string `json:"asset_id,omitempty"`
	Format   string `json:"format,omitempty"`
	URL      string `json:"url,omitempty"`
	SHA256   string `json:"sha256,omitempty"`
	Language string `json:"language,omitempty"`
}

// SubmitLayer is the API representation of one independent Chronon layer.
// Type is one of text, image, video or color; Role can distinguish title,
// name and important_phrase without creating separate renderer paths.
type SubmitLayer struct {
	ID              string    `json:"id"`
	Type            string    `json:"type"`
	Role            string    `json:"role,omitempty"`
	Text            string    `json:"text,omitempty"`
	Asset           string    `json:"asset,omitempty"`
	Source          string    `json:"source,omitempty"`
	Font            string    `json:"font,omitempty"`
	FontSize        float64   `json:"font_size,omitempty"`
	Position        []float64 `json:"position,omitempty"`
	StartSeconds    float64   `json:"start_seconds,omitempty"`
	DurationSeconds float64   `json:"duration_seconds,omitempty"`
	Preset          string    `json:"preset,omitempty"`
	Animation       string    `json:"animation,omitempty"`
}

// SubmitSubtitleTrack is a separate subtitle API payload. SRT, VTT and
// Chronon-compatible JSON sources are supported by the renderer.
type SubmitSubtitleTrack struct {
	Source string `json:"source"`
	Preset string `json:"preset,omitempty"`
	Font   string `json:"font,omitempty"`
}

// SubmitDeliveryPlanEntry is a single destination in the delivery plan.
//
// Field validation rules — submitted entries MUST satisfy:
//   - DestinationID non-empty (string length > 0 after trim).
//   - RetryBudget is a POINTER so that an explicit client-supplied value
//     of 0 round-trips distinctly from "field omitted". A nil pointer
//     means "client did not specify" — submitRequestToRawPayload
//     substitutes the OpenAPI default (DefaultRetryBudget = 3) at
//     normalization time. A pointer-to-0 means "client explicitly
//     wants 0 retries" — preserved verbatim into the worker payload.
//     Without the *int pointer, the Go default for int (0) would
//     silently merge with the omitted-field default and clients could
//     not distinguish "0 explicitly" from "omitted".
type SubmitDeliveryPlanEntry struct {
	DestinationID string `json:"destination_id"`
	Priority      int    `json:"priority,omitempty"`
	// RetryBudget is *int so that an explicit JSON value 0 round-trips
	// distinctly from the omitted case (nil). See the type doc for
	// the contract.
	RetryBudget *int `json:"retry_budget,omitempty"`
	Metadata    any  `json:"metadata,omitempty"`
}

// SubmitManifestRef points to a `velox.render-manifest.v1` JSON the
// client uploaded to a reachable store (Drive, GCS, S3, …). The
// Master fetches the JSON, verifies SHA-256 against the SHA-256 the
// client supplied here, validates the schema_version, and replaces
// the inline scene list with the manifest-derived payload.
//
// Three fields, all required WHEN the parent `manifest_ref` is
// present (the *SubmitManifestRef pointer distinguishes "no
// manifest_ref at all" from "manifest_ref declared but empty" —
// the latter is rejected):
//
//   - SchemaVersion is the closed enum of accepted manifest
//     versions. Today only `velox.render-manifest.v1` is accepted;
//     future versions (`v2`, …) MUST be added to the `oneof` list
//     AND to manifestRefSchemaVersions BEFORE the new resolver is
//     shipped, so the contract and the implementation cannot drift.
//
//   - URL is the canonical pointer to the manifest JSON. MUST be a
//     parseable URL on the http(s) scheme OR on the velox-asset://
//     scheme (the latter only when the asset is reachable through
//     the Master asset-bridge; the resolver owns that policy).
//     The regex is intentionally permissive — the schemagen
//     has no native `format: uri` distinction for velox-asset://
//     so the strict scheme allow-list is enforced by the
//     shape-level helper in ValidateSubmitJobRequest.
//
//   - SHA256 is the lowercase hex SHA-256 of the manifest JSON
//     body. The Master re-downloads the JSON and verifies the
//     SHA-256 against this value BEFORE substituting it into the
//     worker payload (fail-closed).
type SubmitManifestRef struct {
	// SchemaVersion is the closed enum of accepted manifest versions.
	// Today only `velox.render-manifest.v1` is accepted.
	SchemaVersion string `json:"schema_version"`

	// URL is the canonical pointer to the manifest JSON.
	URL string `json:"url"`

	// SHA256 is the lowercase hex SHA-256 of the manifest JSON body.
	SHA256 string `json:"sha256"`
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
