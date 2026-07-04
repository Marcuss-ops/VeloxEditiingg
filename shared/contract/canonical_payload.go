// Package contract — canonical_payload.go codifies the canonical top-level
// shape for any process_video job payload going through the enqueue
// boundary. It is the single source of truth that the Velox hard-purity
// plan (Blocco 4) commits to: every writer at the data-server capture
// point must emit ONLY the keys listed in CanonicalTopLevelKeys, and must
// NEVER emit the legacy aliases listed in LegacyAliasKeys.
//
// The companion entry-point ValidatePayload(raw map[string]interface{}) error
// is a pure preflight gate — no side effects, no allocations beyond the
// error wrap. It is invoked from
// DataServer/internal/jobs/enqueue/enqueue.go and is suitable for any
// caller that wants to assert the canonical contract before persisting
// or dispatching a payload.
//
// Source-of-truth reconciliation:
//   - shared/contract/payload_v2.go (JobPayloadV2 typed struct + ToMap)
//     defines the typed stance; this file freezes the keyset + aliases
//     so writers that still produce untyped map[string]any blobs (script
//     ingestion, calendar cron, smoke fixtures) cannot drift.
//   - DataServer/internal/jobs/enqueue/delivery_plan_validator.go
//     mirrors the per-entry schema for delivery_plan; here we only
//     assert the top-level SHAPE (array OR single map) and defer per-
//     entry shape to that validator to avoid duplication.
//
// Any future top-level field MUST be added canonically in three places
// in lock-step: (a) this slice, (b) JobPayloadV2's struct + ToMap, and
// (c) the corresponding writer tests, or the contract-purity CI
// assertion will fail.
package contract

import (
	"errors"
	"fmt"
	"strings"
)

// CanonicalTopLevelKeys is the documented set of top-level keys for any
// process_video job payload at the enqueue boundary. The order is stable:
// lifecycle/identity first, then business fields, then numeric metadata,
// then routing/audit — matching the JSON marshaling order of
// JobPayloadV2 (see payload_v2.go).
//
// The Phase 4 additions ("items" from Step 2/8, "delivery_plan" from
// Step 4/8) are canonical here even though they are not yet folded into
// the JobPayloadV2 typed struct — they live in the worker payload layer
// and are emitted at the top level by every new writer.
var CanonicalTopLevelKeys = []string{
	// Lifecycle / canonical identity
	"contract_version",
	"job_id", "job_run_id", "correlation_id",
	"job_type", "version", "created_at", "updated_at",

	// Business fields
	"video_name", "script_text",
	"scenes_json", "scenes",
	"voiceover_paths",
	"items", // Step 2/8: items[].role scene/clip contract (worker payload layer)
	"audio_language_for_srt",
	"video_mode", "output_path",
	"drive_output_folder",
	"youtube_group", "channel_id", "output_video_id",
	"scene_image_paths", "image_source_map",

	// Numeric metadata
	"priority", "timeout_secs",
	"scene_count", "voiceover_count",
	"total_duration_secs", "scene_duration_secs",

	// Routing / audit
	"submitted_via", "source",
	"job_fingerprint", "status",

	// Step 4/8: delivery plan preflight gate
	"delivery_plan",
}

// LegacyAliasKeys is the strict denylist of legacy top-level aliases
// that writers MUST NOT emit. They were tolerated by readers past the
// V2 migration but their presence in a canonical payload is a writer
// bug (or a legacy in-flight row that the enqueue preflight must reject).
//
// History of the denylist:
//   - id             → job_id                (script_processor canonical id)
//   - run_id         → job_run_id            (creatorflow envelope)
//   - title          → video_name            (legacy submission field)
//   - voiceover_path → voiceover_paths[]     (legacy single-source field)
//   - audio_path     → voiceover_paths[]     (creatorflow single-source fallback)
//
// All other legacy middle aliases (script_id, script, source_text,
// project_name, audio_lang, language, output_directory, source_media,
// source_media_url, audio_source) are still tolerated by readers for the
// V2 ingest transition; they are NOT in this denylist because they live
// in NewJobPayloadV2's fallback chain and removing them would invalidate
// legacy SQLite row reads. They become candidates for a future denylist
// once the legacy-row backfill migrates them off the read path.
var LegacyAliasKeys = []string{
	"id",
	"run_id",
	"title",
	"voiceover_path",
	"audio_path",
}

// canonicalKeySet is an O(1) lookup mirror of CanonicalTopLevelKeys.
var canonicalKeySet = func() map[string]bool {
	m := make(map[string]bool, len(CanonicalTopLevelKeys))
	for _, k := range CanonicalTopLevelKeys {
		m[k] = true
	}
	return m
}()

// legacyAliasSet is an O(1) lookup mirror of LegacyAliasKeys.
var legacyAliasSet = func() map[string]bool {
	m := make(map[string]bool, len(LegacyAliasKeys))
	for _, k := range LegacyAliasKeys {
		m[k] = true
	}
	return m
}()

// IsCanonicalKey reports whether k is one of the documented canonical top-level keys.
func IsCanonicalKey(k string) bool {
	return canonicalKeySet[k]
}

// IsLegacyAlias reports whether k is a legacy alias that writers must NOT emit.
func IsLegacyAlias(k string) bool {
	return legacyAliasSet[k]
}

// ErrLegacyAlias is returned (wrapped) by ValidatePayload when a legacy
// alias key is present at the top level. Use errors.Is to detect.
var ErrLegacyAlias = errors.New("contract: legacy alias rejected")

// ErrShapeAnomaly is returned (wrapped) by ValidatePayload when a canonical
// key is present but its Go type does not match the expected shape.
// Use errors.Is to detect.
var ErrShapeAnomaly = errors.New("contract: shape anomaly")

// ErrNonCanonicalKey is returned (wrapped) by StrictValidatePayload when a
// key is present that is neither canonical nor a known legacy alias.
// Use errors.Is to detect. This is NOT enforced by the default
// ValidatePayload gate (see UnknownKeys for the non-rejecting observer).
var ErrNonCanonicalKey = errors.New("contract: non-canonical key rejected")

// ValidatePayload validates that the top-level payload map conforms to
// the canonical schema enforced by Velox at the enqueue boundary.
//
// Validation rules, in order:
//  1. Payload must be a non-nil map[string]interface{}.
//  2. No key may appear in LegacyAliasKeys (id, run_id, title,
//     voiceover_path, audio_path).
//  3. Canonical string fields, when present, must be string-shaped:
//     - job_id, video_name, script_text
//  4. Canonical array fields, when present, must be array-shaped:
//     - scenes, voiceover_paths, scene_image_paths, items
//  5. delivery_plan, when present, must be array- OR single-map-shaped.
//
// Unknown keys are tolerated by ValidatePayload (see UnknownKeys); use
// StrictValidatePayload if a hard reject is required.
//
// Returns nil on success. On failure, the error wraps either
// ErrLegacyAlias or ErrShapeAnomaly so callers can classify the failure
// with errors.Is.
func ValidatePayload(payload map[string]interface{}) error {
	if payload == nil {
		return fmt.Errorf("%w: payload is nil", ErrShapeAnomaly)
	}

	// Rule 2 — deny legacy aliases.
	for alias := range legacyAliasSet {
		if _, present := payload[alias]; present {
			return fmt.Errorf("%w: %q — use the canonical key documented in shared/contract/canonical_payload.go",
				ErrLegacyAlias, alias)
		}
	}

	// Rule 3 — string-shaped canonical fields.
	stringFields := []string{"job_id", "video_name", "script_text"}
	for _, field := range stringFields {
		v, ok := payload[field]
		if !ok || v == nil {
			continue
		}
		if _, ok := v.(string); !ok {
			return fmt.Errorf("%w: %q must be a string (got %T)",
				ErrShapeAnomaly, field, v)
		}
	}

	// Rule 4 — array-shaped canonical fields.
	arrayFields := []string{"scenes", "voiceover_paths", "scene_image_paths", "items"}
	for _, field := range arrayFields {
		v, ok := payload[field]
		if !ok || v == nil {
			continue
		}
		if !isArrayLike(v) {
			return fmt.Errorf("%w: %q must be an array (got %T)",
				ErrShapeAnomaly, field, v)
		}
	}

	// Rule 5 — delivery_plan accepts either a single-map OR an array form.
	// Per-entry shape validation lives in delivery_plan_validator.go to
	// keep this file at the top-level boundary only.
	if v, ok := payload["delivery_plan"]; ok && v != nil {
		if !isArrayLike(v) {
			if _, ok := v.(map[string]interface{}); !ok {
				return fmt.Errorf("%w: %q must be an array or single map (got %T)",
					ErrShapeAnomaly, "delivery_plan", v)
			}
		}
	}

	return nil
}

// StrictValidatePayload is ValidatePayload extended to also reject keys
// that are neither canonical nor legacy aliases. Use this in CI gate
// contexts (smoke fixtures, batch import scripts) where any drift key
// should fail closed. The standard enqueue path uses ValidatePayload so
// future-key additions don't break in-flight jobs.
func StrictValidatePayload(payload map[string]interface{}) error {
	if err := ValidatePayload(payload); err != nil {
		return err
	}
	for k := range payload {
		if canonicalKeySet[k] || legacyAliasSet[k] {
			continue
		}
		// Skip common noise keys that may be passed through event/log envelopes.
		if strings.HasPrefix(k, "_") || strings.HasPrefix(k, "x-") {
			continue
		}
		return fmt.Errorf("%w: %q is neither canonical nor a known legacy alias",
			ErrNonCanonicalKey, k)
	}
	return nil
}

// UnknownKeys returns the top-level keys in `payload` that are neither
// canonical nor legacy aliases. Callers can use this to log "potential
// new keys" for review without rejecting them outright. Returns nil if no
// unknown keys are present. Keys prefixed with "_" or "x-" are filtered
// (header / envelope noise).
func UnknownKeys(payload map[string]interface{}) []string {
	if payload == nil {
		return nil
	}
	var unknown []string
	for k := range payload {
		if canonicalKeySet[k] || legacyAliasSet[k] {
			continue
		}
		if strings.HasPrefix(k, "_") || strings.HasPrefix(k, "x-") {
			continue
		}
		unknown = append(unknown, k)
	}
	return unknown
}

// isArrayLike returns true for Go shapes that represent JSON arrays:
//   - []interface{}
//   - []string
//   - []map[string]interface{}
//   - any other kind.Slice whose element type is concrete
func isArrayLike(v interface{}) bool {
	switch v.(type) {
	case []interface{}, []string, []map[string]interface{}:
		return true
	}
	return false
}
