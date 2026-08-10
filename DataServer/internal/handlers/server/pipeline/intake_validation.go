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
	"strings"

	"github.com/gin-gonic/gin"
)

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

	details = append(details, validateSubmitScenes(req)...)

	details = append(details, validateSubmitAudioTracks(req)...)

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
