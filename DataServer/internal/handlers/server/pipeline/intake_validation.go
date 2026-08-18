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

	details = append(details, validateSubmitDelivery(req)...)

	// Publications are validated against the shared canonical contract.
	// This remains separate from the renderer payload and adds the
	// request-level uniqueness/provider-options checks that the shared
	// package cannot know about.
	details = append(details, validateSubmitPublications(req.Publications)...)

	details = append(details, validateSubmitManifestRef(req)...)

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
