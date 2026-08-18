// Package creatorflow — resolver_http_errors.go.
//
// Single canonical translator from the resolver-layer error contract
// to the HTTP error envelope used by every authenticated POST
// endpoint that delegates to Resolver.Resolve.
//
// Before this helper existed, the same error cascade was inlined at
// every caller (creator_push.go had the right shape; job_submit.go
// was missing the enqueue-layer validation branch entirely, which
// meant any enqueue-layer validation error surfaced as a 500). The
// inline copies also drifted over time. WriteResolverError is now
// the canonical mapping; failing to use it from a resolver-fed
// handler is a bug.
//
// Companion: openapi.yaml's ErrorEnvelope schema. Changes here MUST
// stay in lockstep with the validator's EXPECTED_ERROR_CODES set
// (and the ErrorCode enum).
package creatorflow

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"velox-server/internal/deliverycontract"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/store"
	"velox-server/internal/storecore"
	"velox-shared/contract/deliveryplan"
	"velox-shared/contract/domain"
)

// validationFieldExtractor is the indirection point used by
// WriteResolverError for the enqueue-layer typed validationError.
// In production it always equals enqueue.ValidationErrorField;
// tests override it to inject a deterministic field path without
// having to lean on the unexported enqueue.validationError type.
// Keeping the override as a package-private var (rather than a
// parameter or interface) lets callers stay oblivious to the test
// plumbing.
var validationFieldExtractor = enqueue.ValidationErrorField

// deliveryPlanFieldExtractor is the indirection point used by
// WriteResolverError for the store-layer typed
// DeliveryPlanValidationError. In production it always equals
// store.DeliveryPlanValidationField; tests override it to inject a
// deterministic field path. Same indirection rationale as
// validationFieldExtractor above.
//
// The store-layer typed error exists because store cannot import
// enqueue (the dep edge goes enqueue -> store) so the parser in
// internal/store/delivery_plan_payload.go must own its own typed
// error type. Without this extractor, the in-tx parser would emit
// plaintext errors that fall through to the default 500 branch —
// the same classification regression that P0 commit 72a455c closed
// for destination-existence, now closed for retry_budget and any
// future per-entry rejection in this parser.
var deliveryPlanFieldExtractor = store.DeliveryPlanValidationField

// extractUnifiedFieldPath returns the structured field path of
// either an enqueue.validationError OR a
// store.DeliveryPlanValidationError wrapped inside err, or "" if
// neither typed error is present. Used by WriteResolverError as the
// unified classification point so a future contributor can add
// another typed error source without touching the switch below —
// just wire it into one of the two extractors above (or add a
// third indirection var + a fallback line here).
//
// Order matters: enqueue is checked first because the enqueue-layer
// validationError is the canonical wrapper for any rejection that
// crosses the package boundary, and store's typed error is the
// last-resort typed classification for the in-tx parser path.
func extractUnifiedFieldPath(err error) string {
	if f := validationFieldExtractor(err); f != "" {
		return f
	}
	if f := deliveryPlanFieldExtractor(err); f != "" {
		return f
	}
	return ""
}

// idempotencyKeyDefault is the canonical JSON-pointer-style path
// emitted in the 409 detail when ErrIdempotencyKeyReused is raised
// without an underlying typed validationError. Production paths that
// carry the keyed identity as a top-level body field (POST /api/v1/jobs)
// and paths that nest it under an inner envelope (POST /api/v1/creator/
// jobs, where the canonical hash lives on the inner `payload`) BOTH
// reach this fallback when the typed error has no field path. Callers
// that need a context-specific label should wrap the error chain so
// the typed validationError carries it; the helper reads that via
// ValidationErrorField.
const idempotencyKeyDefault = "idempotency_key"

// WriteResolverError writes the canonical HTTP error envelope
// returned by any handler that delegates to creatorflow.Resolver.Resolve.
// Both SubmitJob (POST /api/v1/jobs) and CreatorPush (POST /api/v1/
// creator/jobs) call this helper; removing the call from either is
// a regression (the inline cascade silently dropped enqueue-layer
// validation errors to 500 in the [P0] historical memory's audit).
//
// The mapping is:
//
//	errors.Is(err, ErrResolverNotComplete)
//	    → 422 + "payload_incomplete"
//	      details: nil
//
//	errors.Is(err, ErrIdempotencyKeyReused)
//	    → 409 + "idempotency_key_reused"
//	      details: [{path: <derived>, issue: "hash_mismatch"}]
//	      where <derived> = validationFieldExtractor(err) if non-empty,
//	      else idempotencyKeyDefault ("idempotency_key"). This way a
//	      409 raised over a wrapped validationError with field
//	      "payload" (creator_push) surfaces "payload"; an unwrapped
//	      409 (submit_job) surfaces "idempotency_key" without
//	      for the callsite to thread a third arg through.
//
//	extractUnifiedFieldPath(err) != ""
//	    → 422 + "invalid_payload"
//	      details: [{path: <extracted-field>, issue: "invalid"}]
//	      Covers typed validation rejections from BOTH packages:
//	        - enqueue.validationError (delivery_plan missing /
//	          invalid, scenes empty, script_text oversized,
//	          destination_id missing, social_destination_id
//	          unrecognized, etc.)
//	        - store.DeliveryPlanValidationError (in-tx
//	          parseDeliveryPlanPayload rejections such as
//	          retry_budget < 0, priority < 0, duplicate
//	          destination_id, disabled entry, metadata
//	          serialization failure)
//
//	DomainError
//	    → its canonical HTTP status, code, field/issue detail, and
//	      message. Untyped errors are never classified by their text.
//
//	default
//	    → 500 + "resolver_failure"
//	      message: "failed to enqueue job"
//	      details: nil
//
// Nil err or nil gin.Context: noop. The function does NOT write
// anything in those states. Callers should still gate on `err !=
// nil` at the top of their handler block for clarity; the noop
// branch is defensive against accidental panics in test rigs.
func WriteResolverError(c *gin.Context, err error) {
	if c == nil || err == nil {
		return
	}

	switch {
	case errors.Is(err, ErrResolverNotComplete):
		writeErrorEnvelope(c, http.StatusUnprocessableEntity,
			"payload_incomplete",
			"payload is not complete enough to dispatch",
			nil)
	case errors.Is(err, storecore.ErrCreatorForwardingOwnershipConflict):
		writeErrorEnvelope(c, http.StatusConflict,
			"idempotency_key_reused",
			"idempotency key belongs to another client",
			gin.H{"path": idempotencyKeyDefault, "issue": "ownership_conflict"})
	case errors.Is(err, ErrIdempotencyKeyReused):
		// Derive the 409 detail path from any wrapped
		// validationError so a hash-conflict raised over a
		// context-specific subtree (e.g. creator_push's nested
		// "payload") flows through without callers having to
		// thread a third arg through the helper signature.
		path := validationFieldExtractor(err)
		if path == "" {
			path = idempotencyKeyDefault
		}
		writeErrorEnvelope(c, http.StatusConflict,
			"idempotency_key_reused",
			err.Error(),
			gin.H{"path": path, "issue": "hash_mismatch"})
	case func() bool {
		_, ok := domain.AsDomainError(err)
		return ok
	}():
		derr, ok := domain.AsDomainError(err)
		if !ok || derr == nil {
			break
		}
		detail := any(nil)
		if derr.Field != "" {
			detail = gin.H{"path": derr.Field, "issue": derr.Issue}
		}
		writeErrorEnvelope(c, derr.HTTPCode(), derr.Code, derr.PublicText, detail)
	case extractUnifiedFieldPath(err) != "":
		// Compatibility for typed ValidationError instances whose
		// caller supplied only the historical field extractor. This
		// branch still uses a typed field, never Error() text parsing.
		field := extractUnifiedFieldPath(err)
		writeErrorEnvelope(c, http.StatusUnprocessableEntity, "invalid_payload", err.Error(), gin.H{"path": field, "issue": "invalid"})
	case errors.Is(err, deliveryplan.ErrDeliveryTargetRequired),
		errors.Is(err, deliverycontract.ErrNoExplicitPlan):
		// Converge the bare missing-target sentinels through the canonical
		// DomainError projection so HTTP status, code, message and details
		// come from the single mapper. Typed ValidationError rejections
		// (e.g. deliveryplan.NewDeliveryTargetRequiredError) already project
		// via errors.As above and land in the DomainError branch.
		derr := domain.NewDeliveryTargetRequired("an explicit Drive destination is required", err)
		writeErrorEnvelope(c, derr.HTTPCode(), derr.Code, derr.PublicText,
			gin.H{"path": derr.Field, "issue": derr.Issue})
	default:
		writeErrorEnvelope(c, http.StatusInternalServerError,
			"resolver_failure",
			"failed to enqueue job",
			nil)
	}
}

// writeErrorEnvelope is the package-private body formatter used by
// WriteResolverError. The `details` key is omitted from the JSON
// when detail is nil so the response stays minimal for 4xx-without-
// details / 5xx paths (matches openapi.yaml's ErrorEnvelope where
// `details` is OPTIONAL).
func writeErrorEnvelope(c *gin.Context, status int, code domain.ErrorCode, message string, detail any) {
	body := gin.H{"ok": false, "error": code, "message": message}
	if detail != nil {
		body["details"] = []any{detail}
	}
	c.JSON(status, body)
}
