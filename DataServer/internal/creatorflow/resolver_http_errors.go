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
// stay in lockstep with the validator's REQUIRED_SCHEMAS +
// EXPECTED_ERROR_CODES sets (and the ErrorCode enum).
package creatorflow

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/jobs/enqueue"
)

// ErrIdempotencyKeyReused is the canonical sentinel for the 409
// `idempotency_key_reused` case. The resolver returns (nil, this)
// when the existing forwarding row's payload_sha256 differs from the
// SHA of the freshly-rebuilt worker payload.
//
// IMPORTANT: the literal message here MUST stay byte-equal with any
// future re-declaration in resolver_types.go. errors.Is compares by
// pointer identity, so two distinct fmt.Errorf calls with the same
// string are NOT equal — the P0 payload-hash followup must use
// this exported symbol (or a wrapping helper around it) instead of
// re-introducing its own var. Carrying the message in a single place
// also makes the OpenAPI 409 example's "idempotency_key reused
// with a different payload" match the resolver output byte-for-byte.
//
// Lives here (rather than in resolver_types.go) until the payload-
// hash followup lands, so WriteResolverError compiles today without
// scope expansion.
var ErrIdempotencyKeyReused = fmt.Errorf("creatorflow: Resolve: idempotency key reused with a different payload")

// validationFieldExtractor is the indirection point used by
// WriteResolverError. In production it always equals
// enqueue.ValidationErrorField; tests override it to inject a
// deterministic field path without having to lean on the
// unexported enqueue.validationError type. Keeping the override as
// a package-private var (rather than a parameter or interface) lets
// callers stay oblivious to the test plumbing.
var validationFieldExtractor = enqueue.ValidationErrorField

// WriteResolverError writes the canonical HTTP error envelope
// returned by any handler that delegates to creatorflow.Resolver.Resolve.
// The mapping is:
//
//	errors.Is(err, ErrResolverNotComplete)
//	    → 422 + "payload_incomplete"
//	      message: "payload is not complete enough to dispatch"
//	      details: nil
//
//	errors.Is(err, ErrIdempotencyKeyReused)
//	    → 409 + "idempotency_key_reused"
//	      details: [{path: idempotencyField, issue: "hash_mismatch"}]
//
//	validationFieldExtractor(err) != ""
//	    → 422 + "invalid_payload"
//	      details: [{path: <extracted-field>, issue: "invalid"}]
//	      (covers all enqueue-layer *validationError rejections,
//	      e.g. "delivery_plan[0].external_destination_id",
//	      "scenes", "script_text", etc.)
//
//	strings.Contains(strings.ToLower(err.Error()), "required")
//	    → 422 + "invalid_payload"
//	      details: nil (no typed field path available)
//	      (captures un-typed resolver-internal validation
//	      messages such as "payload is required" or
//	      "source_provider and source_job_id are required",
//	      which without this rule would incorrectly bubble up
//	      as 500.)
//
//	default
//	    → 500 + "resolver_failure"
//	      message: "failed to enqueue job"
//	      details: nil
//
// idempotencyField is the JSON path surfaced in the 409 detail.
// Pass "idempotency_key" when the caller has a content-level dedup
// handle (POST /api/v1/jobs) or "payload" when the dedup is
// embedded in a nested envelope (POST /api/v1/creator/jobs, where
// the canonical hash-mismatch lives on the inner `payload` object).
//
// Nil err or nil gin.Context: noop. The function does NOT write
// anything in those states. Callers should still gate on `err !=
// nil` at the top of their handler block for clarity; the noop
// branch is defensive against accidental panics in test rigs.
func WriteResolverError(c *gin.Context, err error, idempotencyField string) {
	if c == nil || err == nil {
		return
	}

	switch {
	case errors.Is(err, ErrResolverNotComplete):
		writeErrorEnvelope(c, http.StatusUnprocessableEntity,
			"payload_incomplete",
			"payload is not complete enough to dispatch",
			nil)
	case errors.Is(err, ErrIdempotencyKeyReused):
		writeErrorEnvelope(c, http.StatusConflict,
			"idempotency_key_reused",
			err.Error(),
			gin.H{"path": idempotencyField, "issue": "hash_mismatch"})
	case validationFieldExtractor(err) != "":
		field := validationFieldExtractor(err)
		writeErrorEnvelope(c, http.StatusUnprocessableEntity,
			"invalid_payload",
			err.Error(),
			gin.H{"path": field, "issue": "invalid"})
	case strings.Contains(strings.ToLower(err.Error()), "required"):
		// Un-typed resolver validation. We deliberately do NOT
		// pattern-match non-required keywords (e.g. "delivery_plan"
		// or "destination_id" in error text) — those should flow
		// through validationFieldExtractor via the typed
		// enqueue.validationError path. If they don't, fix the
		// enqueue layer, don't paper over it here.
		writeErrorEnvelope(c, http.StatusUnprocessableEntity,
			"invalid_payload",
			err.Error(),
			nil)
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
func writeErrorEnvelope(c *gin.Context, status int, code, message string, detail any) {
	body := gin.H{"ok": false, "error": code, "message": message}
	if detail != nil {
		body["details"] = []any{detail}
	}
	c.JSON(status, body)
}
