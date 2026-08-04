// Package deliveryplan is the canonical delivery_plan domain for the
// Velox stack — single source of truth for the shape rules,
// validation boundaries and typed error contract shared by both the
// enqueue-layer pre-flight (DataServer/internal/jobs/enqueue) and the
// in-tx storage-layer parser (DataServer/internal/store).
//
// Before this package, the same shape rules lived twice:
//
//   - DataServer/internal/jobs/enqueue/delivery_plan_validator.go
//   - DataServer/internal/store/delivery_plan_payload.go
//
// Two copies meant drift risk: the recent validator emits a single
// dot-notation field path ("delivery_plan.0.retry_budget") but the
// store parser had similar paths in a separate struct-literal emit
// site. After this extraction, NEITHER consumer can drift from the
// other because the canonical emit-and-path lives in one place.
//
// The package keeps ZERO outbound dependencies on the Velox stack:
// the only shared dependency is velox-shared/contract
// (DefaultDeliveryRetryBudget). The validator is library-safe
// (no socialclient, no store, no enqueue-state), so the build is
// independent of the DataServer wiring.
//
// Cross-package consumers reach the typed error transparently:
//
//	// store keeps back-compat via a type alias
//	type DeliveryPlanValidationError = deliveryplan.ValidationError
//
//	// enqueue keeps back-compat via the same alias
//	type validationError = deliveryplan.ValidationError
//
// Both aliases make `errors.As` and `errors.Is` work as if the types
// were defined in-package; the only observable change is that the
// type's pointer-equality now resolves through the shared identity.
package deliveryplan

import (
	"errors"
	"fmt"
)

// ErrDeliveryTargetRequired identifies a delivery request that omitted
// every explicit destination. Render-only jobs do not invoke Parse; once
// a delivery envelope is present, this sentinel gives HTTP callers a
// stable machine-readable failure code.
var ErrDeliveryTargetRequired = errors.New("delivery target required")

// ValidationError is the typed rejection surface. Cross-package
// callers reach the structured field path via errors.As + .Field
// and chain the underlying cause via errors.Is + Unwrap. The
// "<FieldPath>: <Msg>" envelope preserves the historical
// "field: message" format used by the enqueue and store validators
// pre-extraction, so substring-matched callers — legacy logs,
// integration_test golden assertions — keep working verbatim.
type ValidationError struct {
	FieldPath string
	Msg       string
	Wrapped   error
}

// Error returns the canonical "field: message" envelope. The
// leading "<FieldPath>:" matches both the previous enqueue validator
// (private *validationError) and the previous store validator
// (*DeliveryPlanValidationError); the test contract
// "delivery_plan.0.retry_budget: must be >= 0 (got -3)" is preserved.
func (e *ValidationError) Error() string {
	if e == nil {
		return ""
	}
	return e.FieldPath + ": " + e.Msg
}

// Field returns the structured field path (e.g.
// "delivery_plan.0.retry_budget"). Exposed via a getter so the
// FieldPath string cannot be mutated post-construction by callers
// that would otherwise reach into the struct.
func (e *ValidationError) Field() string {
	if e == nil {
		return ""
	}
	return e.FieldPath
}

// Message returns the human-readable rejection message WITHOUT the
// field-path prefix. Use Error() if you want the field+message
// concatenation.
func (e *ValidationError) Message() string {
	if e == nil {
		return ""
	}
	return e.Msg
}

// Unwrap returns the underlying cause so errors.Is / errors.As can
// inspect the original error (e.g. a socialclient.ErrPermanent
// wrapped by the enqueue pre-flight loop). Without this getter,
// callers can only inspect the formatted message, which is fragile
// across message refactors.
func (e *ValidationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Wrapped
}

// NewValidationError constructs a plain (no wrapped cause) error.
// Production callers inside Parse use struct literals directly; this
// constructor is the canonical entry point for tests + any future
// call site that builds a typed error from outside the parser path.
func NewValidationError(fieldPath, msg string) *ValidationError {
	return &ValidationError{FieldPath: fieldPath, Msg: msg}
}

// NewDeliveryTargetRequiredError constructs the canonical missing-target
// validation error while preserving errors.Is through the HTTP boundary.
func NewDeliveryTargetRequiredError() *ValidationError {
	return NewValidationErrorWrapped(
		"delivery_plan",
		"explicit delivery plan required; an explicit Drive destination is required",
		ErrDeliveryTargetRequired,
	)
}

// NewValidationErrorWrapped constructs a typed error that wraps
// cause. The enqueue pre-flight loop uses this on
// ErrPermanent/ErrAuth so the wrapped sentinel survives errors.Is at
// the HTTP envelope layer.
func NewValidationErrorWrapped(fieldPath, msg string, wrapped error) *ValidationError {
	return &ValidationError{FieldPath: fieldPath, Msg: msg, Wrapped: wrapped}
}

// FieldPath composes the canonical "delivery_plan.N.<field>" path
// used both in error messages and in HTTP envelope details.path.
// Centralised here so a future upgrade to JSON-Pointer form
// (RFC 6901) is a single rewrite, not a grep-and-replace across
// enqueue validator + store parser + tests.
func FieldPath(index int, field string) string {
	return fmt.Sprintf("delivery_plan.%d.%s", index, field)
}

// ValidationField extracts the structured field path from err,
// returning "" on a non-typed error or nil. Cross-package callers
// use this in expression position — see creatorflow's
// WriteResolverError and the resolver_http_errors_test rig:
//
//	if got := deliveryplan.ValidationField(err); got != "" {
//	    // 422 + details[0].path = got
//	}
//
// Returning "" (not error) on a non-ValidationError input lets the
// caller branch without short-circuiting their flow; a future
// refactor that returns the raw error message instead would force
// a call-site audit across every consumer.
func ValidationField(err error) string {
	if err == nil {
		return ""
	}
	var verr *ValidationError
	if errors.As(err, &verr) && verr != nil {
		return verr.Field()
	}
	return ""
}
