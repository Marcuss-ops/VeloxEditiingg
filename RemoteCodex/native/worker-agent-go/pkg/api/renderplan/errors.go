// Package renderplan: render-plan validation error types.
//
// PlanError/PlanErrors are render-plan specific: they carry an explicit
// typed ErrorCode in addition to a field+message, suitable for surfacing
// structured API errors.
//
// ValidationError/ValidationErrors are aliases to the reusable
// pkg/validation.FieldError/FieldErrors so other Velox consumers (and tests)
// can share the same aggregation primitive.
package renderplan

import (
	"fmt"
	"strings"

	"velox-worker-agent/pkg/validation"
)

// RenderPlanVersion is the current contract version.
const RenderPlanVersion = "v1"

// ErrorCode represents a typed error code for render plan validation.
type ErrorCode string

const (
	ERR_PLAN_SCHEMA         ErrorCode = "ERR_PLAN_SCHEMA"
	ERR_PLAN_REQUIRED_FIELD ErrorCode = "ERR_PLAN_REQUIRED_FIELD"
	ERR_PLAN_INCONSISTENT   ErrorCode = "ERR_PLAN_INCONSISTENT"
)

// PlanError represents a typed error with code, field, and message.
type PlanError struct {
	Code    ErrorCode `json:"code"`
	Field   string    `json:"field,omitempty"`
	Message string    `json:"message"`
}

// Error formats the error for human-readable logs / HTTP responses.
func (e *PlanError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("[%s] %s: %s", e.Code, e.Field, e.Message)
	}
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

// PlanErrors is a collection of plan errors.
type PlanErrors []*PlanError

// Error joins every individual PlanError into a single message.
func (errs PlanErrors) Error() string {
	msgs := make([]string, len(errs))
	for i, e := range errs {
		msgs[i] = e.Error()
	}
	return fmt.Sprintf("render plan validation failed: %s", strings.Join(msgs, "; "))
}

// HasErrors returns true if there is at least one error.
func (errs PlanErrors) HasErrors() bool {
	return len(errs) > 0
}

// ValidationError is an alias for the reusable pkg/validation.FieldError.
// Keep the old name so the existing API surface is stable; the type itself
// (struct, not pointer) lives in pkg/validation so other packages can
// consume it without an extra dependency.
type ValidationError = validation.FieldError

// ValidationErrors is an alias for pkg/validation.FieldErrors.
type ValidationErrors = validation.FieldErrors
