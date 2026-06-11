// Package renderplan provides the RenderPlan v1 contract for job validation.
package renderplan

import (
	"fmt"
	"strings"
)

// RenderPlanVersion is the current contract version.
const RenderPlanVersion = "v1"

// ErrorCode represents a typed error code for render plan validation.
type ErrorCode string

const (
	ERR_PLAN_SCHEMA          ErrorCode = "ERR_PLAN_SCHEMA"
	ERR_PLAN_REQUIRED_FIELD  ErrorCode = "ERR_PLAN_REQUIRED_FIELD"
	ERR_PLAN_INCONSISTENT    ErrorCode = "ERR_PLAN_INCONSISTENT"
)

// PlanError represents a typed error with code, field, and message.
type PlanError struct {
	Code    ErrorCode `json:"code"`
	Field   string    `json:"field,omitempty"`
	Message string    `json:"message"`
}

func (e *PlanError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("[%s] %s: %s", e.Code, e.Field, e.Message)
	}
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

// PlanErrors is a collection of plan errors.
type PlanErrors []*PlanError

func (errs PlanErrors) Error() string {
	msgs := make([]string, len(errs))
	for i, e := range errs {
		msgs[i] = e.Error()
	}
	return fmt.Sprintf("render plan validation failed: %s", strings.Join(msgs, "; "))
}

func (errs PlanErrors) HasErrors() bool {
	return len(errs) > 0
}

// ValidationError represents a contract validation failure.
type ValidationError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
	Value   string `json:"value,omitempty"`
}

func (e *ValidationError) Error() string {
	if e.Value != "" {
		return fmt.Sprintf("validation error on field '%s': %s (got: %s)", e.Field, e.Message, e.Value)
	}
	return fmt.Sprintf("validation error on field '%s': %s", e.Field, e.Message)
}

// ValidationErrors is a collection of validation errors.
type ValidationErrors []*ValidationError

func (errs ValidationErrors) Error() string {
	msgs := make([]string, len(errs))
	for i, e := range errs {
		msgs[i] = e.Error()
	}
	return fmt.Sprintf("render plan validation failed: %s", strings.Join(msgs, "; "))
}

func (errs ValidationErrors) HasErrors() bool {
	return len(errs) > 0
}
