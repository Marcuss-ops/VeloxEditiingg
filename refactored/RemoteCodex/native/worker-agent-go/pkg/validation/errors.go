// Package validation provides reusable validation primitives that are
// domain-agnostic: they can be used to validate API requests, RenderPlans,
// background-job payloads, or any other structured input.
//
// The package is intentionally minimal. It only exposes the error-aggregation
// data types and small string/identifier validators. Business rules such as
// "render job requires a voiceover" live in the consumer packages (e.g.
// pkg/api/renderplan).
package validation

import (
	"fmt"
	"strings"
)

// FieldError describes a single field-level validation failure.
//
// Field is a dotted path (e.g. "parameters.voiceover_paths") or a flat key
// (e.g. "job_id") — whichever the consumer prefers. Value is optional and
// used to record the offending input verbatim for diagnostics.
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
	Value   string `json:"value,omitempty"`
}

// Error implements error.
func (e *FieldError) Error() string {
	if e.Value != "" {
		return fmt.Sprintf("validation error on field '%s': %s (got: %s)", e.Field, e.Message, e.Value)
	}
	return fmt.Sprintf("validation error on field '%s': %s", e.Field, e.Message)
}

// FieldErrors is a collection of FieldError values that satisfies the error
// interface, suitable for fail-fast validation that accumulates all issues
// before failing.
type FieldErrors []FieldError

// Error implements error.
func (errs FieldErrors) Error() string {
	msgs := make([]string, len(errs))
	for i, e := range errs {
		msgs[i] = e.Error()
	}
	return fmt.Sprintf("validation failed: %s", strings.Join(msgs, "; "))
}

// HasErrors returns true if there is at least one error.
func (errs FieldErrors) HasErrors() bool {
	return len(errs) > 0
}

// Add appends a FieldError and returns the receiver (for fluent style).
func (errs *FieldErrors) Add(field, message, value string) {
	*errs = append(*errs, FieldError{Field: field, Message: message, Value: value})
}

// AddMsg appends a FieldError without recording the offending value.
func (errs *FieldErrors) AddMsg(field, message string) {
	*errs = append(*errs, FieldError{Field: field, Message: message})
}

// OrNil returns the receiver if it has errors, nil otherwise. Useful as a
// return value: `return errs.OrNil()`.
func (errs FieldErrors) OrNil() error {
	if errs.HasErrors() {
		return errs
	}
	return nil
}
