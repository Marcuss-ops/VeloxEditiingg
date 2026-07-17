// Package pipeline: HTTP handlers for the remote pipeline-run API.
//
// File: errors.go
// -----------------------------------------------------------------------------
// PR-DI-pipeline — Step 8a+8b of the pipeline.go split.
//
// What lives here
//   - writeHTTPError           — the canonical HTTP error response
//     primitive (Step 8a).
//   - ValidationError          — typed validation failure returned by
//     the request validator (Step 8b).
//   - internalValidationError  — generic internal error type for
//     sub-validators (Step 8b).
//
// Why they live together
//
//	Errors are a cross-cutting concern:
//	  - writeHTTPError           produces a uniform HTTP error envelope.
//	  - ValidationError          carries the typed shape of a request
//	                             validation failure (field + code +
//	                             message) so the handler can surface a
//	                             structured 400 response.
//	  - internalValidationError  carries the shape of a sub-validator
//	                             failure (channel auth, publish_at)
//	                             before the top-level validator wraps it
//	                             with the correct field name.
//
//	Co-locating them in errors.go keeps all error-related types in one
//	place so the shape of any failure (HTTP transport, request
//	validation, sub-validator) is easy to find and evolve.
//
// -----------------------------------------------------------------------------
package pipeline

import (
	"errors"
	"fmt"

	"github.com/gin-gonic/gin"
)

// writeHTTPError writes a JSON error response in the canonical
// {"ok": false, "error": ...} envelope.
//
// For simple errors (Status transport errors, network failures,
// generic 5xx), the envelope is just {ok, error}.
//
// For typed *ValidationError errors (returned by ValidateCreateRequest),
// the primitive auto-detects them via errors.As and enriches the
// envelope with the structured fields:
//
//	{"ok": false, "error": "...", "code": "...", "field": "..."}
//
// In the ValidationError case, the "error" field is set to the clean
// Message (not the formatted Error() string), so clients see only the
// human-readable message in the envelope. The code and field keys are
// omitted from the JSON when err is NOT a *ValidationError, so
// transport errors don't carry empty/unused validation keys.
//
// Examples:
//
//	writeHTTPError(c, http.StatusBadGateway, err)
//	writeHTTPError(c, http.StatusServiceUnavailable, errors.New("remote engine not configured"))
//	writeHTTPError(c, http.StatusBadRequest, valErr) // *ValidationError → structured envelope
func writeHTTPError(c *gin.Context, statusCode int, err error) {
	body := gin.H{
		"ok":    false,
		"error": err.Error(),
	}

	// Auto-detect *ValidationError: enrich envelope with code + field.
	// Transport errors (502/503/etc.) skip this branch and produce the
	// canonical {ok, error} envelope without empty validation keys.
	var valErr *ValidationError
	if errors.As(err, &valErr) {
		body["error"] = valErr.Message
		body["code"] = valErr.Code
		body["field"] = valErr.Field
	}

	c.JSON(statusCode, body)
}

// ValidationError is the typed validation failure returned by
// ValidateCreateRequest. It carries a field name, machine-readable
// code, and human-readable message so the handler can surface a
// structured 400 response with:
//
//	{"ok": false, "error": "...", "code": "...", "field": "..."}
//
// ValidationError implements the error interface so it can flow
// through ordinary error returns; callers typically assert the typed
// pointer (*ValidationError) to read Field/Code/Message for the
// 400 response body.
type ValidationError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
	Code    string `json:"code"`
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation: %s: %s", e.Field, e.Message)
}

// internalValidationError is the generic internal error type for
// sub-validators (channel auth, publish_at). It is NOT exported; the
// top-level ValidateCreateRequest wraps it into a *ValidationError
// with the correct field name.
type internalValidationError struct {
	Code    string
	Message string
}
