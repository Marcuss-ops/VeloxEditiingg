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
	"fmt"

	"github.com/gin-gonic/gin"
)

// writeHTTPError writes a JSON error response in the canonical
// {"ok": false, "error": ...} shape.
//
// Callers wrap their error string with errors.New(...) or pass an
// existing error directly. The primitive sets `ok` to false and
// forwards err.Error() as the `error` field.
//
// Examples:
//
//	writeHTTPError(c, http.StatusBadGateway, err)
//	writeHTTPError(c, http.StatusServiceUnavailable, errors.New("remote engine not configured"))
//
// For responses that need extra fields beyond the canonical envelope
// (trace_id, hint, code, field, ...), callers can fall back to a plain
// c.JSON call — but should keep the envelope consistent: start with
// gin.H{"ok": false, "error": ...} and add fields under it.
func writeHTTPError(c *gin.Context, statusCode int, err error) {
	c.JSON(statusCode, gin.H{
		"ok":    false,
		"error": err.Error(),
	})
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
