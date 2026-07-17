// Package pipeline: HTTP handlers for the remote pipeline-run API.
//
// File: errors.go
// -----------------------------------------------------------------------------
// PR-DI-pipeline — Step 8a of the pipeline.go split.
//
// What lives here (Step 8a)
//   - writeHTTPError — the canonical HTTP error response primitive.
//
// Why it exists
//
//	Before this step, every handler hand-rolled its error responses
//	inline as `c.JSON(http.StatusXxx, gin.H{"ok": false, "error": ...})`.
//	That created drift risk: each site picked its own envelope shape,
//	some included trace_id, some included hint, some forgot ok=false,
//	some accidentally set ok=true on errors. This primitive centralises
//	the canonical shape:
//
//	  {"ok": false, "error": "..."}
//
//	so all error responses are consistent across handlers and the
//	envelope shape can be evolved in one place.
//
// Step 8a extracts this primitive. Step 8b will layer on top the
// typed ValidationError struct + internalValidationError type that
// the validators return.
// -----------------------------------------------------------------------------
package pipeline

import (
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
