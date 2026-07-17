// Package remoteengine: typed error classification.
//
// Goal:
//   - Replace every string-based error classification (e.g.
//     strings.Contains(err.Error(), "4")) with a rigorous, typed error
//     model so callers can branch on error class without parsing free-form
//     messages.
//   - Provide a single ClassifyHTTPError entry point that maps an HTTP
//     status code + response body into a RemoteError, so the retry loop
//     can decide retry-vs-break deterministically.
//   - Keep the error compatible with errors.Is / errors.As so callers
//     outside the package (forwarding runner, pipeline handlers) can
//     inspect the class without importing this package's internals.

package remoteengine

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ── Error class ──────────────────────────────────────────────────────────────

// RemoteErrorClass enumerates the classification of a remote engine error.
type RemoteErrorClass string

const (
	// RemoteErrorValidation — 400, 422: the request payload is invalid.
	// Non-retryable; the caller must fix the request before resubmitting.
	RemoteErrorValidation RemoteErrorClass = "VALIDATION"

	// RemoteErrorAuthentication — 401, 403: token missing, expired, or
	// insufficient permissions. Non-retryable without operator action.
	RemoteErrorAuthentication RemoteErrorClass = "AUTHENTICATION"

	// RemoteErrorRateLimit — 429: the remote service is throttling.
	// Retryable with backoff; honour RetryAfter when present.
	RemoteErrorRateLimit RemoteErrorClass = "RATE_LIMIT"

	// RemoteErrorTransient — 408, 500, 502, 503, 504, network timeout:
	// the remote service had a temporary failure. Retryable with backoff.
	RemoteErrorTransient RemoteErrorClass = "TRANSIENT"

	// RemoteErrorPermanent — 404 or any other 4xx not covered above:
	// the resource does not exist or the request is definitively rejected.
	// Non-retryable.
	RemoteErrorPermanent RemoteErrorClass = "PERMANENT"

	// RemoteErrorMalformed — the remote service returned a response that
	// could not be decoded (truncated JSON, missing required fields).
	// Limited retry, then permanent.
	RemoteErrorMalformed RemoteErrorClass = "MALFORMED_RESPONSE"
)

// ── Typed error ──────────────────────────────────────────────────────────────

// RemoteError is the canonical error returned by the remote engine client.
// Every HTTP failure, network timeout, and malformed response is wrapped
// into this struct so callers can branch on Class without string-matching.
type RemoteError struct {
	Class      RemoteErrorClass
	StatusCode int           // 0 for non-HTTP errors (network timeout)
	RetryAfter time.Duration // parsed from Retry-After header; 0 = unspecified
	Code       string        // short machine-readable code (e.g. "HTTP_429")
	Message    string        // human-readable summary
	Body       string        // raw response body (truncated to 4 KB)
	Cause      error         // wrapped underlying error (network, JSON, etc.)
}

// Error implements the error interface. It returns a concise summary that
// includes the class, status code, and message — NOT the raw body, which
// may be large or contain sensitive data.
func (e *RemoteError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.StatusCode > 0 {
		return fmt.Sprintf("remote engine %s: HTTP %d — %s", e.Class, e.StatusCode, e.Message)
	}
	return fmt.Sprintf("remote engine %s: %s", e.Class, e.Message)
}

// Unwrap allows errors.Is / errors.As to reach the wrapped Cause.
func (e *RemoteError) Unwrap() error {
	return e.Cause
}

// IsRetryable returns true if the error class warrants a retry attempt
// (possibly after backoff). RATE_LIMIT, TRANSIENT, and MALFORMED_RESPONSE
// (limited) are retryable. VALIDATION, AUTHENTICATION, and PERMANENT are not.
func (e *RemoteError) IsRetryable() bool {
	if e == nil {
		return false
	}
	switch e.Class {
	case RemoteErrorRateLimit,
		RemoteErrorTransient,
		RemoteErrorMalformed:
		return true
	default:
		return false
	}
}

// IsPermanent returns true if the error class is definitively non-retryable.
func (e *RemoteError) IsPermanent() bool {
	if e == nil {
		return false
	}
	switch e.Class {
	case RemoteErrorValidation,
		RemoteErrorAuthentication,
		RemoteErrorPermanent:
		return true
	default:
		return false
	}
}

// ── Sentinel errors ───────────────────────────────────────────────────────────

// ErrNotConfigured is returned when the remote engine URL is empty.
var ErrNotConfigured = errors.New("remote engine not configured (set VELOX_REMOTE_ENGINE_URL)")

// ErrMalformedResponse is returned when the remote response cannot be decoded.
var ErrMalformedResponse = errors.New("remote engine returned a malformed response")

// ── Classification helpers ────────────────────────────────────────────────────

// ClassifyHTTPError maps an HTTP status code + response body + optional
// wrapped error into a *RemoteError with the correct Class.
//
// The mapping follows the Area 2 specification:
//
//	400, 422            → VALIDATION (permanent)
//	401, 403            → AUTHENTICATION (permanent)
//	408, 429            → RATE_LIMIT / TRANSIENT (retryable)
//	500, 502, 503, 504  → TRANSIENT (retryable)
//	404, other 4xx      → PERMANENT
//	5xx other than above → TRANSIENT
func ClassifyHTTPError(statusCode int, body string, cause error) *RemoteError {
	code := fmt.Sprintf("HTTP_%d", statusCode)
	msg := truncateBody(body, 256)

	re := &RemoteError{
		StatusCode: statusCode,
		Code:       code,
		Message:    msg,
		Body:       truncateBody(body, 4096),
		Cause:      cause,
	}

	switch {
	case statusCode == 400 || statusCode == 422:
		re.Class = RemoteErrorValidation
	case statusCode == 401 || statusCode == 403:
		re.Class = RemoteErrorAuthentication
	case statusCode == 429:
		re.Class = RemoteErrorRateLimit
	case statusCode == 408:
		re.Class = RemoteErrorTransient
	case statusCode >= 500 && statusCode <= 599:
		re.Class = RemoteErrorTransient
	case statusCode == 404:
		re.Class = RemoteErrorPermanent
	case statusCode >= 400 && statusCode < 500:
		// Any other 4xx (405, 406, 409, 410, 413, 415, etc.) is permanent.
		re.Class = RemoteErrorPermanent
	default:
		// Should not happen (called only on >= 400), but default to transient
		// so we never silently drop a retryable condition.
		re.Class = RemoteErrorTransient
	}

	return re
}

// ClassifyNetworkError wraps a non-HTTP error (connection refused, DNS
// failure, timeout, context cancellation) into a *RemoteError classified
// as TRANSIENT, unless the cause is context.Canceled which is permanent.
func ClassifyNetworkError(cause error) *RemoteError {
	if cause == nil {
		return nil
	}
	class := RemoteErrorTransient
	if errors.Is(cause, context.Canceled) {
		class = RemoteErrorPermanent
	}

	return &RemoteError{
		Class:   class,
		Code:    "NETWORK",
		Message: cause.Error(),
		Cause:   cause,
	}
}

// ClassifyDecodeError wraps a JSON decode / unmarshal failure into a
// *RemoteError classified as MALFORMED_RESPONSE. The caller may retry a
// limited number of times before treating it as permanent.
func ClassifyDecodeError(cause error, rawBody string) *RemoteError {
	if cause == nil {
		return nil
	}
	return &RemoteError{
		Class:   RemoteErrorMalformed,
		Code:    "DECODE",
		Message: cause.Error(),
		Body:    truncateBody(rawBody, 4096),
		// Wrap ErrMalformedResponse so callers can use errors.Is to detect
		// malformed responses without extracting *RemoteError.
		Cause: fmt.Errorf("%w: %v", ErrMalformedResponse, cause),
	}
}

// classifyHTTPResponse builds a *RemoteError from an HTTP error response,
// parsing the Retry-After header when present (for 429 responses).
func classifyHTTPResponse(resp *http.Response, respBody []byte, cause error) *RemoteError {
	re := ClassifyHTTPError(resp.StatusCode, string(respBody), cause)

	// Parse Retry-After for 429 responses.
	if resp.StatusCode == 429 {
		re.RetryAfter = ParseRetryAfter(resp.Header.Get("Retry-After"))
	}

	return re
}

// ParseRetryAfter parses the Retry-After HTTP header. It supports both
// delta-seconds and HTTP-date formats. Returns 0 if the header is absent
// or unparseable.
func ParseRetryAfter(header string) time.Duration {
	header = strings.TrimSpace(header)
	if header == "" {
		return 0
	}

	// Try delta-seconds first.
	if secs, err := strconv.Atoi(header); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second
	}

	// Try HTTP-date (RFC1123).
	if t, err := time.Parse(time.RFC1123, header); err == nil {
		d := time.Until(t)
		if d > 0 {
			return d
		}
		return 0
	}

	return 0
}

// ── Internal helpers ─────────────────────────────────────────────────────────

// truncateBody limits the body string to maxRunes characters, appending an
// ellipsis if truncation occurred.
func truncateBody(body string, maxRunes int) string {
	if len(body) <= maxRunes {
		return body
	}
	// Cut on rune boundary to avoid splitting multi-byte characters.
	runes := []rune(body)
	if len(runes) <= maxRunes {
		return body
	}
	return string(runes[:maxRunes]) + "…"
}
