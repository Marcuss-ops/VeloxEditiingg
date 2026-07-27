package pipeline

import (
	"strings"
	"unicode/utf8"
)

// MaxIdempotencyKeyLen is the upper byte-length bound for an idempotency_key
// submitted to POST /api/v1/jobs.
//
// The cap protects two layered-down invariants:
//
//  1. The forwarding key is built by routing.FormatForwardingKey which
//     percent-encodes `:` and `%` into 3-byte escape sequences. A 128-byte
//     input whose bytes are ALL `%` would explode to 384 bytes inside the
//     forwarding key string; the cap keeps the worst-case payload within
//     reasonable log/UI bounds.
//
//  2. The key is logged in a one-line struct-log entry. A 128-byte cap keeps
//     those lines terminal-friendly and below the standard 4096-byte log line.
//
// Anything longer than this MUST be rejected at the HTTP layer rather than
// truncated, because a truncation-published key would silently collide with
// a future request bearing the truncated prefix.
const MaxIdempotencyKeyLen = 128

// IdempotencyKeyError is a structured 400 error envelope for idempotency_key
// validation failures. It mirrors the ErrorEnvelope shape used elsewhere
// (e.g., creatorflow.WriteResolverError) so the API stays consistent across
// handlers: every validation failure returns the same {error, message, details}
// tuple with details.path pointing at the offending JSON-path (`idempotency_key`).
//
// Path is the *JSON-pointer* style dotted path to the offending field. For
// the top-level idempotency_key that is just "idempotency_key". A nested
// future validation (e.g., scenes[3].idempotency_token) would emit the
// full path here so error messages stay actionable for API clients.
type IdempotencyKeyError struct {
	// Code is the machine-readable error code (snake_case). Used by the
	// client to decide between retry-with-different-key vs. abort.
	Code string

	// Message is a human-readable explanation of the rejection.
	Message string

	// Reason is a short singular noun identifying WHAT was wrong (length,
	// encoding, control_char, separator, whitespace). It is the surface a
	// client UI can show to the user verbatim.
	Reason string

	// FieldLength / FieldByteOffset are diagnostic extras attached for the
	// 400 body so the operator can see the exact violation (length_observed,
	// offset_of_bad_byte). They are pointers so the empty case does not write
	// a misleading "0" or "nil" to the JSON.
	FieldLength   *int `json:"length,omitempty"`
	FieldByteOff  *int `json:"byte_offset,omitempty"`
}

// Error implements the error interface so the helper can be used idiomatically
// with `return err` from a handler. The HTTP envelope is reconstructed by the
// caller from the typed fields, NOT by stringifying Error(), so changing the
// wording of Error() is safe.
func (e *IdempotencyKeyError) Error() string {
	return "idempotency_key: " + e.Code + ": " + e.Message
}

// ValidateIdempotencyKey enforces the idempotency_key boundaries required
// for byte-exact idempotency across HTTP requests. The contract:
//
//  1. Key MUST be 1..MaxIdempotencyKeyLen bytes (no truncation; clients
//     that send huge keys are rejected).
//  2. Key MUST be valid UTF-8 (no silent byte replacement; a non-UTF-8
//     key would split multi-byte runes on byte-truncation).
//  3. Key MUST NOT contain: ':' (separates forwarding key columns), '%'
//     (percent-encoding marker), ASCII whitespace or control chars
//     (log injection, header pollution), NUL (truncates C-string tools),
//     or DEL.
//
// We deliberately reject rather than normalize because idempotency is a
// strict equality contract — silently re-writing the key breaks the
// round-trip between client and server-tracked value (the server would
// canonicalize "video 01" to "video_01" while the client keeps retrying
// "video 01" and never sees its own acknowledgement).
//
// The function returns (*IdempotencyKeyError, true) on rejection and
// (nil, false) on acceptance. The bool bundle lets callers write a
// uniform `if err, bad := ValidateIdempotencyKey(...); bad { ... }`
// branch without confusing the error for a non-error sentinel value.
func ValidateIdempotencyKey(raw string) (*IdempotencyKeyError, bool) {
	// Pre-trim once so length / byte-position checks work on the canonical
	// (post-trim) form. The client is allowed to send ASCII whitespace at
	// either edge; we strip it rather than reject to be lenient on the
	// common case of "client pastes key from a doc with stray newline".
	// We MUST validate AFTER trimming — pre-trim length is irrelevant.
	key := strings.TrimSpace(raw)

	if key == "" {
		return &IdempotencyKeyError{
			Code:          "invalid_payload",
			Reason:        "empty",
			Message:       "idempotency_key is required",
			FieldLength:   intPtr(len(key)),
			FieldByteOff:  nil,
		}, true
	}

	if !utf8.ValidString(key) {
		// Invalid UTF-8: locate the first invalid byte to give the client
		// a precise offset. The byte-truncation would have split a multi-
		// byte rune at exactly this offset — surfacing it lets the
		// operator debug their input. The helper returns >= 0 here
		// because utf8.ValidString already returned false; if the
		// helper ever regresses to returning -1, the raw value
		// propagates and the diagnostic stays debuggable rather than
		// being silently zeroed.
		offset := firstInvalidUTF8Offset(key)
		return &IdempotencyKeyError{
			Code:          "invalid_payload",
			Reason:        "encoding",
			Message:       "idempotency_key must be valid UTF-8",
			FieldLength:   intPtr(len(key)),
			FieldByteOff:  intPtr(offset),
		}, true
	}

	if len(key) > MaxIdempotencyKeyLen {
		return &IdempotencyKeyError{
			Code:        "invalid_payload",
			Reason:      "length",
			Message:     "idempotency_key exceeds 128 bytes",
			FieldLength: intPtr(len(key)),
		}, true
	}

	if off, bad := hasControlOrSeparatorByte(key); bad {
		return &IdempotencyKeyError{
			Code:        "invalid_payload",
			Reason:      "control_char",
			Message:     "idempotency_key contains a forbidden byte (' ' \\t \\r \\n NUL ':' '%' or DEL)",
			FieldLength: intPtr(len(key)),
			FieldByteOff: intPtr(off),
		}, true
	}

	return nil, false
}

// firstInvalidUTF8Offset returns the byte offset of the first invalid
// UTF-8 sequence in s, or -1 if the string is valid. Used to give the
// API client a precise diagnostic when they hit encoding errors.
//
// The implementation walks each byte, attempting a full utf8.DecodeRune
// at every position. DecodeRune returns (RuneError, 1) on a single-byte
// invalid prefix, which is enough to surface the offset.
func firstInvalidUTF8Offset(s string) int {
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			return i
		}
		i += size
	}
	return -1
}

// hasControlOrSeparatorByte returns the offset of the first byte that
// would either:
//
//   - confuse the forwarding key parser (`:` or `%`),
//   - pollute logs / headers (ASCII control chars: 0x00..0x1F, 0x7F),
//   - match a routing-layer forbidden char (whitespace 0x20),
//
// or (-1, false) if the key is clean. The offset is reported so the
// client can locate the bad byte in their own input during debugging.
//
// The check is byte-level (not rune-level) because EVERY forbidden byte
// is itself a single-byte ASCII codepoint — there is no multi-byte rune
// in this set, so a byte scan is correct AND faster than a rune scan.
func hasControlOrSeparatorByte(s string) (int, bool) {
	for i := 0; i < len(s); i++ {
		b := s[i]
		switch {
		case b == ':':
			// Forwarding key column separator.
			return i, true
		case b == '%':
			// Forwarding key percent-encoding prefix. Encoded '%'
			// would be %25 producers in the submitter would still
			// see '25' as the byte after '%' in logs / queries.
			return i, true
		case b <= 0x20:
			// ASCII NUL (0x00), control chars (0x01..0x1F),
			// and ASCII space (0x20). Includes '\t' (0x09),
			// '\n' (0x0A), '\r' (0x0D) — the ones that would
			// break log lines or HTTP headers.
			return i, true
		case b == 0x7F:
			// DEL — also a control character by Unicode definition.
			return i, true
		}
	}
	return -1, false
}

// intPtr is a tiny local helper so the validation result can express
// "field is unset" cleanly via a nil pointer rather than a sentinel -1.
// Avoids importing a third-party ptr/int utility just for this one
// field-shape decision.
func intPtr(v int) *int { return &v }
