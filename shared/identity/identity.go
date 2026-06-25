// Package identity provides shared ID generation and normalization helpers
// used by both the Velox Master Server (DataServer) and the Worker Agent
// (worker-agent-go).
//
// Keeping these helpers in shared/ avoids drift between the two codebases
// (they used to duplicate the same logic in two different places).
package identity

import (
	"crypto/rand"
	"encoding/hex"
	"regexp"
	"strings"
)

// GenerateWorkerID produces a fresh worker identifier in the canonical
// "worker-xxxxxxxx" form, where "xxxxxxxx" is 8 lowercase hex characters
// derived from 4 bytes of crypto-random data.
//
// Determinism note: this function relies on crypto/rand. If the underlying
// RNG fails (extremely rare), generation falls back to a sentinel value so
// callers always receive a syntactically valid ID.
func GenerateWorkerID() string {
	b := make([]byte, 4) // 4 bytes = 8 hex chars
	if _, err := rand.Read(b); err != nil {
		return "worker-00000000"
	}
	return "worker-" + hex.EncodeToString(b)
}

// GenerateSecureWorkerID returns a more entropy-rich worker identifier —
// 16 random bytes encoded as 32 lowercase hex chars (no prefix).
//
// Use this when the ID must be globally unique across long-running clusters.
// Use GenerateWorkerID for compact human-readable IDs.
func GenerateSecureWorkerID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// Deterministic-but-recognizable fallback. Callers should treat the
		// prefix as a sentinel indicating RNG failure rather than a real ID.
		return "00000000000000000000000000000000"
	}
	return hex.EncodeToString(b)
}

// workerIDRegexp is the canonical shape enforced by RW-PROD-001 A4:
//
//   ^                   start
//   [a-z]               first character: lowercase ASCII letter (rejects Worker, W@rker)
//   [a-z0-9_-]{2,62}    2..62 trailing characters: lowercase ASCII letter, digit, hyphen, or underscore
//   $                   end
//
// Total length 3..64. The underscore is included on purpose: the
// canonical IP-derived form produced by NormalizeWorkerID is
//
//   host_57_129_132_133
//
// which would otherwise be silently rejected at validate time even
// though it is the documented output of NormalizeWorkerID (see
// shared/identity/identity_test.go TestNormalizeWorkerIDDedupesPrefixAndDots).
//
// This rejects empty IDs, IDs starting with digits or hyphens, IDs
// containing '.', '@', ' ', uppercase letters, or non-ASCII.
// Anchored (^...$) so partial matches do not count.
//
// Examples:
//
//   worker-8e98ce85        OK (15 chars)
//   host_57_129_132_133    OK (20 chars, post-Normalize)
//   Worker-001             REJECT (uppercase W)
//   w1                     REJECT (too short, 2 chars)
//   -worker                REJECT (starts with hyphen)
//   host-X                 OK  (hyphen allowed)
//   host_X1                OK  (underscore allowed)
var workerIDRegexp = regexp.MustCompile(`^[a-z][a-z0-9_-]{2,62}$`)

// IsValidWorkerID returns true iff id matches the canonical worker_id shape
// (3..64 chars, lowercase ASCII letter/digit/hyphen, starts with a letter).
//
// Pass any ID through this function AFTER calling NormalizeWorkerID so the
// canonical IP-derived shape survives validation:
//
//	id := identity.NormalizeWorkerID(raw)
//	if !identity.IsValidWorkerID(id) { /* refuse */ }
//
// This function is intentionally cheap (one regex match) and side-effect-free,
// so it is safe to call inside hot paths or test setups.
func IsValidWorkerID(id string) bool {
	if id == "" {
		return false
	}
	return workerIDRegexp.MatchString(id)
}

// NormalizeWorkerID canonicalizes IP-derived worker IDs.
//
// Two malformed-but-equivalent forms are normalized to a single canonical
// representation:
//
//  1. Repeated prefix: "host_host_host_57_129_132_133"
//     becomes            "host_57_129_132_133"
//  2. Old dotted format: "host_57.129.132.133"
//     becomes            "host_57_129_132_133"
//
// IDs not matching either pattern (e.g. "worker-8e98ce85", "w1", "test") are
// returned unchanged. Empty / whitespace-only input is also returned
// unchanged so callers can detect "no ID provided" themselves.
//
// Note (RW-PROD-001 A4): callers SHOULD additionally call IsValidWorkerID
// after NormalizeWorkerID to enforce the strict shape defined above.
func NormalizeWorkerID(id string) string {
	s := strings.TrimSpace(id)
	if s == "" {
		return id
	}
	if !strings.HasPrefix(s, "host_") && !strings.Contains(s, ".") {
		return id
	}
	for strings.HasPrefix(s, "host_") {
		s = strings.TrimPrefix(s, "host_")
	}
	s = strings.ReplaceAll(s, ".", "_")
	if s == "" {
		return id
	}
	return "host_" + s
}
