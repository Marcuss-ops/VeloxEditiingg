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
