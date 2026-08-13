// Package m2mkeys / m2mkeys_crypto.go
//
// M2M secret handling: high-entropy generation, SHA-256 hashing, and
// constant-time comparison. The DB only ever holds the hex-encoded
// SHA-256 hash; plaintext is returned to the operator exactly once at
// creation.
package m2mkeys

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"strings"
)

// =====================================================================
// Helpers
// =====================================================================

// GenerateM2MSecret returns a 32-byte (64 hex-char) high-entropy
// plaintext secret suitable as the bearer value of an M2M API key.
// Hex output keeps the secret URL-safe AND copy-paste safe. crypto/rand
// panics on entropy failure (the system is broken). Sits next to
// HashM2MSecret so the create/lookup pair are auditable in one file.
//
// This is paired with HashM2MSecret: the admin POST endpoint calls
// GenerateM2MSecret() to create the plaintext, then HashM2MSecret()
// to obtain the value stored in m2m_api_keys.secret_hash. The
// plaintext is returned to the operator ONCE.
func GenerateM2MSecret() string {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		panic("m2mkeys: GenerateM2MSecret: crypto/rand.Read failed: " + err.Error())
	}
	return hex.EncodeToString(buf)
}

// HashM2MSecret computes the hex-encoded SHA-256 of secret. The
// canonical form is the lowercase hex of the SHA-256 digest. The
// middleware calls this on every request so the helper MUST be
// constant-time-ish; the actual constant-time compare happens in
// M2MSecretMatches.
func HashM2MSecret(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

// M2MSecretMatches reports whether storedHash (hex-encoded SHA-256)
// matches the hash of secret. Constant-time on length matched
// (Go's subtle.ConstantTimeCompare).
//
// NOTE: With 256-bit entropy tokens, the SQLite equality lookup
// (in GetActiveM2MAPIKeyBySecretHash) is non-constant-time on the
// hash value but brute-force from timing remains computationally
// infeasible — a side-channel attacker would need to leak log2 bits
// of position over millions of requests. The in-Go ConstantTimeCompare
// here is the belt-and-braces second line of defense.
func M2MSecretMatches(storedHash, secret string) bool {
	storedHash = strings.ToLower(strings.TrimSpace(storedHash))
	candidate := HashM2MSecret(secret)
	if len(storedHash) != len(candidate) {
		// Length mismatch => cannot match; return false without
		// touching the substring compare so timing of this path is
		// independent of storedHash content.
		return false
	}
	return subtle.ConstantTimeCompare([]byte(storedHash), []byte(candidate)) == 1
}
