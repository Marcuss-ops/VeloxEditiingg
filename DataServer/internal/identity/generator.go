// Package identity provides canonical ID generation for the server.
// All packages that need unique identifiers must use NewHex128.
package identity

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// NewHex128 returns a 128-bit random hex string (32 hex characters).
// crypto/rand.Read uses the OS CSPRNG (/dev/urandom on Linux), which
// cannot fail in practice. If it does, a timestamp-based fallback
// prevents returning an empty string.
func NewHex128() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		// Fallback: rand.Read failing on Linux means the system is
		// in an unrecoverable state. Produce a deterministic unique
		// ID as last resort so callers never receive an empty string.
		return "id_fb_" + time.Now().UTC().Format("20060102T150405.000000000")
	}
	return hex.EncodeToString(buf)
}
