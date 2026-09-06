// Package identity provides canonical ID generation for the server.
// All packages that need unique identifiers must use NewHex128.
package identity

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// NewHex128 returns a 128-bit random hex string (32 hex characters) or an
// error. This is the SSOT for 128-bit random IDs in the Velox codebase
// (A5-2 audit): shared/identity.GenerateSecureWorkerID produces the same
// shape but silently returns an all-zero sentinel on RNG failure and is now
// deprecated; do not add callers to it.
func NewHex128() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate identifier: %w", err)
	}
	return hex.EncodeToString(buf), nil
}
