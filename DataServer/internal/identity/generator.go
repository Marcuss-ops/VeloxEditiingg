// Package identity provides canonical ID generation for the server.
// All packages that need unique identifiers must use NewHex128.
package identity

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// NewHex128 returns a 128-bit random hex string (32 hex characters) or an error.
func NewHex128() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate identifier: %w", err)
	}
	return hex.EncodeToString(buf), nil
}
