// Package util provides shared helper functions used across DataServer packages.
package util

import (
	"crypto/rand"
	"encoding/hex"
)

// GenerateID returns a 128-bit random hex string. It is safe for use as a
// unique identifier for artifacts, uploads, sources, and other entities.
// crypto/rand.Read on Linux's /dev/urandom cannot fail in practice; the
// fallback ensures the function never returns an empty string.
func GenerateID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "id_fb_" + hex.EncodeToString(b)
	}
	return hex.EncodeToString(b)
}

// PtrString returns a pointer to s. Useful for nullable string fields.
func PtrString(s string) *string { return &s }
