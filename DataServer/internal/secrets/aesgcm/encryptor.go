// Package aesgcm provides AES-256-GCM at-rest encryption for OAuth secrets
// stored in SQLite. Each encrypted value is a self-describing BLOB laid out
// as `nonce(12 bytes) || ciphertext || gcm_tag(16 bytes)` so the decryptor
// needs no side metadata to recover the plaintext.
//
// Key resolution policy (see LoadFromEnv):
//   - VELOX_YT_OAUTH_TOKEN_KEY       — base64-encoded 32-byte key (preferred)
//   - VELOX_YT_OAUTH_TOKEN_KEY_FILE  — path to a file holding the same
//     base64-encoded key (avoids leaving
//     the secret in the process env table)
//
// If neither is set AND the caller passes requireIfMissing=false, the
// loader returns a nil *Encryptor with a nil error — operators can boot
// the server in a degraded mode where OAuth secrets are NOT persisted to
// the new youtube_oauth_tokens table, only to the legacy JSON files. The
// service MUST check for nil and refuse OAuth operations in that mode.
//
// The key is never logged or echoed. NewEncryptor validates length (32
// bytes for AES-256) and returns a typed error so callers can decide how
// to react (Refuse / FailFast / Warn).
package aesgcm

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// ErrNoKey is returned when neither env var resolves a usable key and the
// caller required strict loading. Callers typically map this to a refuse
// of token persistence rather than a full process abort.
var ErrNoKey = errors.New("aesgcm: no encryption key configured")

// EnvVarKey is the primary env var the loader reads.
const EnvVarKey = "VELOX_YT_OAUTH_TOKEN_KEY"

// EnvVarKeyFile is the file-based fallback (used in container secrets).
const EnvVarKeyFile = "VELOX_YT_OAUTH_TOKEN_KEY_FILE"

// NonceSize is the standard GCM nonce length (96 bits / 12 bytes).
const NonceSize = 12

// KeySizeBytes is the AES-256 key length (32 bytes).
const KeySizeBytes = 32

// currentKeyVersion is bumped on every key rotation. Stored alongside
// each ciphertext so a future rotation can detect which key was used.
const currentKeyVersion = 1

// Encryptor wraps a single AES key + key version. Construct via
// NewEncryptor (raw key) or LoadFromEnv (env-resolved).
type Encryptor struct {
	key        []byte // exactly KeySizeBytes long; never logged
	keyVersion int    // exposed so callers can persist alongside ciphertext
}

// NewEncryptor validates the key length and returns an Encryptor bound to
// keyVersion=1. Returns an error if key is not exactly 32 bytes — we
// reject shorter keys explicitly because AES-128 is no longer acceptable
// for storage of OAuth credentials.
func NewEncryptor(key []byte) (*Encryptor, error) {
	if len(key) != KeySizeBytes {
		return nil, fmt.Errorf("aesgcm: key must be %d bytes, got %d", KeySizeBytes, len(key))
	}
	// Defensive copy: callers occasionally reuse slices and we don't want
	// a stray zero-write later to leak into the key buffer.
	buf := make([]byte, KeySizeBytes)
	copy(buf, key)
	return &Encryptor{key: buf, keyVersion: currentKeyVersion}, nil
}

// KeyVersion returns the rotation version stamp to persist alongside
// each ciphertext so a future rotation can detect old rows.
func (e *Encryptor) KeyVersion() int {
	if e == nil {
		return 0
	}
	return e.keyVersion
}

// Encrypt produces the canonical BLOB shape: nonce(12) || ciphertext || tag(16).
// Each call reads a fresh nonce from crypto/rand. A nil Encryptor returns
// an error so callers can decide whether to fail closed.
func (e *Encryptor) Encrypt(plaintext []byte) ([]byte, error) {
	if e == nil {
		return nil, errors.New("aesgcm.Encrypt: nil encryptor")
	}
	gcm, err := newGCM(e.key)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, NonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("aesgcm.Encrypt: read nonce: %w", err)
	}
	// Seal appends ciphertext+tag to its first arg; we pass nonce to keep
	// the BLOB self-describing for the decryptor.
	out := gcm.Seal(nonce, nonce, plaintext, nil)
	return out, nil
}

// Decrypt extracts the plaintext from a BLOB produced by Encrypt. Returns
// an error on tampered ciphertext, wrong-key binding, or shortened BLOB.
// Authenticated against the GCM tag so any modification fails loudly.
func (e *Encryptor) Decrypt(blob []byte) ([]byte, error) {
	if e == nil {
		return nil, errors.New("aesgcm.Decrypt: nil encryptor")
	}
	if len(blob) < NonceSize+16 {
		return nil, fmt.Errorf("aesgcm.Decrypt: blob too short (%d bytes)", len(blob))
	}
	gcm, err := newGCM(e.key)
	if err != nil {
		return nil, err
	}
	nonce := blob[:NonceSize]
	ciphertext := blob[NonceSize:]
	plain, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("aesgcm.Decrypt: open: %w", err)
	}
	return plain, nil
}

func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("aesgcm: NewCipher: %w", err)
	}
	return cipher.NewGCM(block)
}

// LoadFromEnv resolves the key from process env. If EnvVarKey is set it
// is decoded as base64. Else if EnvVarKeyFile is set, the file contents
// are read and decoded. If neither resolves a usable 32-byte key:
//   - requireIfMissing=true → returns ErrNoKey (server refuses OAuth ops)
//   - requireIfMissing=false → returns (nil, nil) so a degraded boot can
//     proceed with legacy JSON-only persistence (the prior behaviour).
//
// The raw key bytes are never returned; only the constructed Encryptor.
func LoadFromEnv(requireIfMissing bool) (*Encryptor, error) {
	raw, src, err := readKeySource()
	if err != nil {
		return nil, err
	}
	if raw == "" {
		if requireIfMissing {
			return nil, ErrNoKey
		}
		return nil, nil
	}
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("aesgcm: %s: base64 decode: %w", src, err)
	}
	if len(decoded) != KeySizeBytes {
		return nil, fmt.Errorf("aesgcm: %s: decoded key is %d bytes, want %d", src, len(decoded), KeySizeBytes)
	}
	enc, err := NewEncryptor(decoded)
	if err != nil {
		return nil, err
	}
	// Operators sometimes leave trailing whitespace or CRLF in the env
	// file (esp. Windows line endings). Trim is fine because Trim was
	// already applied to `raw` and base64 ignores whitespace anyway.
	_ = src // referenced for error messages only
	return enc, nil
}

func readKeySource() (string, string, error) {
	if v := strings.TrimSpace(os.Getenv(EnvVarKey)); v != "" {
		return v, EnvVarKey, nil
	}
	if path := strings.TrimSpace(os.Getenv(EnvVarKeyFile)); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				// Treat as missing rather than fatal so the loader's
				// requireIfMissing flag governs the response.
				return "", EnvVarKeyFile, nil
			}
			return "", EnvVarKeyFile, fmt.Errorf("aesgcm: read %s: %w", EnvVarKeyFile, err)
		}
		s := strings.TrimSpace(string(data))
		if s == "" {
			return "", EnvVarKeyFile, nil
		}
		return s, EnvVarKeyFile, nil
	}
	return "", "", nil
}
