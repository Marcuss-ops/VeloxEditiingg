package aesgcm

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"strings"
	"testing"
)

// TestEncryptorRoundTrip asserts that every Encrypt must be cancellable by
// the matching Decrypt, including the empty plaintext and a multi-KB blob.
// The empty plaintext case guards against a corner bug in earlier GCM
// implementations that panicked on len(plaintext) == 0.
func TestEncryptorRoundTrip(t *testing.T) {
	key := make([]byte, KeySizeBytes)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("rand: %v", err)
	}
	enc, err := NewEncryptor(key)
	if err != nil {
		t.Fatalf("NewEncryptor: %v", err)
	}
	if enc.KeyVersion() != 1 {
		t.Errorf("KeyVersion: got %d, want 1", enc.KeyVersion())
	}

	cases := []string{
		"",
		"x",
		"a longer string with characters like /+ and ünicodé",
		strings.Repeat("Y", 4096),
	}
	for _, p := range cases {
		blob, err := enc.Encrypt([]byte(p))
		if err != nil {
			t.Errorf("Encrypt(%q): %v", p, err)
			continue
		}
		// BLOB shape: nonce(NonceSize bytes) || ciphertext || gcm_tag(16 bytes).
		// Empty plaintext still produces a NIL-output but tag-only blob.
		if len(blob) < NonceSize+16 {
			t.Errorf("blob too short for plaintext len=%d: got %d bytes", len(p), len(blob))
		}
		out, err := enc.Decrypt(blob)
		if err != nil {
			t.Errorf("Decrypt(%q): %v", p, err)
			continue
		}
		if string(out) != p {
			t.Errorf("roundtrip mismatch: got %q, want %q", out, p)
		}
	}
}

// TestEncryptorNoncesAreUnique protects against a regression where someone
// accidentally holds the nonce constant across calls — reuse would leak
// plaintext under GCM. Repeated encrypts of the same plaintext must produce
// distinct BLOBs.
func TestEncryptorNoncesAreUnique(t *testing.T) {
	key := make([]byte, KeySizeBytes)
	_, _ = rand.Read(key)
	enc, _ := NewEncryptor(key)
	for i := 0; i < 100; i++ {
		a, errA := enc.Encrypt([]byte("same-plaintext"))
		b, errB := enc.Encrypt([]byte("same-plaintext"))
		if errA != nil || errB != nil {
			t.Fatalf("encrypt errors: %v / %v", errA, errB)
		}
		if bytes.Equal(a, b) {
			t.Fatalf("two encrypts of identical plaintext produced identical BLOBs (nonce reuse or deterministic cipher)")
		}
	}
}

// TestEncryptorTamperDetection flips a byte in the ciphertext region past
// the nonce header and asserts Decrypt fails. Without this guarantee the
// at-rest storage loses its only authentication signal.
func TestEncryptorTamperDetection(t *testing.T) {
	key := make([]byte, KeySizeBytes)
	_, _ = rand.Read(key)
	enc, _ := NewEncryptor(key)
	blob, _ := enc.Encrypt([]byte("secret-thing-a"))
	// Flip a byte well inside the ciphertext region.
	blob[NonceSize+1] ^= 0xFF
	if _, err := enc.Decrypt(blob); err == nil {
		t.Fatal("Decrypt accepted tampered blob")
	}
}

// TestEncryptorWrongKeyDetection asserts ciphertexts do not decrypt under
// a different 32-byte key — catches mis-keyed-at-rest data corruption.
// Two NewEncryptor instances must be wired to two DISTINCT keys (zero-filled
// 32-byte slices are byte-identical, so the test would silently pass).
func TestEncryptorWrongKeyDetection(t *testing.T) {
	keyA := make([]byte, KeySizeBytes)
	keyB := make([]byte, KeySizeBytes)
	if _, err := rand.Read(keyA); err != nil {
		t.Fatal(err)
	}
	if _, err := rand.Read(keyB); err != nil {
		t.Fatal(err)
	}
	a, err := NewEncryptor(keyA)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewEncryptor(keyB)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(keyA, keyB) {
		t.Fatal("test setup invariant violated: two random keys collided (try again)")
	}
	blob, err := a.Encrypt([]byte("secret"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Decrypt(blob); err == nil {
		t.Fatal("Decrypt with a different key accepted ciphertext")
	}
}

// TestNewEncryptorKeyLength: only 32-byte keys are accepted. AES-128 is no
// longer acceptable for OAuth credentials at-rest, and longer keys are
// silently truncated by some cipher implementations.
func TestNewEncryptorKeyLength(t *testing.T) {
	if _, err := NewEncryptor(make([]byte, 16)); err == nil {
		t.Error("expected error for 16-byte key (AES-128 unsupported)")
	}
	if _, err := NewEncryptor(make([]byte, 24)); err == nil {
		t.Error("expected error for 24-byte key")
	}
	if _, err := NewEncryptor(make([]byte, 64)); err == nil {
		t.Error("expected error for 64-byte key")
	}
}

// TestEncryptorNilSafe guards the nil-receiver shortcuts so callers in
// graceful-degraded mode don't crash with nil-pointer panics.
func TestEncryptorNilSafe(t *testing.T) {
	var enc *Encryptor
	if _, err := enc.Encrypt([]byte("x")); err == nil {
		t.Error("nil Encryptor must return error")
	}
	if _, err := enc.Decrypt([]byte("anything")); err == nil {
		t.Error("nil Encryptor must return error")
	}
	if enc.KeyVersion() != 0 {
		t.Errorf("nil Encryptor.KeyVersion: got %d, want 0", enc.KeyVersion())
	}
}

// TestLoadFromEnv covers all four env resolution paths the production
// loader can encounter. t.Setenv auto-restores on test cleanup.
func TestLoadFromEnv(t *testing.T) {
	key := make([]byte, KeySizeBytes)
	_, _ = rand.Read(key)
	encoded := base64.StdEncoding.EncodeToString(key)

	t.Run("primary env var", func(t *testing.T) {
		t.Setenv(EnvVarKey, encoded)
		t.Setenv(EnvVarKeyFile, "")
		enc, err := LoadFromEnv(true)
		if err != nil {
			t.Fatalf("LoadFromEnv: %v", err)
		}
		if enc == nil {
			t.Fatal("expected non-nil Encryptor with env var set")
		}
	})

	t.Run("missing key returns nil when not requireIfMissing", func(t *testing.T) {
		t.Setenv(EnvVarKey, "")
		t.Setenv(EnvVarKeyFile, "")
		enc, err := LoadFromEnv(false)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if enc != nil {
			t.Fatal("expected nil Encryptor when env vars are unset and requireIfMissing=false")
		}
	})

	t.Run("missing key returns ErrNoKey when requireIfMissing", func(t *testing.T) {
		t.Setenv(EnvVarKey, "")
		t.Setenv(EnvVarKeyFile, "")
		_, err := LoadFromEnv(true)
		if err != ErrNoKey {
			t.Fatalf("expected ErrNoKey, got %v", err)
		}
	})

	t.Run("bad base64 returns error", func(t *testing.T) {
		t.Setenv(EnvVarKey, "not-base64-string")
		t.Setenv(EnvVarKeyFile, "")
		_, err := LoadFromEnv(true)
		if err == nil || err == ErrNoKey {
			t.Fatalf("expected parse error, got %v", err)
		}
	})

	t.Run("wrong length returns error", func(t *testing.T) {
		// 16 bytes base64-encoded => key decodes to 16 bytes, not 32.
		short := base64.StdEncoding.EncodeToString(make([]byte, 16))
		t.Setenv(EnvVarKey, short)
		t.Setenv(EnvVarKeyFile, "")
		_, err := LoadFromEnv(true)
		if err == nil {
			t.Fatal("expected error for short base64-decoded key")
		}
	})
}
