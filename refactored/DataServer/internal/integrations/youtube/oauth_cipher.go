package youtube

// OAuthCipher is the canonical interface for encrypting and decrypting
// OAuth token bytes before they touch the SQLite oauth_tokens table.
//
// The runtime mounts an AES-GCM backed implementation resolved from
// VELOX_YT_OAUTH_TOKEN_KEY (or _FILE variant) onto Service.oauthBuf
// during NewService. The interface keeps the service surface decoupled
// from any specific cipher so future rotation (e.g. ChaCha20-Poly1305)
// only swaps the asset and the public method signatures stay put.
//
// Defining it here (rather than only inside an internal/aesgcm package)
// is intentional: the storage layer's `youtube_oauth_tokens` columns
// surface `[]byte` blobs and the service is the SINGLE place that knows
// whether a blob is plaintext or ciphertext. Tests that exercise the
// boot hydrator (loadOAuthChannelsFromSQLite) can therefore satisfy
// OAuthCipher with a no-op in-memory cipher without importing any
// production crypto asset.
//
// Fail-closed semantics: `Service.NewService` returns an error when
// oauthBuf is nil so a server that boots without a mounted cipher
// cannot start emitting OAuth traffic. The runtime checks before each
// hydration call log + return early rather than panic so a transient
// cipher missing reflects as a `[ERR]` log line instead of a hard crash.
type OAuthCipher interface {
	// Encrypt takes plaintext bytes and returns the ciphertext that
	// is then written into youtube_oauth_tokens.access_token_encrypted
	// / refresh_token_encrypted. Implementations MUST be safe for
	// concurrent callers (the boot hydrator and refresh path can both
	// fire on the same Service from different goroutines).
	Encrypt(plaintext []byte) ([]byte, error)

	// Decrypt is the inverse of Encrypt and is called by the hydration
	// path before the OAuth client rehydrates an AuthChannel's
	// AccessToken / RefreshToken strings. Errors here surface as
	// `(0, err)` from loadOAuthChannelsFromSQLite so callers see the
	// per-row failure explicitly.
	Decrypt(ciphertext []byte) ([]byte, error)

	// KeyVersion returns the cipher key rotation stamp persisted on
	// every youtube_oauth_tokens row. Future rotation can detect
	// rows that still need re-encryption. Implementations typically
	// read VELOX_YT_OAUTH_TOKEN_KEY_VERSION (or fall back to 1).
	KeyVersion() int
}

// NoopOAuthCipher is the canonical zero-asset cipher used in unit tests
// and as the "we accept the risk" fallback for development builds where
// VELOX_YT_OAUTH_TOKEN_KEY is intentionally absent. It is intentionally
// NOT a production cipher: any operator who wires a NoopOAuthCipher
// into a live Service is persisting OAuth tokens as plaintext, which
// contradicts the storage-layer comment that the blobs are always
// ciphertext. The type lives here so test code can `&NoopOAuthCipher{}`
// without re-declaring a duplicate in every test file.
type NoopOAuthCipher struct{}

// Encrypt is a passthrough: bytes in, bytes out, no error. The empty
// error return keeps the interface clean for callers that don't
// distinguish "no cipher" from "plaintext token".
func (NoopOAuthCipher) Encrypt(plaintext []byte) ([]byte, error) {
	return plaintext, nil
}

// Decrypt is a passthrough; see Encrypt for caveats.
func (NoopOAuthCipher) Decrypt(ciphertext []byte) ([]byte, error) {
	return ciphertext, nil
}

// KeyVersion reports a fixed key version for the noop cipher so tests
// can assert that the cipher-version stamp stays stable across calls.
func (NoopOAuthCipher) KeyVersion() int {
	return 1
}
