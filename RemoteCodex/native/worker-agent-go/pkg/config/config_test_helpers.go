package config

// Shared test fixtures for the config_test_*.go files.
//
// Lives as a non-test (*_test.go-less) file per the project precedent
// set by DataServer/internal/store/sqlite_task_atomic_test_helpers.go:
// helpers that every test in the package needs are exported across
// file boundaries under unexported names so they don't leak into the
// production API surface. Each helper pins exactly one thing (PEM
// fixture shape, baseline-config shape, mode-bit trick for key-files,
// etc.) so the tests can compose them without inline fixtures.

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// devValidBase returns a WorkerConfig that satisfies Validate() via the
// dev-only insecure path (environment=dev + AllowInsecureGRPC=true).
// Tests that exercise other Validate-invariants (missing fields, log
// level, etc.) start from this and nil-out / break one thing at a time.
func devValidBase() *WorkerConfig {
	return &WorkerConfig{
		MasterURL:         "http://localhost:8080",
		WorkerID:          "test-worker-001",
		WorkerName:        "Test Worker",
		WorkDir:           "/opt/velox",
		LogLevel:          "info",
		ControlGRPCURL:    "localhost:8443",
		Environment:       "dev",
		AllowInsecureGRPC: true,
	}
}

// generateCompatibleTLSPair is the inverse of generateKeyCertMismatchPair:
// it produces a (cert.pem, key.pem, ca.pem) triplet where the cert and the
// key on disk ACTUALLY pair, so the TLS handshake and
// cryptotls.LoadX509KeyPair both succeed.
//
// Used by fullTLSBase() to provide realistic test fixtures that the new
// LoadX509KeyPair guard inside Validate() can pass cleanly.
func generateCompatibleTLSPair(t *testing.T) (certFile, keyFile, caFile string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("genkey: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test-cert"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(30 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}

	// Self-signed leaf cert using `key`.
	leafBytes, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("createleaf: %v", err)
	}
	leafPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafBytes})

	// CA is a separate self-signed cert (still legitimate PEM material so
	// os.Stat succeeds at validate time; the cert/key pairing is what
	// LoadX509KeyPair actually checks).
	caBytes, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("createca: %v", err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caBytes})

	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})

	dir := t.TempDir()
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	caFile = filepath.Join(dir, "ca.pem")
	mustWrite(t, certFile, leafPEM)
	mustWrite(t, keyFile, keyPEM)
	mustWrite(t, caFile, caPEM)
	return
}

// fullTLSBase returns a WorkerConfig that satisfies Validate() via the
// full mTLS triple. The on-disk PEMs are real (generated in-memory by
// generateCompatibleTLSPair) so Validate's LoadX509KeyPair check passes.
func fullTLSBase(t *testing.T) *WorkerConfig {
	t.Helper()
	certFile, keyFile, caFile := generateCompatibleTLSPair(t)
	return &WorkerConfig{
		MasterURL:      "http://localhost:8080",
		WorkerID:       "tls-worker-001",
		WorkerName:     "TLS Worker",
		WorkDir:        "/opt/velox",
		LogLevel:       "info",
		ControlGRPCURL: "localhost:8443",
		Environment:    "production",
		TLSCertFile:    certFile,
		TLSKeyFile:     keyFile,
		TLSCAFile:      caFile,
	}
}

func writeTempDummy(t *testing.T, name string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("dummy"), 0600); err != nil {
		t.Fatalf("writeTempDummy(%s): %v", name, err)
	}
	return path
}

// generateTLSPairWithNotAfter is the canonical test fixture factory for
// RW-PROD-001 tests. Unlike generateCompatibleTLSPair (which hardcodes a
// 24h NotAfter), this helper accepts an arbitrary NotAfter so the
// expiry-window tests can sweep past the 14-day production floor
// without re-generating the certificate each time.
func generateTLSPairWithNotAfter(t *testing.T, notAfter time.Time) (certFile, keyFile, caFile string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("genkey: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "rw-prod-001-fixture"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}

	leafBytes, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("createleaf: %v", err)
	}
	leafPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafBytes})
	caBytes, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("createca: %v", err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caBytes})
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})

	dir := t.TempDir()
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	caFile = filepath.Join(dir, "ca.pem")
	mustWrite(t, certFile, leafPEM)
	mustWrite(t, keyFile, keyPEM)
	mustWrite(t, caFile, caPEM)
	return
}

// writeKeyMode writes a key-shaped file at path with the requested POSIX mode.
// Used by TS-1.4 (mode 0644 rejected in production) and TS-1.5 (mode 0644
// recorded as a non-fatal Warning in dev). The companion pair-cert at the
// returned certFile is generated by generateCompatibleTLSPair so LoadX509KeyPair
// still pairs successfully — isolating the permission check from the
// cert/key pairing guard.
func writeKeyMode(t *testing.T, mode os.FileMode) (certFile, keyFile, caFile string) {
	t.Helper()
	// Build a compatible (cert, ca, key) triple first…
	cert, key, ca := generateCompatibleTLSPair(t)
	// …then overwrite the key file with one whose mode is the requested value.
	data, err := os.ReadFile(key)
	if err != nil {
		t.Fatalf("read key: %v", err)
	}
	if err := os.Remove(key); err != nil {
		t.Fatalf("remove key: %v", err)
	}
	if err := os.WriteFile(key, data, mode); err != nil {
		t.Fatalf("rewrite key: %v", err)
	}
	return cert, key, ca
}

// generateKeyCertMismatchPair creates an in-memory PEM triple where the
// certificate was created with one RSA key and the key.pem on disk is
// a DIFFERENT RSA key. tls.LoadX509KeyPair MUST reject this pair.
//
// We don't shell out to openssl because the test must stay portable
// (and pure Go cert/key generation is fast enough).
func generateKeyCertMismatchPair(t *testing.T) (certFile, keyFile, caFile string) {
	t.Helper()

	key1, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("genkey1: %v", err)
	}
	key2, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("genkey2: %v", err)
	}

	serial := big.NewInt(1)
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "test-cert-mismatch"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(30 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}

	// Cert is signed by key1, but key.pem on disk is key2.
	derBytes, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key1.PublicKey, key1)
	if err != nil {
		t.Fatalf("createcert: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})

	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key2),
	})

	// CA file: a self-signed cert (acceptable as a CA pointer for the
	// purposes of failing on key-pair mismatch alone).
	caBytes, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key1.PublicKey, key1)
	if err != nil {
		t.Fatalf("createca: %v", err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caBytes})

	dir := t.TempDir()
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	caFile = filepath.Join(dir, "ca.pem")
	mustWrite(t, certFile, certPEM)
	mustWrite(t, keyFile, keyPEM)
	mustWrite(t, caFile, caPEM)
	return
}

func mustWrite(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
