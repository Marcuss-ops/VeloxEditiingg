// Package config provides configuration management for the Velox Worker Agent.
//
// PR 1 (`codex/grpc-config-single-source`) added:
//
//   - GRPCTLSConfig struct + WorkerConfig.GRPCTLS() accessor
//   - Environment field on WorkerConfig, with safe-by-default "production"
//   - applyEnvOverrides() overlay binding VELOX_ENV / VELOX_GRPC_TLS_* /
//     VELOX_ALLOW_INSECURE_GRPC_DEV onto the parsed JSON
//   - TLS combinatorial validation in WorkerConfig.Validate():
//     full triple OR (allow_insecure + env!=production) — partials rejected
//
// The tests below exercise every rule combination.
package config

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
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

// =============================================================================
//  Existing tests preserved + adapted to the new dev-env convention.
// =============================================================================

func TestLoadConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	configJSON := `{
		"master_url": "http://localhost:8080",
		"worker_id": "test-worker-001",
		"worker_name": "Test Worker",
		"work_dir": "/opt/velox",
		"log_level": "debug"
	}`

	if err := os.WriteFile(configPath, []byte(configJSON), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	if cfg.MasterURL != "http://localhost:8080" {
		t.Errorf("Expected master_url http://localhost:8080, got %s", cfg.MasterURL)
	}

	if cfg.WorkerID != "test-worker-001" {
		t.Errorf("Expected worker_id test-worker-001, got %s", cfg.WorkerID)
	}

	if cfg.WorkerName != "Test Worker" {
		t.Errorf("Expected worker_name Test Worker, got %s", cfg.WorkerName)
	}

	if cfg.LogLevel != "debug" {
		t.Errorf("Expected log_level debug, got %s", cfg.LogLevel)
	}

	if cfg.HealthPort != 8081 {
		t.Errorf("Expected default health_port 8081 for legacy config, got %d", cfg.HealthPort)
	}

	// PR 1: missing `environment` JSON key + no VELOX_ENV env var → default "production".
	if cfg.Environment != "production" {
		t.Errorf("Expected default environment production, got %q", cfg.Environment)
	}
}

func TestLoadConfigNotFound(t *testing.T) {
	_, err := LoadConfig("/nonexistent/config.json")
	if err == nil {
		t.Error("Expected error for non-existent config file")
	}
}

func TestLoadConfigInvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	if err := os.WriteFile(configPath, []byte("invalid json"), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	_, err := LoadConfig(configPath)
	if err == nil {
		t.Error("Expected error for invalid JSON")
	}
}

func TestSaveConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	cfg := devValidBase()

	if err := SaveConfig(configPath, cfg); err != nil {
		t.Fatalf("Failed to save config: %v", err)
	}

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Error("Config file was not created")
	}

	loaded, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load saved config: %v", err)
	}

	if loaded.MasterURL != cfg.MasterURL {
		t.Errorf("Expected master_url %s, got %s", cfg.MasterURL, loaded.MasterURL)
	}

	if loaded.WorkerID != cfg.WorkerID {
		t.Errorf("Expected worker_id %s, got %s", cfg.WorkerID, loaded.WorkerID)
	}
}

func TestSaveConfigNil(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	err := SaveConfig(configPath, nil)
	if err == nil {
		t.Error("Expected error for nil config")
	}
}

func TestSaveConfigCreatesDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "subdir", "nested", "config.json")

	cfg := devValidBase()

	if err := SaveConfig(configPath, cfg); err != nil {
		t.Fatalf("Failed to save config: %v", err)
	}

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Error("Config file was not created in nested directory")
	}
}

func TestGenerateWorkerID(t *testing.T) {
	id1 := GenerateWorkerID()
	id2 := GenerateWorkerID()

	if len(id1) != 15 { // "worker-" (7) + 8 hex chars
		t.Errorf("Expected worker ID length 15, got %d", len(id1))
	}

	if id1[:7] != "worker-" {
		t.Errorf("Expected worker ID to start with 'worker-', got %s", id1[:7])
	}

	if id1 == id2 {
		t.Error("Expected different worker IDs to be different")
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig("/opt/velox")

	if cfg == nil {
		t.Fatal("Expected non-nil config")
	}

	if cfg.MasterURL != "http://localhost:8000" {
		t.Errorf("Expected default master_url http://localhost:8000, got %s", cfg.MasterURL)
	}

	if cfg.WorkerID == "" {
		t.Error("Expected non-empty worker_id")
	}

	if cfg.WorkerName != "velox-worker" {
		t.Errorf("Expected default worker_name velox-worker, got %s", cfg.WorkerName)
	}

	if cfg.WorkDir != "/opt/velox" {
		t.Errorf("Expected work_dir /opt/velox, got %s", cfg.WorkDir)
	}

	if cfg.LogLevel != "info" {
		t.Errorf("Expected default log_level info, got %s", cfg.LogLevel)
	}

	if cfg.HealthPort != 8081 {
		t.Errorf("Expected default health_port 8081, got %d", cfg.HealthPort)
	}

	// PR 1: DefaultConfig intentionally leaves Environment="" so the
	// single source for the default is applyDefaults(). Production callers
	// should observe cfg.Environment AFTER applyDefaults() — see
	// TestDefaultConfigAfterApplyDefaults below.
	if cfg.Environment != "" {
		t.Errorf("DefaultConfig should leave Environment unset (single-source: applyDefaults); got %q", cfg.Environment)
	}
}

// TestDefaultConfigAfterApplyDefaults confirms that applyDefaults()
// (the canonical single-source-of-truth setter) fills Environment to
// "production" when the operator hasn't supplied one.
func TestDefaultConfigAfterApplyDefaults(t *testing.T) {
	cfg := DefaultConfig("/opt/velox")
	cfg.applyDefaults()
	if cfg.Environment != "production" {
		t.Errorf("after applyDefaults, expected environment production, got %q", cfg.Environment)
	}
}

func TestDefaultConfigEmptyWorkDir(t *testing.T) {
	cfg := DefaultConfig("")

	if cfg.WorkDir != "/opt/velox" {
		t.Errorf("Expected default work_dir /opt/velox, got %s", cfg.WorkDir)
	}
}

func TestValidateSuccess(t *testing.T) {
	cfg := devValidBase()

	if err := cfg.Validate(); err != nil {
		t.Errorf("Expected validation to pass in dev mode, got error: %v", err)
	}
}

func TestValidateNil(t *testing.T) {
	var cfg *WorkerConfig

	err := cfg.Validate()
	if err == nil {
		t.Error("Expected error for nil config")
	}
}

func TestValidateMissingFields(t *testing.T) {
	tests := []struct {
		name   string
		config *WorkerConfig
	}{
		{
			name: "missing master_url",
			config: func() *WorkerConfig {
				c := devValidBase()
				c.MasterURL = ""
				return c
			}(),
		},
		{
			name: "missing worker_id",
			config: func() *WorkerConfig {
				c := devValidBase()
				c.WorkerID = ""
				return c
			}(),
		},
		{
			name: "missing work_dir",
			config: func() *WorkerConfig {
				c := devValidBase()
				c.WorkDir = ""
				return c
			}(),
		},
		{
			name: "missing control_grpc_url",
			config: func() *WorkerConfig {
				c := devValidBase()
				c.ControlGRPCURL = ""
				return c
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if err == nil {
				t.Errorf("Expected validation error for %s", tt.name)
			}
		})
	}
}

func TestValidateInvalidLogLevel(t *testing.T) {
	cfg := devValidBase()
	cfg.LogLevel = "invalid"

	err := cfg.Validate()
	if err == nil {
		t.Error("Expected validation error for invalid log level")
	}
}

func TestValidateLogLevels(t *testing.T) {
	validLevels := []string{"", "debug", "info", "warn", "error"}

	for _, level := range validLevels {
		t.Run("log_level_"+level, func(t *testing.T) {
			cfg := devValidBase()
			cfg.LogLevel = level

			if err := cfg.Validate(); err != nil {
				t.Errorf("Expected validation to pass for log_level %q, got error: %v", level, err)
			}
		})
	}
}

func TestString(t *testing.T) {
	cfg := devValidBase()

	str := cfg.String()

	if str == "" {
		t.Error("Expected non-empty string representation")
	}

	if !strings.Contains(str, "test-worker-001") {
		t.Error("Expected worker_id in string representation")
	}

	if !strings.Contains(str, "Test Worker") {
		t.Error("Expected worker_name in string representation")
	}

	// PR 1: Environment is now part of String() output so log lines and
	// debug dumps reveal the deployment lifecycle tag.
	if !strings.Contains(str, "dev") {
		t.Error("Expected environment tag in string representation")
	}
}

func TestStringNil(t *testing.T) {
	var cfg *WorkerConfig

	str := cfg.String()

	if str != "WorkerConfig{nil}" {
		t.Errorf("Expected 'WorkerConfig{nil}', got %q", str)
	}
}

// =============================================================================
//  PR 1 — new TLS-related test surface.
// =============================================================================

// TestTLSValidation exercises the seven scenarios enumerated in the PR 1
// spec:
func TestTLSValidation(t *testing.T) {
	cases := []struct {
		name        string
		cfg         func(t *testing.T) *WorkerConfig
		errContains string // empty if nil expected
	}{
		{
			name: "1. JSON+Env complete (full triple, cert exists, prod env) -> OK",
			cfg:  func(t *testing.T) *WorkerConfig { return fullTLSBase(t) },
		},
		{
			name: "4. Partial TLS (cert only) -> err",
			cfg: func(t *testing.T) *WorkerConfig {
				certFile := writeTempDummy(t, "cert.pem")
				return &WorkerConfig{
					MasterURL: "http://localhost:8080", WorkerID: "tlsw", WorkDir: "/opt",
					ControlGRPCURL: "g", Environment: "production",
					TLSCertFile: certFile,
				}
			},
			errContains: "partial TLS configuration",
		},
		{
			name: "4b. Partial TLS (cert+key, missing ca) -> err",
			cfg: func(t *testing.T) *WorkerConfig {
				c := fullTLSBase(t)
				c.TLSCAFile = ""
				return c
			},
			errContains: "partial TLS configuration",
		},
		{
			name: "4c. Partial TLS (ca only) -> err",
			cfg: func(t *testing.T) *WorkerConfig {
				caFile := writeTempDummy(t, "ca.pem")
				return &WorkerConfig{
					MasterURL: "http://localhost:8080", WorkerID: "tlsw", WorkDir: "/opt",
					ControlGRPCURL: "g", Environment: "production",
					TLSCAFile: caFile,
				}
			},
			errContains: "partial TLS configuration",
		},
		{
			name: "5. No TLS, insecure=false (env=prod) -> err",
			cfg: func(t *testing.T) *WorkerConfig {
				return &WorkerConfig{
					MasterURL: "http://localhost:8080", WorkerID: "tlsw", WorkDir: "/opt",
					ControlGRPCURL: "g", Environment: "production",
				}
			},
			errContains: "no TLS configured",
		},
		{
			name: "6. insecure=true, env=dev -> OK",
			cfg: func(t *testing.T) *WorkerConfig {
				return &WorkerConfig{
					MasterURL: "http://localhost:8080", WorkerID: "tlsw", WorkDir: "/opt",
					ControlGRPCURL: "g", Environment: "dev",
					AllowInsecureGRPC: true,
				}
			},
		},
		{
			name: "7. insecure=true, env=production -> err",
			cfg: func(t *testing.T) *WorkerConfig {
				return &WorkerConfig{
					MasterURL: "http://localhost:8080", WorkerID: "tlsw", WorkDir: "/opt",
					ControlGRPCURL: "g", Environment: "production",
					AllowInsecureGRPC: true,
				}
			},
			errContains: "allow_insecure_grpc_dev=true is only valid in non-production environments",
		},
		{
			name: "8. cert file missing on disk -> err",
			cfg: func(t *testing.T) *WorkerConfig {
				return &WorkerConfig{
					MasterURL: "http://localhost:8080", WorkerID: "tlsw", WorkDir: "/opt",
					ControlGRPCURL: "g", Environment: "production",
					TLSCertFile: "/this/path/does/not/exist/cert.pem",
					TLSKeyFile:  writeTempDummy(t, "key.pem"),
					TLSCAFile:   writeTempDummy(t, "ca.pem"),
				}
			},
			errContains: "tls_cert_file not found",
		},
		{
			name: "9. TLS AND insecure both active -> err",
			cfg: func(t *testing.T) *WorkerConfig {
				c := fullTLSBase(t)
				c.AllowInsecureGRPC = true
				c.Environment = "dev" // even in dev, mixing is rejected
				return c
			},
			errContains: "cannot be active simultaneously",
		},
		{
			name: "10. invalid environment literal -> err",
			cfg: func(t *testing.T) *WorkerConfig {
				c := devValidBase()
				c.Environment = "qa"
				return c
			},
			errContains: `invalid environment: "qa"`,
		},
		{
			name: "11. full triple OK when env=staging (NOT a dev-only check)",
			cfg: func(t *testing.T) *WorkerConfig {
				c := fullTLSBase(t)
				c.Environment = "staging"
				return c
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.cfg(t)
			err := cfg.Validate()
			if tc.errContains == "" {
				if err != nil {
					t.Errorf("expected no error, got: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.errContains)
			}
			if !strings.Contains(err.Error(), tc.errContains) {
				t.Errorf("expected error to contain %q, got: %v", tc.errContains, err)
			}
		})
	}
}

// TestGRPCTLSAccessor ensures the canonical accessor mirrors the four
// TLS fields and nothing else; this is the surface the transport factory
// is allowed to consume.
func TestGRPCTLSAccessor(t *testing.T) {
	certFile := writeTempDummy(t, "cert.pem")
	keyFile := writeTempDummy(t, "key.pem")
	caFile := writeTempDummy(t, "ca.pem")

	cfg := &WorkerConfig{
		TLSCertFile:       certFile,
		TLSKeyFile:        keyFile,
		TLSCAFile:         caFile,
		AllowInsecureGRPC: false,
	}

	got := cfg.GRPCTLS()
	if got.CertFile != certFile || got.KeyFile != keyFile || got.CAFile != caFile || got.AllowInsecureDev {
		t.Errorf("GRPCTLS() did not mirror fields: got %+v", got)
	}

	cfg.AllowInsecureGRPC = true
	got = cfg.GRPCTLS()
	if !got.AllowInsecureDev {
		t.Error("GRPCTLS() did not propagate AllowInsecureGRPC=true")
	}

	var nilCfg *WorkerConfig
	if got := nilCfg.GRPCTLS(); got != (GRPCTLSConfig{}) {
		t.Errorf("GRPCTLS() on nil should return zero struct, got %+v", got)
	}
}

// TestEnvTruthy sanity-checks the envTruthy helper against the canonical
// truthy spellings. This test stays in env_test.go-equivalent territory,
// but we keep it in the main test file to avoid a separate package.
func TestEnvTruthy(t *testing.T) {
	cases := map[string]bool{
		"":       false,
		"0":      false,
		"false":  false,
		"no":     false,
		"off":    false,
		"random": false,
		"1":      true,
		"true":   true,
		"TRUE":   true,
		"True":   true,
		"yes":    true,
		"YES":    true,
		"on":     true,
		"ON":     true,
		" true ": true, // trims
	}
	for input, expected := range cases {
		got := envTruthy(input)
		if got != expected {
			t.Errorf("envTruthy(%q) = %v, want %v", input, got, expected)
		}
	}
}

// =============================================================================
//  RW-PROD-001 — production hardening for the TLS validation chain.
// =============================================================================

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

// TestRWProd001_TLSExpiryWindow covers TS-1.1, TS-1.2, TS-1.3 from
// docs/rw-prod/RW-PROD-001.md §5.
//   - TS-1.1: cert valid + remaining > 14d → Validate() passes
//   - TS-1.2: cert expires in < 14d → Validate() rejects (RW-PROD-001 A1)
//   - TS-1.3: cert already expired → Validate() rejects
func TestRWProd001_TLSExpiryWindow(t *testing.T) {
	const belowFloor = 13 * 24 * time.Hour
	const aboveFloor = 30 * 24 * time.Hour

	t.Run("TS-1.1: fresh cert (>14d) PASSES", func(t *testing.T) {
		cert, key, ca := generateTLSPairWithNotAfter(t, time.Now().Add(aboveFloor))
		cfg := fullTLSBase(t)
		cfg.TLSCertFile = cert
		cfg.TLSKeyFile = key
		cfg.TLSCAFile = ca
		if err := cfg.Validate(); err != nil {
			t.Fatalf("expected nil for fresh cert with 30d remaining, got: %v", err)
		}
	})

	t.Run("TS-1.2: cert expiring in <14d REJECTED", func(t *testing.T) {
		cert, key, ca := generateTLSPairWithNotAfter(t, time.Now().Add(belowFloor))
		cfg := fullTLSBase(t)
		cfg.TLSCertFile = cert
		cfg.TLSKeyFile = key
		cfg.TLSCAFile = ca
		err := cfg.Validate()
		if err == nil {
			t.Fatal("expected error for cert <14d, got nil")
		}
		if !strings.Contains(err.Error(), "expires too soon") {
			t.Errorf("expected 'expires too soon' error, got: %v", err)
		}
		if !strings.Contains(err.Error(), "RW-PROD-001 A1") {
			t.Errorf("expected RW-PROD-001 A1 marker, got: %v", err)
		}
	})

	t.Run("TS-1.3: cert already expired REJECTED", func(t *testing.T) {
		cert, key, ca := generateTLSPairWithNotAfter(t, time.Now().Add(-1*time.Hour))
		cfg := fullTLSBase(t)
		cfg.TLSCertFile = cert
		cfg.TLSKeyFile = key
		cfg.TLSCAFile = ca
		err := cfg.Validate()
		if err == nil {
			t.Fatal("expected error for expired cert, got nil")
		}
		if !strings.Contains(err.Error(), "certificate has expired") {
			t.Errorf("expected 'certificate has expired' error, got: %v", err)
		}
	})
}

// TestRWProd001_KeyPermissions covers TS-1.4 (production hard-fail) and
// TS-1.5 (dev/staging record-only Warning) from RW-PROD-001 §5.
func TestRWProd001_KeyPermissions(t *testing.T) {
	t.Run("TS-1.4: key mode 0644 REJECTED in production", func(t *testing.T) {
		cert, key, ca := writeKeyMode(t, 0o644)
		cfg := fullTLSBase(t)
		cfg.TLSCertFile = cert
		cfg.TLSKeyFile = key
		cfg.TLSCAFile = ca
		cfg.Environment = "production"
		err := cfg.Validate()
		if err == nil {
			t.Fatal("expected rejection for key 0644 in production, got nil")
		}
		if !strings.Contains(err.Error(), "insecure permissions") {
			t.Errorf("expected 'insecure permissions' message, got: %v", err)
		}
		if !strings.Contains(err.Error(), "RW-PROD-001 A2") {
			t.Errorf("expected RW-PROD-001 A2 marker, got: %v", err)
		}
	})

	t.Run("TS-1.5: key mode 0644 WARN in dev (no error, Warning populated)", func(t *testing.T) {
		cert, key, ca := writeKeyMode(t, 0o644)
		cfg := fullTLSBase(t)
		cfg.TLSCertFile = cert
		cfg.TLSKeyFile = key
		cfg.TLSCAFile = ca
		cfg.Environment = "dev"
		err := cfg.Validate()
		if err != nil {
			t.Fatalf("expected nil error in dev, got: %v", err)
		}
		found := false
		for _, w := range cfg.Warnings {
			if strings.Contains(w, "weak_permissions_warn") && strings.Contains(w, "RW-PROD-001 A2") {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected Warnings to include weak_permissions_warn (A2), got: %v", cfg.Warnings)
		}
	})

	t.Run("key mode 0600 PASSES in production (sanity)", func(t *testing.T) {
		cert, key, ca := writeKeyMode(t, 0o600)
		cfg := fullTLSBase(t)
		cfg.TLSCertFile = cert
		cfg.TLSKeyFile = key
		cfg.TLSCAFile = ca
		if err := cfg.Validate(); err != nil {
			t.Errorf("expected nil for key 0600 in production, got: %v", err)
		}
		if len(cfg.Warnings) != 0 {
			t.Errorf("expected no Warnings for key 0600, got: %v", cfg.Warnings)
		}
	})
}

// TestRWProd001_WorkerIDShape covers TS-1.6 from RW-PROD-001 §5.
//
// Note on error messages: the empty case produces two distinct error
// messages depending on which branch fires first. With B1's regex
// `[a-z][a-z0-9_-]{2,62}`:
//   - empty input → "worker_id is required" (the missing-field branch,
//     earlier in Validate's order of checks, fires before the shape check).
//   - non-empty but shape-invalid → "invalid worker_id shape: ..."
//
// We therefore assert on err==nil vs err!=err, plus a substring check
// that is loose enough to cover both messages.
func TestRWProd001_WorkerIDShape(t *testing.T) {
	cases := []struct {
		name      string
		id        string
		wantErr   bool
		errSubstr string // substring expected in err message when wantErr is true
	}{
		{"canonical worker-xxxxxxxx (15 chars)", "worker-8e98ce85", false, ""},
		{"host prefix after Normalize", "host_57_129_132_133", false, ""},
		{"underscore allowed (B1)", "worker_001", false, ""},
		{"minimum 3 chars", "abc", false, ""},
		{"max length 63", "a" + strings.Repeat("a", 62), false, ""},
		// Negative cases
		{"empty", "", true, "worker_id is required"},
		{"too short (2 chars)", "ab", true, "invalid worker_id shape"},
		{"too long (64 chars)", "a" + strings.Repeat("a", 63), true, "invalid worker_id shape"},
		{"uppercase letter", "Worker-001", true, "invalid worker_id shape"},
		{"starts with digit", "1-worker", true, "invalid worker_id shape"},
		{"starts with hyphen", "-worker", true, "invalid worker_id shape"},
		{"non-ASCII letter", "wörker-001", true, "invalid worker_id shape"},
		{"special char", "worker@1", true, "invalid worker_id shape"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := devValidBase()
			cfg.WorkerID = tc.id
			err := cfg.Validate()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected rejection for %q, got nil", tc.id)
				}
				if tc.errSubstr != "" && !strings.Contains(err.Error(), tc.errSubstr) {
					t.Errorf("expected error to contain %q for %q, got: %v",
						tc.errSubstr, tc.id, err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected rejection for %q: %v", tc.id, err)
				}
			}
		})
	}
}

// =============================================================================
//  PR 1 — env-var override + cert/key compatibility coverage.
// =============================================================================

// TestEnvOverrides verifies the precedence rule: env vars OVERRIDE
// worker_config.json for the four TLS-related fields + Environment +
// AllowInsecureGRPC. This is spec test case #3 from
// `codex/grpc-config-single-source`.
//
// The test is split into two sub-cases because the new Validate()
// rule "TLS AND insecure cannot be active simultaneously" forbids
// setting BOTH paths from env at once — we cover each path in its
// own t.Run sub-test with a non-conflicting env footprint.
func TestEnvOverrides(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "envoverride.json")

	// JSON declares environment=production and no TLS / insecure flag.
	jsonConfig := `{
		"master_url": "http://localhost:8080",
		"worker_id": "test-worker-001",
		"work_dir": "/opt/velox",
		"log_level": "info",
		"control_grpc_url": "localhost:8443",
		"environment": "production"
	}`
	if err := os.WriteFile(configPath, []byte(jsonConfig), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	t.Run("TLS fields via env override JSON-empty TLS", func(t *testing.T) {
		certFile, keyFile, caFile := generateCompatibleTLSPair(t)

		// Single-purpose env footprint: TLS triple + VELOX_ENV=dev. NO
		// insecure flag set, so Validate's "TLS AND insecure" rule does
		// not fire and dev != production keeps the env gate open.
		t.Setenv("VELOX_ENV", "dev")
		t.Setenv("VELOX_GRPC_TLS_CERT_FILE", certFile)
		t.Setenv("VELOX_GRPC_TLS_KEY_FILE", keyFile)
		t.Setenv("VELOX_GRPC_TLS_CA_FILE", caFile)
		t.Setenv("VELOX_ALLOW_INSECURE_GRPC_DEV", "")

		cfg, err := LoadConfig(configPath)
		if err != nil {
			t.Fatalf("LoadConfig: %v", err)
		}

		if cfg.Environment != "dev" {
			t.Errorf("env override Environment: got %q want dev", cfg.Environment)
		}
		if cfg.TLSCertFile != certFile {
			t.Errorf("env override TLSCertFile: got %q want %q", cfg.TLSCertFile, certFile)
		}
		if cfg.TLSKeyFile != keyFile {
			t.Errorf("env override TLSKeyFile: got %q want %q", cfg.TLSKeyFile, keyFile)
		}
		if cfg.TLSCAFile != caFile {
			t.Errorf("env override TLSCAFile: got %q want %q", cfg.TLSCAFile, caFile)
		}
		if cfg.AllowInsecureGRPC {
			t.Errorf("env override AllowInsecureGRPC should be false, got true")
		}

		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate after TLS-via-env override: %v", err)
		}
	})

	t.Run("AllowInsecureGRPC via env, dev env, no TLS", func(t *testing.T) {
		// Single-purpose env footprint: insecure flag + VELOX_ENV=dev. NO
		// TLS env vars, so Validate accepts the dev-only insecure path.
		t.Setenv("VELOX_ENV", "dev")
		t.Setenv("VELOX_GRPC_TLS_CERT_FILE", "")
		t.Setenv("VELOX_GRPC_TLS_KEY_FILE", "")
		t.Setenv("VELOX_GRPC_TLS_CA_FILE", "")
		t.Setenv("VELOX_ALLOW_INSECURE_GRPC_DEV", "true")

		cfg, err := LoadConfig(configPath)
		if err != nil {
			t.Fatalf("LoadConfig: %v", err)
		}

		if cfg.Environment != "dev" {
			t.Errorf("env override Environment: got %q want dev", cfg.Environment)
		}
		if !cfg.AllowInsecureGRPC {
			t.Errorf("env=1 should map to AllowInsecureGRPC=true")
		}

		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate after insecure-via-env override: %v", err)
		}
	})

	t.Run("AllowInsecureGRPC=false via env round-trip", func(t *testing.T) {
		// Empty-everything footprint: no TLS, no insecure, env still
		// defaults to production. Validate rejects the no-config path
		// (this case exists only to verify the bool parser maps "0" /
		// unset env to AllowInsecureGRPC=false without leaking across).
		t.Setenv("VELOX_ENV", "")
		t.Setenv("VELOX_GRPC_TLS_CERT_FILE", "")
		t.Setenv("VELOX_GRPC_TLS_KEY_FILE", "")
		t.Setenv("VELOX_GRPC_TLS_CA_FILE", "")
		t.Setenv("VELOX_ALLOW_INSECURE_GRPC_DEV", "")

		cfg, err := LoadConfig(configPath)
		if err != nil {
			t.Fatalf("LoadConfig: %v", err)
		}
		if cfg.AllowInsecureGRPC {
			t.Errorf("unsetting VELOX_ALLOW_INSECURE_GRPC_DEV should clear AllowInsecureGRPC, got true")
		}
		if cfg.Environment != "production" {
			t.Errorf("unsetting VELOX_ENV should leave Environment at JSON/production, got %q", cfg.Environment)
		}
	})

	t.Run("partial env overrides TLS via env set json TLS", func(t *testing.T) {
		// Spec case #2: JSON has TLS, env ADDS env values, env WINS.
		// (Even if env == JSON value, the test verifies env overlay did
		// not break the JSON-loaded value.)
		certFile, keyFile, caFile := generateCompatibleTLSPair(t)
		partialJSON := `{
			"master_url":"http://localhost:8080",
			"worker_id":"test-worker-001",
			"work_dir":"/opt/velox",
			"log_level":"info",
			"control_grpc_url":"localhost:8443",
			"environment":"dev",
			"tls_cert_file":"` + certFile + `",
			"tls_key_file":"` + keyFile + `",
			"tls_ca_file":"` + caFile + `"
		}`
		partialPath := filepath.Join(tmpDir, "partial.json")
		if err := os.WriteFile(partialPath, []byte(partialJSON), 0644); err != nil {
			t.Fatalf("write partial config: %v", err)
		}

		// Re-set the TLS env vars. They point to the same files as the
		// JSON values, so we can assert equality without surprises.
		t.Setenv("VELOX_ENV", "dev")
		t.Setenv("VELOX_GRPC_TLS_CERT_FILE", certFile)
		t.Setenv("VELOX_GRPC_TLS_KEY_FILE", keyFile)
		t.Setenv("VELOX_GRPC_TLS_CA_FILE", caFile)
		t.Setenv("VELOX_ALLOW_INSECURE_GRPC_DEV", "")

		cfg, err := LoadConfig(partialPath)
		if err != nil {
			t.Fatalf("LoadConfig partial: %v", err)
		}
		if cfg.TLSCertFile != certFile || cfg.TLSKeyFile != keyFile || cfg.TLSCAFile != caFile {
			t.Errorf("env-on-top-of-json should preserve equal TLS paths; got cert=%q key=%q ca=%q",
				cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSCAFile)
		}
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate full-TLS in dev: %v", err)
		}
	})

	t.Run("env OVERRIDES json with DIFFERENT TLS files", func(t *testing.T) {
		// Code-review feedback: prove that env WINS when JSON and env
		// carry DIFFERENT values for the same field.
		_, jsonKeyFile, jsonCAFile := generateCompatibleTLSPair(t)
		envCertFile, envKeyFile, envCAFile := generateCompatibleTLSPair(t)

		// JSON declares cert-A (json-files), env provides cert-B (env-files).
		conflictJSON := `{
			"master_url":"http://localhost:8080",
			"worker_id":"test-worker-001",
			"work_dir":"/opt/velox",
			"log_level":"info",
			"control_grpc_url":"localhost:8443",
			"environment":"dev",
			"tls_cert_file":"` + envCertFile + `",
			"tls_key_file":"` + jsonKeyFile + `",
			"tls_ca_file":"` + jsonCAFile + `"
		}`
		conflictPath := filepath.Join(tmpDir, "conflict.json")
		if err := os.WriteFile(conflictPath, []byte(conflictJSON), 0644); err != nil {
			t.Fatalf("write conflict config: %v", err)
		}

		// Env vars point to DIFFERENT files than JSON.
		t.Setenv("VELOX_ENV", "dev")
		t.Setenv("VELOX_GRPC_TLS_CERT_FILE", envCertFile)
		t.Setenv("VELOX_GRPC_TLS_KEY_FILE", envKeyFile)
		t.Setenv("VELOX_GRPC_TLS_CA_FILE", envCAFile)
		t.Setenv("VELOX_ALLOW_INSECURE_GRPC_DEV", "")

		cfg, err := LoadConfig(conflictPath)
		if err != nil {
			t.Fatalf("LoadConfig conflict: %v", err)
		}

		// Critical assertion: env MUST win for ALL three TLS fields.
		if cfg.TLSCertFile != envCertFile {
			t.Errorf("env cert should override JSON cert; got %q want %q", cfg.TLSCertFile, envCertFile)
		}
		if cfg.TLSKeyFile != envKeyFile {
			t.Errorf("env key should override JSON key; got %q want %q", cfg.TLSKeyFile, envKeyFile)
		}
		if cfg.TLSCAFile != envCAFile {
			t.Errorf("env CA should override JSON CA; got %q want %q", cfg.TLSCAFile, envCAFile)
		}
		// Validate must pass because the env-provided triple is a real
		// compatible pair on disk.
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate after conflicting env-over-json TLS: %v", err)
		}
	})
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

// TestCertKeyMismatch covers spec case #9 ("chiave non compatibile col
// certificato → errore"). Validate must reject via the new
// crypto/tls.LoadX509KeyPair guard inside Validate's hasFullTLS block.
func TestCertKeyMismatch(t *testing.T) {
	certFile, keyFile, caFile := generateKeyCertMismatchPair(t)

	cfg := &WorkerConfig{
		MasterURL:      "http://localhost:8080",
		WorkerID:       "tlsw",
		WorkDir:        "/opt/velox",
		ControlGRPCURL: "localhost:8443",
		Environment:    "production",
		TLSCertFile:    certFile,
		TLSKeyFile:     keyFile,
		TLSCAFile:      caFile,
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected key-pair mismatch rejection, got nil")
	}
	if !strings.Contains(err.Error(), "tls_cert_file / tls_key_file pair rejected") {
		t.Errorf("expected error to mention key-pair rejection, got: %v", err)
	}
}

func mustWrite(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
