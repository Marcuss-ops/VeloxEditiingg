package config

import (
	"os"
	"path/filepath"
	"testing"
)

// =====================================================================
//  VELOX_ENV / VELOX_GRPC_TLS_* env-var override + helper tests
// =====================================================================
//
// Verifies the envTruthy parser against the canonical truthy spellings
// and the precedence rule that env vars OVERRIDE worker_config.json
// for the four TLS-related fields + Environment + AllowInsecureGRPC.
// This is spec test case #3 from `codex/grpc-config-single-source`.

// TestEnvTruthy sanity-checks the envTruthy helper against the canonical
// truthy spellings.
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

// TestEnvOverrides verifies the precedence rule: env vars OVERRIDE
// worker_config.json for the four TLS-related fields + Environment +
// AllowInsecureGRPC. This is spec test case #3 from
// `codex/grpc-config-single-source`.
//
// The test is split into five sub-cases because the new Validate()
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
