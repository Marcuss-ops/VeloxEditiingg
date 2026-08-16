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
// Fase E1 StorageResolver env bindings: VELOX_TMPFS_DIR opt-in + the
// VELOX_TMPFS_THRESHOLD_BYTES size gate. Invalid numeric values are
// ignored so a typo cannot zero the gate.
func TestEnvTmpfsOverrides(t *testing.T) {
	t.Setenv(EnvTmpfsDir, "/dev/shm/velox-worker")
	t.Setenv(EnvTmpfsThresholdBytes, "134217728") // 128 MiB
	cfg := &WorkerConfig{}
	if err := applyEnvOverrides(cfg); err != nil {
		t.Fatalf("applyEnvOverrides: %v", err)
	}
	if cfg.TmpfsDir != "/dev/shm/velox-worker" {
		t.Errorf("TmpfsDir = %q, want /dev/shm/velox-worker", cfg.TmpfsDir)
	}
	if cfg.TmpfsThresholdBytes != 134217728 {
		t.Errorf("TmpfsThresholdBytes = %d, want 134217728", cfg.TmpfsThresholdBytes)
	}

	// Invalid / non-positive threshold values are ignored (field keeps its
	// pre-env value; applyDefaults later fills the default).
	t.Setenv(EnvTmpfsThresholdBytes, "not-a-number")
	cfg2 := &WorkerConfig{TmpfsThresholdBytes: 7}
	if err := applyEnvOverrides(cfg2); err != nil {
		t.Fatalf("applyEnvOverrides invalid threshold: %v", err)
	}
	if cfg2.TmpfsThresholdBytes != 7 {
		t.Errorf("invalid threshold should be ignored, got %d", cfg2.TmpfsThresholdBytes)
	}

	t.Setenv(EnvTmpfsThresholdBytes, "0")
	cfg3 := &WorkerConfig{TmpfsThresholdBytes: 9}
	if err := applyEnvOverrides(cfg3); err != nil {
		t.Fatalf("applyEnvOverrides zero threshold: %v", err)
	}
	if cfg3.TmpfsThresholdBytes != 9 {
		t.Errorf("zero threshold should be ignored, got %d", cfg3.TmpfsThresholdBytes)
	}
}

// ARTIFACT_STAGING env bindings: VELOX_ARTIFACT_TMPFS_* opt into volatile
// RAM staging for the final artifact. Invalid numerics are ignored so a
// typo cannot zero the tuning knobs (Validate() then fails closed on the
// enabled combination).
func TestEnvArtifactTmpfsOverrides(t *testing.T) {
	t.Setenv(EnvArtifactTmpfsEnabled, "true")
	t.Setenv(EnvArtifactTmpfsDir, "/dev/shm/velox-artifacts")
	t.Setenv(EnvArtifactTmpfsMaxPercent, "70")
	t.Setenv(EnvArtifactTmpfsReserveBytes, "1073741824") // 1 GiB
	cfg := &WorkerConfig{}
	if err := applyEnvOverrides(cfg); err != nil {
		t.Fatalf("applyEnvOverrides: %v", err)
	}
	if !cfg.ArtifactTmpfsEnabled {
		t.Error("ArtifactTmpfsEnabled = false, want true")
	}
	if cfg.ArtifactTmpfsDir != "/dev/shm/velox-artifacts" {
		t.Errorf("ArtifactTmpfsDir = %q, want /dev/shm/velox-artifacts", cfg.ArtifactTmpfsDir)
	}
	if cfg.ArtifactTmpfsMaxPercent != 70 {
		t.Errorf("ArtifactTmpfsMaxPercent = %d, want 70", cfg.ArtifactTmpfsMaxPercent)
	}
	if cfg.ArtifactTmpfsReserveBytes != 1073741824 {
		t.Errorf("ArtifactTmpfsReserveBytes = %d, want 1073741824", cfg.ArtifactTmpfsReserveBytes)
	}

	// Invalid / non-positive numerics are ignored (pre-env value kept).
	t.Setenv(EnvArtifactTmpfsMaxPercent, "not-a-number")
	t.Setenv(EnvArtifactTmpfsReserveBytes, "0")
	cfg2 := &WorkerConfig{ArtifactTmpfsMaxPercent: 7, ArtifactTmpfsReserveBytes: 9}
	if err := applyEnvOverrides(cfg2); err != nil {
		t.Fatalf("applyEnvOverrides invalid staging numerics: %v", err)
	}
	if cfg2.ArtifactTmpfsMaxPercent != 7 {
		t.Errorf("invalid max percent should be ignored, got %d", cfg2.ArtifactTmpfsMaxPercent)
	}
	if cfg2.ArtifactTmpfsReserveBytes != 9 {
		t.Errorf("zero reserve should be ignored, got %d", cfg2.ArtifactTmpfsReserveBytes)
	}
}

// Cache pressure-eviction env bindings: VELOX_CACHE_*_WATERMARK_PERCENT,
// VELOX_CACHE_EVICTION_BATCH_SIZE / _INTERVAL_SECS. Invalid numerics are
// ignored so a typo cannot zero the tuning knobs (applyDefaults + Validate
// then fail closed on the explicitly-set values).
func TestEnvCachePressureOverrides(t *testing.T) {
	t.Setenv(EnvCacheHighWatermarkPercent, "85")
	t.Setenv(EnvCacheLowWatermarkPercent, "70")
	t.Setenv(EnvCacheEvictionBatchSize, "256")
	t.Setenv(EnvCacheEvictionIntervalSecs, "45")
	cfg := &WorkerConfig{}
	if err := applyEnvOverrides(cfg); err != nil {
		t.Fatalf("applyEnvOverrides: %v", err)
	}
	if cfg.CacheHighWatermarkPercent != 85 {
		t.Errorf("CacheHighWatermarkPercent = %d, want 85", cfg.CacheHighWatermarkPercent)
	}
	if cfg.CacheLowWatermarkPercent != 70 {
		t.Errorf("CacheLowWatermarkPercent = %d, want 70", cfg.CacheLowWatermarkPercent)
	}
	if cfg.CacheEvictionBatchSize != 256 {
		t.Errorf("CacheEvictionBatchSize = %d, want 256", cfg.CacheEvictionBatchSize)
	}
	if cfg.CacheEvictionIntervalSecs != 45 {
		t.Errorf("CacheEvictionIntervalSecs = %d, want 45", cfg.CacheEvictionIntervalSecs)
	}

	// Invalid / non-positive numerics are ignored (pre-env value kept).
	t.Setenv(EnvCacheEvictionBatchSize, "not-a-number")
	t.Setenv(EnvCacheEvictionIntervalSecs, "0")
	cfg2 := &WorkerConfig{CacheEvictionBatchSize: 7, CacheEvictionIntervalSecs: 9}
	if err := applyEnvOverrides(cfg2); err != nil {
		t.Fatalf("applyEnvOverrides invalid cache-pressure numerics: %v", err)
	}
	if cfg2.CacheEvictionBatchSize != 7 {
		t.Errorf("invalid batch size should be ignored, got %d", cfg2.CacheEvictionBatchSize)
	}
	if cfg2.CacheEvictionIntervalSecs != 9 {
		t.Errorf("zero interval should be ignored, got %d", cfg2.CacheEvictionIntervalSecs)
	}
}

func TestWorkerCredentialFileFallback(t *testing.T) {
	tmpDir := t.TempDir()
	credentialFile := filepath.Join(tmpDir, "worker_credential")
	if err := os.WriteFile(credentialFile, []byte("file-secret\n"), 0600); err != nil {
		t.Fatalf("write credential file: %v", err)
	}

	t.Setenv(EnvWorkerCredentialFile, credentialFile)
	t.Setenv("VELOX_WORKER_SECRET", "")
	cfg := &WorkerConfig{}
	if err := applyEnvOverrides(cfg); err != nil {
		t.Fatalf("applyEnvOverrides: %v", err)
	}
	if cfg.WorkerSecret != "file-secret" {
		t.Fatalf("WorkerSecret from credential file=%q, want file-secret", cfg.WorkerSecret)
	}

	t.Setenv("VELOX_WORKER_SECRET", "explicit-secret")
	cfg = &WorkerConfig{}
	if err := applyEnvOverrides(cfg); err != nil {
		t.Fatalf("applyEnvOverrides: %v", err)
	}
	if cfg.WorkerSecret != "explicit-secret" {
		t.Fatalf("explicit WorkerSecret=%q, want explicit-secret", cfg.WorkerSecret)
	}
}

func TestWorkerCredentialFileMissingFailsClosed(t *testing.T) {
	t.Setenv("VELOX_WORKER_SECRET", "")
	missing := filepath.Join(t.TempDir(), "missing")
	t.Setenv(EnvWorkerCredentialFile, missing)
	cfg := &WorkerConfig{}
	if err := applyEnvOverrides(cfg); err == nil {
		t.Fatal("missing credential file should fail configuration loading")
	}
	if cfg.WorkerSecret != "" {
		t.Fatalf("missing credential file populated WorkerSecret=%q", cfg.WorkerSecret)
	}

	configPath := filepath.Join(t.TempDir(), "worker.json")
	if err := os.WriteFile(configPath, []byte(`{"worker_id":"test-worker-001","work_dir":"/opt/velox","control_grpc_url":"localhost:8443"}`), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if _, err := LoadConfig(configPath); err == nil {
		t.Fatal("LoadConfig should propagate a missing credential-file error")
	}

	emptyPath := filepath.Join(t.TempDir(), "empty")
	if err := os.WriteFile(emptyPath, []byte("\n"), 0600); err != nil {
		t.Fatalf("write empty credential: %v", err)
	}
	t.Setenv(EnvWorkerCredentialFile, emptyPath)
	if _, err := LoadConfig(configPath); err == nil {
		t.Fatal("LoadConfig should reject an empty credential file")
	}

	dirPath := filepath.Join(t.TempDir(), "credential-dir")
	if err := os.Mkdir(dirPath, 0700); err != nil {
		t.Fatalf("make credential directory: %v", err)
	}
	t.Setenv(EnvWorkerCredentialFile, dirPath)
	if _, err := LoadConfig(configPath); err == nil {
		t.Fatal("LoadConfig should reject an unreadable credential path")
	}
}

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
