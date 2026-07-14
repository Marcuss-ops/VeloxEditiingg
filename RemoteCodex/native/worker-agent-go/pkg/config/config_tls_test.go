package config

import (
	"strings"
	"testing"
	"time"
)

// =====================================================================
//  TLS-related Validate() & accessor tests
// =====================================================================
//
// Covers the 11-case PR 1 combinatorial table (full triple OK,
// partial-TLS rejection, no-TLS rejection, dev insecure, prod insecure
// rejection, cert-file-missing, TLS+insecure both active, invalid
// environment literal, full triple in staging) plus the GRPCTLS
// accessor contract and the RW-PROD-001 cert-expiry / key-permissions
// guards (TS-1.1/1.2/1.3 and 1.4/1.5). Cert/key pairing guard
// (MismatchLoadedKeyPair) is pinned by TestCertKeyMismatch.

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
