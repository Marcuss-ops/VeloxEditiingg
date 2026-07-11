package main

import (
	"strings"
	"testing"

	"velox-server/internal/config"
)

// TestEnforceGRPCRequireTLS covers RW-PROD-001 M2: the helper that gates
// master bootstrap on VELOX_GRPC_REQUIRE_TLS=true must be inference-testable.
// We previously called log.Fatalf directly, which forced any test into a
// process-exit wrapper. Now that the helper returns an error, this test
// proves all four branches with zero process-side effects.
//
// Cases:
//  1. env unset / "false" / whitespace → no-op (returns nil)
//  2. env="true" + all 3 TLS paths populated → no-op (returns nil)
//  3. env="true" + one TLS path missing → returns non-nil, error names env var
//  4. env="true" + all 3 missing → returns non-nil, error lists all three
func TestEnforceGRPCRequireTLS(t *testing.T) {
	t.Run("env unset \u2192 no-op", func(t *testing.T) {
		t.Setenv("VELOX_GRPC_REQUIRE_TLS", "")
		if err := enforceGRPCRequireTLS(&config.Config{}); err != nil {
			t.Errorf("expected nil when env unset, got: %v", err)
		}
	})
	t.Run("env whitespace \u2192 no-op", func(t *testing.T) {
		t.Setenv("VELOX_GRPC_REQUIRE_TLS", "   ")
		if err := enforceGRPCRequireTLS(&config.Config{}); err != nil {
			t.Errorf("expected nil for non-'true' whitespace env, got: %v", err)
		}
	})
	t.Run("env=true + all TLS paths populated \u2192 no-op", func(t *testing.T) {
		t.Setenv("VELOX_GRPC_REQUIRE_TLS", "true")
		cfg := &config.Config{Server: config.ServerConfig{
			GRPCTLSCertFile: "/opt/velox/certs/master/server.crt",
			GRPCTLSKeyFile:  "/opt/velox/certs/master/server.key",
			GRPCTLSCAFile:   "/opt/velox/certs/intermediate/ca.crt",
		}}
		if err := enforceGRPCRequireTLS(cfg); err != nil {
			t.Errorf("expected nil when all TLS paths populated, got: %v", err)
		}
	})
	t.Run("env=true + missing cert \u2192 error names env var", func(t *testing.T) {
		t.Setenv("VELOX_GRPC_REQUIRE_TLS", "true")
		cfg := &config.Config{Server: config.ServerConfig{
			GRPCTLSKeyFile: "/opt/velox/certs/master/server.key",
			GRPCTLSCAFile:  "/opt/velox/certs/intermediate/ca.crt",
			// GRPCTLSCertFile intentionally empty
		}}
		err := enforceGRPCRequireTLS(cfg)
		if err == nil {
			t.Fatal("expected error when GRPCTLSCertFile missing under opt-in")
		}
		if !strings.Contains(err.Error(), "VELOX_GRPC_TLS_CERT_FILE") {
			t.Errorf("expected error to name VELOX_GRPC_TLS_CERT_FILE, got: %v", err)
		}
		if !strings.Contains(err.Error(), "RW-PROD-001 A5") {
			t.Errorf("expected error to carry RW-PROD-001 A5 audit tag, got: %v", err)
		}
	})
	t.Run("env=true + all 3 missing \u2192 error lists all", func(t *testing.T) {
		t.Setenv("VELOX_GRPC_REQUIRE_TLS", "true")
		err := enforceGRPCRequireTLS(&config.Config{})
		if err == nil {
			t.Fatal("expected error when all three TLS paths missing under opt-in")
		}
		for _, w := range []string{"VELOX_GRPC_TLS_CERT_FILE", "VELOX_GRPC_TLS_KEY_FILE", "VELOX_GRPC_TLS_CA_FILE"} {
			if !strings.Contains(err.Error(), w) {
				t.Errorf("expected error to name %s, got: %v", w, err)
			}
		}
	})
}
