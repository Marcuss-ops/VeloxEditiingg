// Package worker — transport factory tests for the two-key insecure gRPC lock.
//
// newGRPCStreamTransport enforces a double-consent pattern:
//   - Config field allow_insecure_grpc_dev: true  (in WorkerConfig)
//   - Environment variable VELOX_ALLOW_INSECURE_GRPC_DEV=true  (os.Getenv)
//
// Both must be set. A single key alone is rejected. This prevents accidental
// cleartext gRPC in production when only one of the two is accidentally set.
package worker

import (
	"strings"
	"testing"

	"velox-worker-agent/pkg/config"
)

// TestGRPCStreamTransportTwoKeyLock tests the double-consent pattern enforced
// by newGRPCStreamTransport (called through New()):
//
//   Pass:  AllowInsecureGRPC=true  +  VELOX_ALLOW_INSECURE_GRPC_DEV=true
//   Fail:  AllowInsecureGRPC=true  +  VELOX_ALLOW_INSECURE_GRPC_DEV unset
//   Fail:  AllowInsecureGRPC=false +  VELOX_ALLOW_INSECURE_GRPC_DEV=true
//   Fail:  AllowInsecureGRPC=false +  VELOX_ALLOW_INSECURE_GRPC_DEV unset
//
// The last case fails because neither TLS nor insecure flags are set.
func TestGRPCStreamTransportTwoKeyLock(t *testing.T) {
	makeCfg := func(allowInsecure bool) *config.WorkerConfig {
		return &config.WorkerConfig{
			WorkerID:          "two-key-test",
			WorkerName:        "two-key-test",
			MasterURL:         "http://localhost:8000",
			ControlGRPCURL:    "localhost:9000",
			WorkDir:           t.TempDir(),
			LogLevel:          "info",
			HealthPort:        8081,
			AllowInsecureGRPC: allowInsecure,
		}
	}

	t.Run("both keys set — pass", func(t *testing.T) {
		t.Setenv("VELOX_ALLOW_INSECURE_GRPC_DEV", "true")
		w, err := New(makeCfg(true), "two-key-test")
		if err != nil {
			t.Fatalf("expected pass with both keys, got: %v", err)
		}
		if w == nil {
			t.Fatal("expected non-nil Worker on success")
		}
	})

	t.Run("config key only — fail", func(t *testing.T) {
		// VELOX_ALLOW_INSECURE_GRPC_DEV is NOT set
		_, err := New(makeCfg(true), "two-key-test")
		if err == nil {
			t.Fatal("expected failure when AllowInsecureGRPC=true but VELOX_ALLOW_INSECURE_GRPC_DEV is unset")
		}
		if !strings.Contains(err.Error(), "insecure gRPC requested") {
			t.Fatalf("expected 'insecure gRPC requested' error, got: %v", err)
		}
	})

	t.Run("env key only — fail", func(t *testing.T) {
		t.Setenv("VELOX_ALLOW_INSECURE_GRPC_DEV", "true")
		// AllowInsecureGRPC defaults to false
		_, err := New(makeCfg(false), "two-key-test")
		if err == nil {
			t.Fatal("expected failure when env is set but AllowInsecureGRPC=false (and no TLS)")
		}
		if !strings.Contains(err.Error(), "no TLS configured") {
			t.Fatalf("expected 'no TLS configured' error, got: %v", err)
		}
	})

	t.Run("neither key set — fail", func(t *testing.T) {
		// Neither AllowInsecureGRPC nor VELOX_ALLOW_INSECURE_GRPC_DEV is set
		_, err := New(makeCfg(false), "two-key-test")
		if err == nil {
			t.Fatal("expected failure when neither key is set (and no TLS)")
		}
		if !strings.Contains(err.Error(), "no TLS configured") {
			t.Fatalf("expected 'no TLS configured' error, got: %v", err)
		}
	})
}
