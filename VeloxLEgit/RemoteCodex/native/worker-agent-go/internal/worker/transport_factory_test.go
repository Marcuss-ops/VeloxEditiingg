// Package worker — transport factory tests for startup-time TLS validation.
package worker

import (
	"strings"
	"testing"

	"velox-worker-agent/pkg/config"
)

func TestNewValidatesTransportTLSConfig(t *testing.T) {
	makeCfg := func(environment string, allowInsecure bool) *config.WorkerConfig {
		return &config.WorkerConfig{
			Environment:       environment,
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

	t.Run("insecure dev config passes", func(t *testing.T) {
		w, err := New(makeCfg("dev", true), "two-key-test")
		if err != nil {
			t.Fatalf("expected pass with insecure dev config, got: %v", err)
		}
		if w == nil {
			t.Fatal("expected non-nil Worker on success")
		}
	})

	t.Run("insecure in production fails", func(t *testing.T) {
		_, err := New(makeCfg("production", true), "two-key-test")
		if err == nil {
			t.Fatal("expected failure when AllowInsecureGRPC=true in production")
		}
		if !strings.Contains(err.Error(), "only valid in non-production environments") {
			t.Fatalf("expected environment-gated insecure gRPC error, got: %v", err)
		}
	})

	t.Run("missing TLS and insecure flag fails", func(t *testing.T) {
		_, err := New(makeCfg("dev", false), "two-key-test")
		if err == nil {
			t.Fatal("expected failure when neither TLS nor insecure dev is configured")
		}
		if !strings.Contains(err.Error(), "no TLS configured and insecure dev flag not enabled") {
			t.Fatalf("expected missing TLS/insecure config error, got: %v", err)
		}
	})

	t.Run("invalid environment fails", func(t *testing.T) {
		_, err := New(makeCfg("qa", true), "two-key-test")
		if err == nil {
			t.Fatal("expected failure for unsupported environment value")
		}
		if !strings.Contains(err.Error(), "invalid environment") {
			t.Fatalf("expected invalid environment error, got: %v", err)
		}
	})
}
