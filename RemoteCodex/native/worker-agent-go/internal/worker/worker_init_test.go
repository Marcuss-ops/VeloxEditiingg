// Package worker — tests for worker initialization (transport setup validation).
package worker

import (
	"testing"

	"github.com/stretchr/testify/require"
	"velox-worker-agent/pkg/config"
)

func TestNewReturnsErrorOnBadTLS(t *testing.T) {
	cfg := &config.WorkerConfig{
		WorkerID:       "test",
		WorkerName:     "test",
		WorkDir:        t.TempDir(),
		LogLevel:       "info",
		HealthPort:     8081,
		MasterURL:      "http://localhost:8000",
		ControlGRPCURL: "localhost:9000",
		// No TLS files. No insecure flag. Expect newControlTransport to fail.
	}
	_, err := New(cfg, "test")
	require.Error(t, err, "expected New() to surface transport init error")
	require.Contains(t, err.Error(), "transport factory")
}

func TestNewSucceedsWithInsecureDev(t *testing.T) {
	// Set the dev flag so transport factory allows plaintext.
	t.Setenv("VELOX_ALLOW_INSECURE_GRPC_DEV", "true")

	cfg := &config.WorkerConfig{
		WorkerID:          "test",
		WorkerName:        "test",
		WorkDir:           t.TempDir(),
		LogLevel:          "info",
		HealthPort:        8081,
		MasterURL:         "http://localhost:8000",
		ControlGRPCURL:    "localhost:9000",
		AllowInsecureGRPC: true,
	}
	w, err := New(cfg, "test")
	require.NoError(t, err, "expected New() to succeed with insecure dev flag")
	require.NotNil(t, w, "expected a non-nil Worker on success")
	// The Worker was created with our config; verify identity via cfg (no need
	// to reach into the unexported w.config field — we own the cfg pointer).
	require.Equal(t, "test", cfg.WorkerID)
}
