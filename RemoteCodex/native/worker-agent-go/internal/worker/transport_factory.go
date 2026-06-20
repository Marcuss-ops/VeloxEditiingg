// Package worker — transport selection extracted from worker_init.go.
// Creates a GRPCStreamTransport or PollingHTTPTransport based on config.
// gRPC push is the default and recommended transport (PR12).
package worker

import (
	"fmt"
	"os"
	"time"

	"velox-shared/controltransport"
	"velox-worker-agent/internal/transport"
	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

// maxGRPCConnectFailuresBeforeFallback is the number of consecutive gRPC
// connect failures before the worker falls back to HTTP polling (PR12).
const maxGRPCConnectFailuresBeforeFallback = 3

// newControlTransport creates a ControlTransport based on config.JobDelivery (PR12).
// Supported modes:
//   - "push" (default): GRPCStreamTransport with mTLS or insecure dev.
//   - "polling": PollingHTTPTransport using the remaining V2 HTTP endpoints.
// Returns (nil, err) on any configuration problem so Worker.Start can propagate
// the failure instead of nil-panicking on the first Connect call.
//
// Validations enforced for push (gRPC) mode:
//   - `control_grpc_url` must be non-empty.
//   - At least one of `tls_cert_file` / `tls_key_file` / `tls_ca_file` must
//     be present OR none of them must be present. Mixing partial
//     triple-presence is rejected.
//   - Server-side auth mode requires `worker_secret`.
//   - Unauthenticated gRPC requires VELOX_ALLOW_INSECURE_GRPC_DEV=true
//     (or `AllowInsecureGRPC` set on WorkerConfig) — this prevents
//     accidental cleartext in production.
//
// Validations enforced for polling (HTTP) mode:
//   - `master_url` must be non-empty (used to create the api.Client).
func newControlTransport(cfg *config.WorkerConfig, log *logger.Logger) (controltransport.ControlTransport, error) {
	if cfg == nil {
		return nil, fmt.Errorf("transport factory: nil WorkerConfig")
	}

	mode := cfg.JobDelivery
	if mode == "" {
		mode = "push"
	}

	switch mode {
	case "polling":
		return newHTTPPollingTransport(cfg, log)
	default:
		return newGRPCStreamTransport(cfg, log)
	}
}

// newHTTPPollingTransport creates a PollingHTTPTransport using the master's
// V2 HTTP endpoints. The HTTP client is created from cfg.MasterURL.
// Exported for use by the gRPC-failure fallback path in worker.go (PR12).
func newHTTPPollingTransport(cfg *config.WorkerConfig, log *logger.Logger) (controltransport.ControlTransport, error) {
	if cfg.MasterURL == "" {
		return nil, fmt.Errorf("transport factory: master_url is required for HTTP transport")
	}

	client := api.NewClient(cfg.MasterURL,
		api.WithWorkerID(cfg.WorkerID),
		api.WithRetry(3, 5*time.Second),
	)

	httpTransport := transport.NewPollingHTTPTransport(
		client,
		cfg.WorkerID,
		transport.DefaultPollingTransportConfig(),
	)

	log.Info("[TRANSPORT] Using HTTP polling transport (url=%s)", cfg.MasterURL)
	return httpTransport, nil
}

// newHTTPPollingTransportUnvalidated creates a fallback HTTP transport
// without repeating all config validations (PR12 gRPC-failure fallback).
// This is only called from inside the Start() reconnect loop after gRPC
// validation has already passed — the HTTP URL is known-good.
func newHTTPPollingTransportUnvalidated(cfg *config.WorkerConfig, log *logger.Logger) controltransport.ControlTransport {
	client := api.NewClient(cfg.MasterURL,
		api.WithWorkerID(cfg.WorkerID),
		api.WithRetry(3, 5*time.Second),
	)

	httpTransport := transport.NewPollingHTTPTransport(
		client,
		cfg.WorkerID,
		transport.DefaultPollingTransportConfig(),
	)

	log.Info("[TRANSPORT] Using HTTP polling transport (fallback — url=%s)", cfg.MasterURL)
	return httpTransport
}

// newGRPCStreamTransport creates a GRPCStreamTransport with TLS or insecure dev.
func newGRPCStreamTransport(cfg *config.WorkerConfig, log *logger.Logger) (controltransport.ControlTransport, error) {
	if cfg.ControlGRPCURL == "" {
		return nil, fmt.Errorf("transport factory: control_grpc_url is required — worker cannot start")
	}

	if err := validateTLSConfigTriple(cfg); err != nil {
		return nil, err
	}

	if cfg.AllowInsecureGRPC {
		if !insecureDevFlagSet() {
			return nil, fmt.Errorf("transport factory: insecure gRPC requested but VELOX_ALLOW_INSECURE_GRPC_DEV=true not set")
		}
		log.Info("[TRANSPORT] Insecure gRPC allowed by dev flag — NEVER USE IN PRODUCTION")
	} else if !hasAnyTLS(cfg) {
		return nil, fmt.Errorf("transport factory: no TLS configured. Set tls_cert_file/tls_key_file/tls_ca_file, "+
			"or enable allow_insecure_grpc_dev=true only for local development")
	}

	if cfg.RequiresWorkerSecret && cfg.WorkerSecret == "" {
		return nil, fmt.Errorf("transport factory: auth mode requires worker_secret to be set")
	}

	grpcTransport := transport.NewGRPCStreamTransport(cfg.ControlGRPCURL, cfg.WorkerID)

	if hasAnyTLS(cfg) {
		if err := grpcTransport.WithTLS(cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSCAFile); err != nil {
			return nil, fmt.Errorf("transport factory: mTLS setup failed: %w", err)
		}
		log.Info("[TRANSPORT] mTLS enabled for gRPC stream (cert=%s)", cfg.TLSCertFile)
	}

	log.Info("[TRANSPORT] Using gRPC push transport (url=%s)", cfg.ControlGRPCURL)
	return grpcTransport, nil
}

// validateTLSConfigTriple rejects partial TLS configuration.
// Either the triple (cert + key + ca) is fully set, or none of the three is set.
func validateTLSConfigTriple(cfg *config.WorkerConfig) error {
	hasCert := cfg.TLSCertFile != ""
	hasKey := cfg.TLSKeyFile != ""
	hasCA := cfg.TLSCAFile != ""

	if !hasCert && !hasKey && !hasCA {
		return nil
	}
	if hasCert && hasKey && hasCA {
		return nil
	}
	missing := []string{}
	if !hasCert {
		missing = append(missing, "tls_cert_file")
	}
	if !hasKey {
		missing = append(missing, "tls_key_file")
	}
	if !hasCA {
		missing = append(missing, "tls_ca_file")
	}
	return fmt.Errorf("transport factory: partial TLS configuration. "+
		"Provide all three (cert/key/ca) or none. Missing: %v", missing)
}

// hasAnyTLS reports whether any TLS file is configured.
func hasAnyTLS(cfg *config.WorkerConfig) bool {
	return cfg.TLSCertFile != "" || cfg.TLSKeyFile != "" || cfg.TLSCAFile != ""
}

// insecureDevFlagSet returns true when VELOX_ALLOW_INSECURE_GRPC_DEV is enabled.
// Reads the environment directly via os.Getenv — no platform-specific wrappers.
func insecureDevFlagSet() bool {
	v := os.Getenv("VELOX_ALLOW_INSECURE_GRPC_DEV")
	if v == "" {
		return false
	}
	return v == "1" || v == "true" || v == "TRUE" || v == "yes"
}
