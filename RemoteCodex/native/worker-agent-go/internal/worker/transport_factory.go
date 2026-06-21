// Package worker — transport factory for the gRPC push transport.
// This is the ONLY ControlTransport implementation the worker creates.
// HTTP polling transport and shadow mode were removed in PR3 — see the repo
// roadmap docs and PR3 commit history.
package worker

import (
	"fmt"
	"os"

	"velox-shared/controltransport"
	"velox-worker-agent/internal/transport"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

// newControlTransport creates the GRPCStreamTransport. The worker has a
// single control plane after PR3 — gRPC bidi stream with mTLS (or insecure
// dev). Returns (nil, err) on any configuration problem so Worker.Start
// can propagate the failure instead of nil-panicking on the first Connect.
//
// Validations for gRPC mode:
//   - `control_grpc_url` must be non-empty.
//   - At least one of `tls_cert_file` / `tls_key_file` / `tls_ca_file` must
//     be present OR none of them must be present. Mixing partial
//     triple-presence is rejected.
//   - Server-side auth mode requires `worker_secret`.
//   - Unauthenticated gRPC requires VELOX_ALLOW_INSECURE_GRPC_DEV=true
//     (or `AllowInsecureGRPC` set on WorkerConfig) — this prevents
//     accidental cleartext in production.
func newControlTransport(cfg *config.WorkerConfig, log *logger.Logger) (controltransport.ControlTransport, error) {
	if cfg == nil {
		return nil, fmt.Errorf("transport factory: nil WorkerConfig")
	}
	return newGRPCStreamTransport(cfg, log)
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
		return nil, fmt.Errorf("transport factory: no TLS configured. Set tls_cert_file/tls_key_file/tls_ca_file, " +
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
