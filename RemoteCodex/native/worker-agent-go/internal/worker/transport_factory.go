// Package worker — transport factory for the gRPC push transport.
// This is the ONLY ControlTransport implementation the worker creates.
// HTTP polling transport and shadow mode were removed in PR3 — see the repo
// roadmap docs and PR3 commit history.
//
// PR 1 (`codex/grpc-config-single-source`): this file no longer reads
// environment variables or performs TLS combinatorial validation. Both
// responsibilities moved upstream into pkg/config:
//   - env-var overlay       → pkg/config/env.go
//   - combinatorial checks  → pkg/config/config.go Validate()
//
// newGRPCStreamTransport() now takes a pre-validated GRPCTLSConfig and
// only translates it into transport.WithTLS(...) or transport-insecure.
// Any TLS misconfiguration surfaces earlier (config.Validate()), at startup,
// where it belongs — not lazily at first Recv().
package worker

import (
	"fmt"

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
// TLS validation is performed upstream by config.Validate(), which the
// caller must invoke BEFORE this is reached. We do NOT re-validate here;
// we trust the GRPCTLSConfig is consistent (either full TLS triple OR
// allow_insecure=true with environment=dev).
func newControlTransport(cfg *config.WorkerConfig, log *logger.Logger) (controltransport.ControlTransport, error) {
	if cfg == nil {
		return nil, fmt.Errorf("transport factory: nil WorkerConfig")
	}

	// Surfacing the proto-level invariants here keeps the existing error
	// shape ("transport factory: ... — worker cannot start") for log
	// parsers and runbooks, even though the underlying validation lives
	// in pkg/config.
	if cfg.ControlGRPCURL == "" {
		return nil, fmt.Errorf("transport factory: control_grpc_url is required — worker cannot start")
	}
	if cfg.RequiresWorkerSecret && cfg.WorkerSecret == "" {
		return nil, fmt.Errorf("transport factory: auth mode requires worker_secret to be set")
	}

	return newGRPCStreamTransport(cfg.ControlGRPCURL, cfg.WorkerID, cfg.GRPCTLS(), log)
}

// newGRPCStreamTransport instantiates the GRPCStreamTransport using a
// pre-validated GRPCTLSConfig. Two branches:
//
//   - cfg.AllowInsecureDev == true → plaintext transport (dev mode only,
//     enforced upstream by config.Validate which requires environment=dev).
//
//   - else → transport.WithTLS(cert, key, ca). The config.Validate layer
//     has already proven all three are present and cert is on disk.
func newGRPCStreamTransport(
	grpcURL string,
	workerID string,
	tlsCfg config.GRPCTLSConfig,
	log *logger.Logger,
) (controltransport.ControlTransport, error) {
	grpcTransport := transport.NewGRPCStreamTransport(grpcURL, workerID)

	if tlsCfg.AllowInsecureDev {
		log.Info("[TRANSPORT] Insecure gRPC allowed by dev flag — NEVER USE IN PRODUCTION")
		log.Info("[TRANSPORT] Using gRPC push transport (url=%s)", grpcURL)
		return grpcTransport, nil
	}

	if err := grpcTransport.WithTLS(tlsCfg.CertFile, tlsCfg.KeyFile, tlsCfg.CAFile); err != nil {
		return nil, fmt.Errorf("transport factory: mTLS setup failed: %w", err)
	}
	log.Info("[TRANSPORT] mTLS enabled for gRPC stream (cert=%s)", tlsCfg.CertFile)
	log.Info("[TRANSPORT] Using gRPC push transport (url=%s)", grpcURL)
	return grpcTransport, nil
}
