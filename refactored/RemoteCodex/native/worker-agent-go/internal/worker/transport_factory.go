// Package worker — transport selection extracted from worker_init.go.
// Creates a GRPCStreamTransport exclusively. HTTP polling has been removed.
package worker

import (
	"velox-shared/controltransport"
	"velox-worker-agent/internal/transport"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

// newControlTransport creates a GRPCStreamTransport based on config.
// gRPC is the only transport mode. The worker will not start if gRPC
// is not properly configured.
func newControlTransport(cfg *config.WorkerConfig, log *logger.Logger) controltransport.ControlTransport {
	grpcURL := cfg.ControlGRPCURL
	if grpcURL == "" {
		log.Error("[TRANSPORT] control_grpc_url is required — worker cannot start")
		return nil
	}

	grpcTransport := transport.NewGRPCStreamTransport(grpcURL, cfg.WorkerID)

	// Apply mTLS when configured
	if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" && cfg.TLSCAFile != "" {
		if err := grpcTransport.WithTLS(cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSCAFile); err != nil {
			log.Error("[TRANSPORT] mTLS setup failed: %v", err)
			return nil
		}
		log.Info("[TRANSPORT] mTLS enabled for gRPC stream (cert=%s)", cfg.TLSCertFile)
	}

	log.Info("[TRANSPORT] Using gRPC stream transport (url=%s)", grpcURL)
	return grpcTransport
}
