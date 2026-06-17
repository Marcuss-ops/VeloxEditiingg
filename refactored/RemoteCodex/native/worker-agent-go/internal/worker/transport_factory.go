// Package worker — transport selection logic extracted from worker_init.go.
// Chooses between PollingHTTPTransport and GRPCStreamTransport based on
// config.ControlTransport, with automatic fallback when gRPC is unavailable.
package worker

import (
	"velox-shared/controltransport"
	"velox-worker-agent/internal/transport"
	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

// newControlTransport creates the appropriate ControlTransport based on config.
//
// Rules:
//   - control_transport = "grpc" → tries GRPCStreamTransport first
//     If fallback_to_http_polling is true and gRPC fails at Connect() time,
//     falls back to PollingHTTPTransport transparently.
//   - control_transport = "polling" (or empty) → PollingHTTPTransport
func newControlTransport(apiClient *api.Client, cfg *config.WorkerConfig, log *logger.Logger) controltransport.ControlTransport {
	transportMode := cfg.ControlTransport
	if transportMode == "" {
		transportMode = "polling"
	}

	switch transportMode {
	case "grpc":
		grpcURL := cfg.ControlGRPCURL
		if grpcURL == "" {
			log.Warn("[TRANSPORT] control_grpc_url not set — falling back to HTTP polling")
			pollingHTTP := transport.NewPollingHTTPTransport(apiClient, cfg.WorkerID)
			pollingHTTP.DisablePolling = cfg.DisableHTTPPolling
			return pollingHTTP
		}

		grpcTransport := transport.NewGRPCStreamTransport(grpcURL, cfg.WorkerID)

		// Phase 7: apply mTLS when configured
		if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" && cfg.TLSCAFile != "" {
			if err := grpcTransport.WithTLS(cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSCAFile); err != nil {
				log.Error("[TRANSPORT] mTLS setup failed: %v — falling back to HTTP polling", err)
				pollingHTTP := transport.NewPollingHTTPTransport(apiClient, cfg.WorkerID)
				pollingHTTP.DisablePolling = cfg.DisableHTTPPolling
				return pollingHTTP
			}
			log.Info("[TRANSPORT] mTLS enabled for gRPC stream (cert=%s)", cfg.TLSCertFile)
		}

		if cfg.FallbackToHTTPPolling == nil || *cfg.FallbackToHTTPPolling {
			pollingFallback := transport.NewPollingHTTPTransport(apiClient, cfg.WorkerID)
			pollingFallback.DisablePolling = cfg.DisableHTTPPolling
			log.Info("[TRANSPORT] Using gRPC stream transport with HTTP polling fallback (url=%s)", grpcURL)
			return transport.NewFallbackTransport(grpcTransport, pollingFallback, log)
		}

		log.Info("[TRANSPORT] Using gRPC stream transport (no fallback) (url=%s)", grpcURL)
		return grpcTransport

	default:
		pollingHTTP := transport.NewPollingHTTPTransport(apiClient, cfg.WorkerID)
		pollingHTTP.DisablePolling = cfg.DisableHTTPPolling
		if cfg.DisableHTTPPolling {
			log.Info("[TRANSPORT] Using HTTP transport (polling disabled — Phase 6)")
		} else {
			log.Info("[TRANSPORT] Using HTTP polling transport")
		}
		return pollingHTTP
	}
}
