package config

import (
	"strings"
)

func loadServerConfig(raw RawConfig) ServerConfig {
	c := ServerConfig{
		Port:            raw.Int("VELOX_MASTER_PORT", 8000, 1),
		StudioPort:      raw.Int("VELOX_STUDIO_PORT", 5000, 0),
		GRPCPort:        raw.Int("VELOX_GRPC_PORT", 0, 0),
		GRPCPushMode:    raw.Bool("VELOX_GRPC_PUSH_MODE", true), // default true: gRPC push is the primary job delivery path
		TLSCertFile:     raw.Get("VELOX_TLS_CERT_FILE"),
		TLSKeyFile:      raw.Get("VELOX_TLS_KEY_FILE"),
		GRPCTLSCertFile: raw.Get("VELOX_GRPC_TLS_CERT_FILE"),
		GRPCTLSKeyFile:  raw.Get("VELOX_GRPC_TLS_KEY_FILE"),
		GRPCTLSCAFile:   raw.Get("VELOX_GRPC_TLS_CA_FILE"),
		GRPCRequireTLS:  raw.Bool("VELOX_GRPC_REQUIRE_TLS", false),
		LogRoutesAtBoot: raw.Bool("VELOX_LOG_ROUTES_AT_BOOT", false),
		GinMode:         strings.TrimSpace(raw.Get("GIN_MODE")),
	}
	c.AllowLocalhost = raw.Bool("VELOX_ALLOW_LOCALHOST_MASTER", false) ||
		raw.Bool("VELOX_DEV_MODE", false)
	return c
}
