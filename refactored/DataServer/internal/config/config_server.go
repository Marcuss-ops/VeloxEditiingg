package config

import "os"

func loadServerConfig() ServerConfig {
	c := ServerConfig{
		Port:            intFromEnv("VELOX_MASTER_PORT", 8000, 1),
		StudioPort:      intFromEnv("VELOX_STUDIO_PORT", 5000, 0),
		GRPCPort:        intFromEnv("VELOX_GRPC_PORT", 0, 0),
		TLSCertFile:     os.Getenv("VELOX_TLS_CERT_FILE"),
		TLSKeyFile:      os.Getenv("VELOX_TLS_KEY_FILE"),
		GRPCTLSCertFile: os.Getenv("VELOX_GRPC_TLS_CERT_FILE"),
		GRPCTLSKeyFile:  os.Getenv("VELOX_GRPC_TLS_KEY_FILE"),
		GRPCTLSCAFile:   os.Getenv("VELOX_GRPC_TLS_CA_FILE"),
	}
	c.AllowLocalhost = boolFromEnv("VELOX_ALLOW_LOCALHOST_MASTER", false) ||
		boolFromEnv("VELOX_DEV_MODE", false)
	return c
}
