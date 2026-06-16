package config

import (
	"os"
	"strconv"
)

func loadServerConfig() ServerConfig {
	c := ServerConfig{
		Port:       8000,
		StudioPort: 5000,
	}
	if p := os.Getenv("VELOX_MASTER_PORT"); p != "" {
		if v, _ := strconv.Atoi(p); v > 0 {
			c.Port = v
		}
	}
	if p := os.Getenv("VELOX_STUDIO_PORT"); p != "" {
		if v, _ := strconv.Atoi(p); v >= 0 {
			c.StudioPort = v
		}
	}
	c.TLSCertFile = os.Getenv("VELOX_TLS_CERT_FILE")
	c.TLSKeyFile = os.Getenv("VELOX_TLS_KEY_FILE")
	c.AllowLocalhost = os.Getenv("VELOX_ALLOW_LOCALHOST_MASTER") == "true" ||
		os.Getenv("VELOX_ALLOW_LOCALHOST_MASTER") == "1" ||
		os.Getenv("VELOX_DEV_MODE") == "true"
	return c
}
