package config

import (
	"log"
	"os"
	"path/filepath"
)

func loadDatabaseConfig() DatabaseConfig {
	raw := os.Getenv("VELOX_DB_PATH")
	if raw == "" {
		return DatabaseConfig{}
	}
	resolved := raw
	if filepath.IsAbs(raw) {
		if r, err := filepath.EvalSymlinks(raw); err == nil {
			log.Printf("config: database path resolved: %s -> %s", raw, r)
			resolved = r
		} else {
			log.Printf("config: cannot resolve symlinks for %s: %v (using original path)", raw, err)
		}
	}
	return DatabaseConfig{DBPath: resolved}
}
