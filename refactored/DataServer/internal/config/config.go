package config

import (
	"fmt"
	"os"
	"path/filepath"
)

// LegacyMasterServerURL returns the master server URL previously exposed as a
// flat field on Config. Callers should be migrated to Config.Workers.MasterServerURL.
// Kept temporarily to avoid breaking out-of-tree callers (anecdotal integrations).
// DEPRECATED: removed in the next release that bumps the minor version of
// VERSION.txt (currently v1.0.6). See spec §8. Use Config.Workers.MasterServerURL.
func (c *Config) LegacyMasterServerURL() string {
	if c == nil {
		return ""
	}
	return c.Workers.MasterServerURL
}

// FromEnv loads configuration from environment variables.
// Only sub-configs are populated — no flat field aliases.
func FromEnv() *Config {
	// First pass: determine data directory for dependent configs
	dataDir := GetDataDir()
	runtimeDir := os.Getenv("VELOX_RUNTIME_DIR")
	if runtimeDir == "" {
		if dataDir != "" {
			runtimeDir = filepath.Dir(dataDir)
		} else {
			runtimeDir = ".velox"
		}
	}
	if dataDir == "" {
		dataDir = filepath.Join(runtimeDir, "data")
	}
	secretsDir := os.Getenv("VELOX_SECRETS_DIR")
	if secretsDir == "" {
		secretsDir = filepath.Join(runtimeDir, "secrets")
	}

	// Load sub-configs
	server := loadServerConfig()
	runtime := loadRuntimeConfig(dataDir)
	database := loadDatabaseConfig()
	workers := loadWorkersConfig()
	auth := loadAuthConfig()
	storage := loadStorageConfig()
	drive := loadDriveConfig(secretsDir, dataDir)
	youtube := loadYouTubeConfig(secretsDir, dataDir)
	ansible := loadAnsibleConfig(runtime.DataDir)
	frontend := loadFrontendConfig()
	render := loadRenderConfig()
	nvidia := loadNVIDIAConfig()
	pipeline := loadPipelineConfig()

	return &Config{
		Server:   server,
		Runtime:  runtime,
		Database: database,
		Workers:  workers,
		Auth:     auth,
		Storage:  storage,
		Drive:    drive,
		YouTube:  youtube,
		Ansible:  ansible,
		Frontend: frontend,
		Render:   render,
		NVIDIA:   nvidia,
		Pipeline: pipeline,
	}
}

// Validate checks that required fields are set.
// Returns nil if the configuration is valid, or an error describing the first missing field.
func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("config: nil Config")
	}
	if c.Database.DBPath == "" {
		return fmt.Errorf("config: VELOX_DB_PATH is required (absolute path to SQLite database)")
	}
	if !filepath.IsAbs(c.Database.DBPath) {
		return fmt.Errorf("config: VELOX_DB_PATH must be an absolute path, got: %s", c.Database.DBPath)
	}
	return nil
}
