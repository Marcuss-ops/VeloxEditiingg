package config

import (
	"fmt"
	"os"
	"path/filepath"
)

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
	c := &Config{
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
	return c
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
	// GRPC control-plane fail-fast: if push mode is the primary delivery
	// channel then gRPC must be enabled, otherwise the master accepts HTTP
	// API calls but workers have no way to receive JobOffer/JobLeaseGranted
	// and silently degrade to "no jobs ever picked up".
	if c.Server.GRPCPushMode && c.Server.GRPCPort <= 0 {
		return fmt.Errorf(
			"config: GRPCPushMode=true requires VELOX_GRPC_PORT>0 (got %d). "+
				"Either set VELOX_GRPC_PORT or disable VELOX_GRPC_PUSH_MODE.",
			c.Server.GRPCPort)
	}

	// Worker policy: canonical, non-duplicated validator.
	if err := ValidateProductionWorkers(c.Workers.AllowedWorkerIDs); err != nil {
		return fmt.Errorf("config: VELOX_ALLOWED_WORKERS: %w", err)
	}

	// NopBlobStore is a development-only escape hatch.  It MUST NOT be
	// active in production — this guard uses the canonical Server.GinMode
	// and VELOX_ENVIRONMENT env var, centralised here so no caller can
	// silently bypass it.
	if c.Runtime.AllowNopBlobStoreDev {
		if c.Server.GinMode == "release" {
			return fmt.Errorf(
				"config: VELOX_ALLOW_NOP_BLOBSTORE_DEV=true is forbidden when GIN_MODE=release")
		}
		if c.Runtime.Environment == "production" || c.Runtime.Environment == "prod" {
			return fmt.Errorf(
				"config: VELOX_ALLOW_NOP_BLOBSTORE_DEV=true is forbidden in environment=%q", c.Runtime.Environment)
		}
	}

	return nil
}
