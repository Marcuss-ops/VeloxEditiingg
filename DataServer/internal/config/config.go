package config

import (
	"fmt"
	"path/filepath"
	"strings"
)

// FromEnv is the compatibility entry point for callers that still need to
// load directly from the process environment. The canonical bootstrap uses
// LoadFromEnv, which captures the raw snapshot once and freezes Config.
func FromEnv() *Config {
	return FromRaw(RawConfigFromEnv())
}

// FromRaw maps one captured, unvalidated RawConfig into the typed Config.
// RawConfig never escapes this loading boundary.
func FromRaw(raw RawConfig) *Config {
	// First pass: determine data directory for dependent configs
	dataDir := dataDirFromRaw(raw)
	runtimeDir := raw.Get("VELOX_RUNTIME_DIR")
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
	secretsDir := raw.Get("VELOX_SECRETS_DIR")
	if secretsDir == "" {
		secretsDir = filepath.Join(runtimeDir, "secrets")
	}

	// Load sub-configs.
	server := loadServerConfig(raw)
	runtime := loadRuntimeConfig(dataDir, raw)
	database := loadDatabaseConfig(raw)
	workers := loadWorkersConfig(raw)
	fleet := loadFleetConfig(raw)
	compatibility := loadCompatibilityConfig(raw)
	retention := loadRetentionConfig(raw)
	auth := loadAuthConfig(raw)
	storage := loadStorageConfig(raw)
	drive := loadDriveConfig(secretsDir, dataDir, raw)
	ansible := loadAnsibleConfig(runtime.DataDir, raw)
	render := loadRenderConfig(raw)

	pipeline := loadPipelineConfig(raw)
	m2m := loadM2MConfig(raw)
	allowedDomains := loadAllowedExternalDomains(raw)
	controlPlane, frontend := loadEndpointConfig(raw)
	c := &Config{
		Server:   server,
		Runtime:  runtime,
		Database: database, Workers: workers,
		Fleet:                  fleet,
		Compatibility:          compatibility,
		Retention:              retention,
		Auth:                   auth,
		M2M:                    m2m,
		Storage:                storage,
		Drive:                  drive,
		Ansible:                ansible,
		Render:                 render,
		Pipeline:               pipeline,
		ControlPlane:           controlPlane,
		Frontend:               frontend,
		AllowedExternalDomains: allowedDomains,
	}
	loadProcessConfig(c, raw)
	loadOperationalRuntimeConfigInto(c, raw)
	c.validationErrors = operationalParseErrors(raw)
	return c
}

// Validate checks that required fields are set.
// Returns nil if the configuration is valid, or an error describing the first missing field.
func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("config: nil Config")
	}
	if strings.TrimSpace(c.Auth.ProjectBridgeContractVersion) == "" {
		c.Auth.ProjectBridgeContractVersion = "instaedit.velox.project-bridge.v1"
	}
	if c.Auth.ProjectBridgeContractVersion != "instaedit.velox.project-bridge.v1" {
		return fmt.Errorf("config: VELOX_PROJECT_BRIDGE_CONTRACT_VERSION must be instaedit.velox.project-bridge.v1")
	}
	// Postgres is not a supported runtime backend (removed 2026-08-10).
	// Reject it at the configuration boundary so production bootstrap never
	// reaches database.Open (and therefore never opens or pings a Postgres
	// connection). Keep this check before DBPath validation so an unsupported
	// Postgres deployment receives the actionable driver error even when it
	// does not provide a SQLite path.
	if strings.EqualFold(strings.TrimSpace(c.Database.Driver), "postgres") {
		return fmt.Errorf("config: VELOX_DB_DRIVER=postgres is not supported at runtime; only sqlite is configured for the master")
	}
	if c.Compatibility.Mode == "" {
		c.Compatibility.Mode = "strict"
	}
	if c.Compatibility.Mode != "compat" && c.Compatibility.Mode != "strict" {
		return fmt.Errorf("config: VELOX_COMPATIBILITY_MODE must be compat or strict, got %q", c.Compatibility.Mode)
	}
	if len(c.validationErrors) > 0 {
		return fmt.Errorf("config: invalid environment values: %s", strings.Join(c.validationErrors, "; "))
	}
	if err := validateConfiguredEndpoints(c); err != nil {
		return err
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
	} // NopBlobStore is a development-only escape hatch.  It MUST NOT be
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
				"config: VELOX_ALLOW_NOP_BLOBSTORE_DEV=true is forbidden in environment=%q",
				c.Runtime.Environment)
		}
	}
	if c.Runtime.AllowLoopbackAdminAuthDev {
		if c.Server.GinMode == "release" || c.Runtime.Environment == "production" || c.Runtime.Environment == "prod" || c.Runtime.Environment == "staging" {
			return fmt.Errorf("config: VELOX_ALLOW_LOOPBACK_ADMIN_AUTH_DEV=true is forbidden in release/staging/production")
		}
	}

	// Commit HMAC key (P0 #6/#7, Blocco 2). Two-tier guard:
	//   (1) When the key is non-empty, validate length + hex format
	//       in EVERY environment so dev/staging catches a malformed
	//       key pre-promote to production (no more "works in staging,
	//       fails in prod" surprises).
	//   (2) In production, additionally fail-fast on empty key so a
	//       stale deploy without VELOX_COMMIT_HMAC_KEY never reaches
	//       Coordinator construction. The Coordinator is itself
	//       defense-in-depth fail-closed on short keys (NewCoordinator
	//       returns an error when len(HMACKey) < 32), so this is the
	//       last line of defense, not the only one.
	if c.Runtime.CommitHMACKey != "" {
		if err := validateCommitHMACKey(c.Runtime.CommitHMACKey); err != nil {
			return fmt.Errorf("config: VELOX_COMMIT_HMAC_KEY: %w", err)
		}
	}
	if c.Runtime.Environment == "production" || c.Runtime.Environment == "prod" {
		if strings.TrimSpace(c.Runtime.CommitHMACKey) == "" {
			return fmt.Errorf("config: VELOX_COMMIT_HMAC_KEY required in environment=%q (hex of >= 32 raw bytes)", c.Runtime.Environment)
		}
	}

	return nil
}

// validateCommitHMACKey enforces a minimum of 32 RAW bytes when the
// key is hex-encoded (64 hex chars). Empty key is allowed for non-
// production deployments so dev/test fixtures stay succinct; the
// Coordinator itself rejects zero/short keys at construction time.
func validateCommitHMACKey(hexKey string) error {
	hexKey = strings.TrimSpace(hexKey)
	if hexKey == "" {
		return fmt.Errorf("required in production (32+ raw bytes / 64+ hex chars)")
	}
	if len(hexKey) < 64 {
		return fmt.Errorf("hex-encoded key too short: got %d hex chars, want >= 64", len(hexKey))
	}
	for _, c := range hexKey {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return fmt.Errorf("non-hex character %q at position %d", c, strings.Index(hexKey, string(c)))
		}
	}
	return nil
}
