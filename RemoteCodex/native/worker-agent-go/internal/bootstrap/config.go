// Package bootstrap owns the worker-agent's initial configuration
// resolution and the RW-PROD-003 bootstrap-gate dispatch. It is the
// composition-root sidekick of cmd/velox-worker-agent/main.go: main()
// stays a thin wiring skeleton while "how do we get a validated
// *config.WorkerConfig" and "how do we run the pre-registration
// bootstrap gate" live here.
//
// The package deliberately keeps pkg/video at arm's length (dispatch
// accepts the canonical *pipeline.Runner but only touches it through a
// narrow adapter), mirroring pkg/bootstrap's own decoupling from the
// C++ render stack.
package bootstrap

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

// Default paths
const (
	// DefaultConfigPath is the canonical worker_config.json location.
	DefaultConfigPath = "/opt/velox/worker_config.json"
	// DefaultWorkDir is the canonical Velox install/work directory.
	DefaultWorkDir = "/opt/velox"
)

// ConfigOptions carries the CLI-flag layer of configuration. Precedence
// (highest wins): CLI flags > environment variables > worker_config.json
// > built-in defaults. ResolveConfig applies this struct on top of the
// env+JSON layers that pkg/config already merged.
type ConfigOptions struct {
	ConfigPath string
	WorkDir    string
	MasterURL  string
	WorkerID   string
	LogLevel   string
	// ReadyzEndpoint overrides the /health/ready mount path
	// (RW-PROD-004 A9). Empty means "keep the canonical default".
	ReadyzEndpoint string
	// Version is the build-time version injected via -ldflags.
	Version string
}

// GenerateDefaultConfig writes a default worker_config.json at
// configPath (creating parent dirs) and returns the generated config so
// the caller can print the worker ID. workDir defaults to
// DefaultWorkDir when empty.
func GenerateDefaultConfig(configPath, workDir string) (*config.WorkerConfig, error) {
	if workDir == "" {
		workDir = DefaultWorkDir
	}
	cfg := config.DefaultConfig(workDir)
	// The raw SaveConfig error is returned unwrapped: the caller prints
	// it with its own "failed to save config" prefix (matching the
	// historical single-prefix output in main.go).
	if err := config.SaveConfig(configPath, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// ResolveConfig loads or creates worker_config.json, applies the
// CLI/flag overrides and the boot-time env bindings, validates, then
// resolves the effective version + bundle identity fields. It returns
// the fully-resolved cfg and the resolved version string (the ldflags
// value, or the VERSION.txt fallback when the ldflags value is "dev").
func ResolveConfig(opts ConfigOptions) (*config.WorkerConfig, string, error) {
	cfgPath := opts.ConfigPath

	// Load existing config or create a default when the file is absent.
	var cfg *config.WorkerConfig
	var err error
	cfg, err = config.LoadConfig(cfgPath)
	if err != nil {
		// Config file doesn't exist, create default.
		if os.IsNotExist(err) {
			// Use structured event for config creation.
			dir := opts.WorkDir
			if dir == "" {
				dir = DefaultWorkDir
			}
			cfg = config.DefaultConfig(dir)
			logger.LogConfigCreated(cfgPath, cfg.WorkerID)
		} else {
			logger.LogConfigError(err)
			return nil, "", err
		}
	} else {
		// Log config loaded.
		logger.LogConfigLoaded(cfgPath, cfg.WorkerID)
	}

	// Apply command-line overrides.
	if opts.WorkDir != "" {
		cfg.WorkDir = opts.WorkDir
	}
	if opts.MasterURL != "" {
		cfg.MasterURL = opts.MasterURL
	}
	if opts.WorkerID != "" {
		cfg.WorkerID = opts.WorkerID
	}
	if opts.LogLevel != "" {
		cfg.LogLevel = opts.LogLevel
	}
	if envWorkerID := os.Getenv("VELOX_WORKER_ID"); envWorkerID != "" {
		cfg.WorkerID = envWorkerID
	}
	if envWorkerProfile := strings.TrimSpace(os.Getenv("VELOX_WORKER_PROFILE")); envWorkerProfile != "" {
		cfg.WorkerProfile = envWorkerProfile
	}
	if bundleVersion := os.Getenv("VELOX_BUNDLE_VERSION"); bundleVersion != "" {
		cfg.BundleVersion = bundleVersion
	}
	if cfg.BundleVersion == "" {
		cfg.BundleVersion = opts.Version
	}

	// Validate config.
	if err := cfg.Validate(); err != nil {
		return nil, "", err
	}
	if err := config.ValidateRemoteMasterEndpoint(cfg); err != nil {
		return nil, "", err
	}

	// Normalize worker_id to prevent double host_ prefixes and dot-format IDs.
	cfg.WorkerID = config.NormalizeWorkerID(cfg.WorkerID)
	logger.Info("[WORKER_ID] Normalized worker ID: %s", cfg.WorkerID)

	// Save config if it's new (ensures worker_id is persisted).
	if _, err := os.Stat(cfgPath); os.IsNotExist(err) {
		if err := config.SaveConfig(cfgPath, cfg); err != nil {
			logger.Warn("Failed to save config: %v", err)
		}
	}

	// Resolve the effective version. The Version from ldflags takes
	// precedence; "dev" triggers the VERSION.txt fallback.
	resolvedVersion := opts.Version
	if resolvedVersion == "dev" {
		if v := readVersionFile(cfg.WorkDir); v != "" {
			resolvedVersion = v
			logger.Info("[VERSION] Loaded version from VERSION.txt: %s", resolvedVersion)
		}
	}
	// Ensure BundleVersion is set from the resolved version if not already set.
	if cfg.BundleVersion == "" || cfg.BundleVersion == "dev" {
		cfg.BundleVersion = resolvedVersion
	}
	if bundleHash := os.Getenv("VELOX_BUNDLE_HASH"); bundleHash != "" {
		cfg.BundleHash = bundleHash
	}
	if assetCacheDir := os.Getenv("VELOX_ASSET_CACHE_DIR"); strings.TrimSpace(assetCacheDir) != "" {
		cfg.AssetCacheDir = strings.TrimSpace(assetCacheDir)
	}
	// RW-PROD-004 §3 A9: --ready-endpoint CLI flag beats the env var and
	// the JSON config (CLI > env > JSON). Empty string from the flag is a
	// no-op so the canonical /health/ready stays in force when the
	// operator does not opt in.
	if opts.ReadyzEndpoint != "" {
		cfg.ReadyzEndpoint = opts.ReadyzEndpoint
	}
	if cfg.ReadyzEndpoint == "" {
		cfg.ReadyzEndpoint = "/health/ready"
	}
	if cfg.BundleHash == "" {
		cfg.BundleHash = readTextFileFirst(cfg.WorkDir, "BUNDLE_HASH.txt")
	}
	if protocolVersion := os.Getenv("VELOX_WORKER_PROTOCOL_VERSION"); protocolVersion != "" {
		cfg.ProtocolVersion = protocolVersion
	}
	if cfg.ProtocolVersion == "" {
		cfg.ProtocolVersion = "v3"
	}
	if workerSecret := os.Getenv("VELOX_WORKER_SECRET"); workerSecret != "" {
		cfg.WorkerSecret = workerSecret
	}
	if engineVersion := os.Getenv("VELOX_ENGINE_VERSION"); engineVersion != "" {
		cfg.EngineVersion = engineVersion
	}
	if cfg.EngineVersion == "" {
		cfg.EngineVersion = resolvedVersion
	}
	if strings.TrimSpace(cfg.VideoEngineCppBin) != "" && strings.TrimSpace(os.Getenv("VELOX_VIDEO_ENGINE_CPP_BIN")) == "" {
		// Make the composition-root config authoritative for the native
		// renderer. The render client resolves the engine path from
		// VELOX_VIDEO_ENGINE_CPP_BIN, so mirror the validated config into
		// the environment before pipeline wiring.
		if err := os.Setenv("VELOX_VIDEO_ENGINE_CPP_BIN", strings.TrimSpace(cfg.VideoEngineCppBin)); err != nil {
			return nil, "", fmt.Errorf("failed to export VELOX_VIDEO_ENGINE_CPP_BIN from config: %w", err)
		}
	}

	return cfg, resolvedVersion, nil
}

// readVersionFile attempts to read version from VERSION.txt in the work directory
// as a fallback when the ldflags version is "dev".
func readVersionFile(workDir string) string {
	// Try several known locations for VERSION.txt
	candidates := []string{
		filepath.Join(workDir, "VERSION.txt"),
		filepath.Join(workDir, "..", "VERSION.txt"),
		filepath.Join(workDir, "..", "..", "VERSION.txt"),
		"/opt/velox/VERSION.txt",
		filepath.Join(workDir, "versions", "current", "VERSION.txt"),
	}
	seen := make(map[string]bool)
	for _, path := range candidates {
		abs, err := filepath.Abs(path)
		if err != nil {
			continue
		}
		if seen[abs] {
			continue
		}
		seen[abs] = true
		data, err := os.ReadFile(abs)
		if err == nil {
			v := strings.TrimSpace(string(data))
			if v != "" {
				return v
			}
		}
	}
	return ""
}

// readTextFileFirst returns the first non-empty, whitespace-trimmed
// occurrence of filename across the canonical search locations.
func readTextFileFirst(workDir, filename string) string {
	candidates := []string{
		filepath.Join(workDir, filename),
		filepath.Join(workDir, "versions", "current", filename),
		"/opt/velox/" + filename,
	}
	seen := make(map[string]bool)
	for _, path := range candidates {
		abs, err := filepath.Abs(path)
		if err != nil {
			continue
		}
		if seen[abs] {
			continue
		}
		seen[abs] = true
		data, err := os.ReadFile(abs)
		if err == nil {
			v := strings.TrimSpace(string(data))
			if v != "" {
				return v
			}
		}
	}
	return ""
}

// EnvOr returns the value of the named environment variable, trimmed of
// surrounding whitespace; if unset or empty, it returns fallback.
// Centralised here so the composition root does not sprinkle os.Getenv
// calls across its wiring.
func EnvOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

// EnvBool returns the bool value of the named environment variable.
// Accepts 1/true/yes/on (case-insensitive) as true; anything else
// (including empty / "0" / "false" / "no" / "off") as false. Missing var
// returns fallback. Used by the anti-collision gate (RW-PROD-005 §3) for
// VELOX_ALLOW_MULTI_HOST_WORKER_IDS.
func EnvBool(key string, fallback bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	switch strings.ToLower(v) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}
