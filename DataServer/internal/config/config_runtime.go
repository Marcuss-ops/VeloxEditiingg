package config

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func loadRuntimeConfig(dataDir string) RuntimeConfig {
	c := RuntimeConfig{
		VideosDir:   os.Getenv("VELOX_VIDEOS_DIR"),
		StaticDir:   os.Getenv("VELOX_STATIC_DIR"),
		Environment: strings.TrimSpace(os.Getenv("VELOX_ENVIRONMENT")),
	}
	c.RuntimeDir = os.Getenv("VELOX_RUNTIME_DIR")
	c.DataDir = dataDir
	if c.RuntimeDir == "" {
		if c.DataDir != "" {
			c.RuntimeDir = filepath.Dir(c.DataDir)
		} else {
			c.RuntimeDir = ".velox"
		}
	}
	if c.DataDir == "" {
		c.DataDir = filepath.Join(c.RuntimeDir, "data")
	}
	c.JobQueueFile = os.Getenv("VELOX_JOB_QUEUE_FILE")
	c.SecretsDir = os.Getenv("VELOX_SECRETS_DIR")
	if c.SecretsDir == "" {
		c.SecretsDir = filepath.Join(c.RuntimeDir, "secrets")
	}
	// Staging directory for artifact uploads (before verification).
	c.StagingDir = os.Getenv("VELOX_STAGING_DIR")
	if c.StagingDir == "" {
		c.StagingDir = filepath.Join(c.DataDir, "staging")
	}
	// Final storage directory for verified artifacts.
	c.StorageDir = os.Getenv("VELOX_STORAGE_DIR")
	if c.StorageDir == "" {
		c.StorageDir = filepath.Join(c.DataDir, "storage")
	}

	// Max voiceover asset store size (bytes). Default 256 MiB.
	c.MaxVoiceoverBytes = 256 * 1024 * 1024
	if raw := strings.TrimSpace(os.Getenv("VELOX_MAX_VOICEOVER_BYTES")); raw != "" {
		if parsed, perr := strconv.ParseInt(raw, 10, 64); perr == nil && parsed > 0 {
			c.MaxVoiceoverBytes = parsed
		}
	}

	// NopBlobStore dev opt-in (production ban enforced in Validate()).
	c.AllowNopBlobStoreDev = strings.TrimSpace(os.Getenv("VELOX_ALLOW_NOP_BLOBSTORE_DEV")) == "true"

	// gRPC insecure dev opt-in.
	c.GRPCAllowInsecureDev = strings.TrimSpace(os.Getenv("VELOX_GRPC_ALLOW_INSECURE_DEV")) == "true"

	// Delivery global fallback dev opt-in.
	c.DeliveryGlobalFallback = strings.TrimSpace(os.Getenv("VELOX_DELIVERY_GLOBAL_FALLBACK")) == "true"

	// Release channel — PR-5 P0 guard. Default "dev" preserves
	// backward compatibility for installs that pre-date PR-5. The
	// fail-fast in bootstrap.go refuses to start the master with
	// VELOX_GRPC_ALLOW_INSECURE_DEV=true on a non-dev channel.
	c.ReleaseChannel = strings.TrimSpace(os.Getenv("VELOX_RELEASE_CHANNEL"))
	if c.ReleaseChannel == "" {
		c.ReleaseChannel = "dev"
	}

	return c
}
