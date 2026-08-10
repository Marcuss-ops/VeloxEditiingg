package config

import (
	"path/filepath"
	"strconv"
	"strings"
)

func loadRuntimeConfig(dataDir string, raw RawConfig) RuntimeConfig {
	c := RuntimeConfig{
		VideosDir:   raw.Get("VELOX_VIDEOS_DIR"),
		StaticDir:   raw.Get("VELOX_STATIC_DIR"),
		Environment: strings.TrimSpace(raw.Get("VELOX_ENVIRONMENT")),
	}
	c.RuntimeDir = raw.Get("VELOX_RUNTIME_DIR")
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
	c.JobQueueFile = raw.Get("VELOX_JOB_QUEUE_FILE")
	c.SecretsDir = raw.Get("VELOX_SECRETS_DIR")
	if c.SecretsDir == "" {
		c.SecretsDir = filepath.Join(c.RuntimeDir, "secrets")
	}
	// Staging directory for artifact uploads (before verification).
	c.StagingDir = raw.Get("VELOX_STAGING_DIR")
	if c.StagingDir == "" {
		c.StagingDir = filepath.Join(c.DataDir, "staging")
	}
	// Final storage directory for verified artifacts.
	c.StorageDir = raw.Get("VELOX_STORAGE_DIR")
	if c.StorageDir == "" {
		c.StorageDir = filepath.Join(c.DataDir, "storage")
	}

	// Max voiceover asset store size (bytes). Default 256 MiB.
	c.MaxVoiceoverBytes = 256 * 1024 * 1024
	if value := strings.TrimSpace(raw.Get("VELOX_MAX_VOICEOVER_BYTES")); value != "" {
		if parsed, perr := strconv.ParseInt(value, 10, 64); perr == nil && parsed > 0 {
			c.MaxVoiceoverBytes = parsed
		}
	}

	// NopBlobStore dev opt-in (production ban enforced in Validate()).
	c.AllowNopBlobStoreDev = strings.TrimSpace(raw.Get("VELOX_ALLOW_NOP_BLOBSTORE_DEV")) == "true"
	c.AllowLoopbackAdminAuthDev = strings.TrimSpace(raw.Get("VELOX_ALLOW_LOOPBACK_ADMIN_AUTH_DEV")) == "true"

	// gRPC insecure dev opt-in.
	c.GRPCAllowInsecureDev = strings.TrimSpace(raw.Get("VELOX_GRPC_ALLOW_INSECURE_DEV")) == "true"

	// Release channel — PR-5 P0 guard. Default "dev" preserves
	// backward compatibility for installs that pre-date PR-5. The
	// fail-fast in bootstrap.go refuses to start the master with
	// VELOX_GRPC_ALLOW_INSECURE_DEV=true on a non-dev channel.
	c.ReleaseChannel = strings.TrimSpace(raw.Get("VELOX_RELEASE_CHANNEL"))
	if c.ReleaseChannel == "" {
		c.ReleaseChannel = "dev"
	}

	// Commit HMAC key (P0 #6, Blocco 2). Hex-encoded raw bytes; the
	// config layer keeps it as the on-wire string so operators can
	// inject via VELOX_COMMIT_HMAC_KEY=<hex>. The Coordinator
	// validates length on NewCoordinator() and boot fails-fast.
	c.CommitHMACKey = strings.TrimSpace(raw.Get("VELOX_COMMIT_HMAC_KEY"))
	c.DeliveryDisabled = raw.Bool("VELOX_DELIVERY_DISABLED", false)
	c.DeliveryConcurrency = raw.Int("VELOX_DELIVERY_CONCURRENCY", 2, 1)

	return c
}
