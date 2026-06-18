package config

import (
	"os"
	"path/filepath"
)

func loadRuntimeConfig(dataDir string) RuntimeConfig {
	c := RuntimeConfig{
		VideosDir: os.Getenv("VELOX_VIDEOS_DIR"),
		StaticDir: os.Getenv("VELOX_STATIC_DIR"),
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
	return c
}
