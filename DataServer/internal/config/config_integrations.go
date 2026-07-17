package config

import (
	"os"
	"path/filepath"
)

// ── StorageConfig (S3/MinIO/R2) ────────────────────────────────────────

func loadStorageConfig() StorageConfig {
	c := StorageConfig{
		Region: "us-east-1",
	}
	c.Endpoint = os.Getenv("VELOX_S3_ENDPOINT")
	if r := os.Getenv("VELOX_S3_REGION"); r != "" {
		c.Region = r
	}
	c.Bucket = os.Getenv("VELOX_S3_BUCKET")
	c.AccessKeyID = os.Getenv("VELOX_S3_ACCESS_KEY_ID")
	c.SecretKey = os.Getenv("VELOX_S3_SECRET_ACCESS_KEY")
	c.UseSSL = boolFromEnv("VELOX_S3_USE_SSL", false)
	return c
}

// ── DriveConfig ────────────────────────────────────────────────────────

func loadDriveConfig(secretsDir, dataDir string) DriveConfig {
	c := DriveConfig{
		ClientID:     os.Getenv("VELOX_DRIVE_CLIENT_ID"),
		ClientSecret: os.Getenv("VELOX_DRIVE_CLIENT_SECRET"),
		RedirectURI:  os.Getenv("VELOX_DRIVE_REDIRECT_URI"),
		TokensDir:    os.Getenv("VELOX_DRIVE_TOKENS_DIR"),
	}
	c.CredentialsDir = os.Getenv("VELOX_DRIVE_CREDENTIALS_DIR")
	if c.TokensDir == "" {
		c.TokensDir = firstExistingDir([]string{
			filepath.Join(secretsDir, "drive", "tokens"),
			filepath.Join(dataDir, "drive", "tokens"),
		})
		if c.TokensDir == "" {
			c.TokensDir = filepath.Join(secretsDir, "drive", "tokens")
		}
	}
	if c.CredentialsDir == "" {
		c.CredentialsDir = firstExistingDir([]string{
			filepath.Join(secretsDir, "drive", "credentials"),
			filepath.Join(dataDir, "drive", "credentials"),
		})
		if c.CredentialsDir == "" {
			c.CredentialsDir = filepath.Join(secretsDir, "drive", "credentials")
		}
	}
	return c
}

// ── AnsibleConfig ──────────────────────────────────────────────────────

func loadAnsibleConfig(dataDir string) AnsibleConfig {
	c := AnsibleConfig{
		PlaybookDir: os.Getenv("VELOX_ANSIBLE_PLAYBOOK_DIR"),
	}
	if c.PlaybookDir == "" {
		c.PlaybookDir = filepath.Join(dataDir, "ansible", "playbooks")
	}
	return c
}

// ── RenderConfig ───────────────────────────────────────────────────────

func loadRenderConfig() RenderConfig {
	c := RenderConfig{
		RemoteEngineURL:   os.Getenv("VELOX_REMOTE_ENGINE_URL"),
		RemoteEngineToken: os.Getenv("VELOX_REMOTE_ENGINE_TOKEN"),
	}
	c.RemoteEngineTimeoutMS = intFromEnv("VELOX_REMOTE_ENGINE_TIMEOUT_MS", 60000, 1)
	c.RemoteEngineRetries = intFromEnv("VELOX_REMOTE_ENGINE_RETRIES", 3, 1)
	c.RemoteEnginePollInterval = intFromEnv("VELOX_REMOTE_ENGINE_POLL_INTERVAL", 30, 5)
	return c
}
