package config

import (
	"path/filepath"
)

// ── StorageConfig (S3/MinIO/R2) ────────────────────────────────────────

func loadStorageConfig(raw RawConfig) StorageConfig {
	c := StorageConfig{
		Region: "us-east-1",
	}
	c.Endpoint = raw.Get("VELOX_S3_ENDPOINT")
	if r := raw.Get("VELOX_S3_REGION"); r != "" {
		c.Region = r
	}
	c.Bucket = raw.Get("VELOX_S3_BUCKET")
	c.AccessKeyID = raw.Get("VELOX_S3_ACCESS_KEY_ID")
	c.SecretKey = raw.Get("VELOX_S3_SECRET_ACCESS_KEY")
	c.UseSSL = raw.Bool("VELOX_S3_USE_SSL", false)
	return c
}

// ── DriveConfig ────────────────────────────────────────────────────────

func loadDriveConfig(secretsDir, dataDir string, raw RawConfig) DriveConfig {
	c := DriveConfig{
		ClientID:     raw.Get("VELOX_DRIVE_CLIENT_ID"),
		ClientSecret: raw.Get("VELOX_DRIVE_CLIENT_SECRET"),
		RedirectURI:  raw.Get("VELOX_DRIVE_REDIRECT_URI"),
		TokensDir:    raw.Get("VELOX_DRIVE_TOKENS_DIR"),
	}
	c.CredentialsDir = raw.Get("VELOX_DRIVE_CREDENTIALS_DIR")
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

func loadAnsibleConfig(dataDir string, raw RawConfig) AnsibleConfig {
	c := AnsibleConfig{
		PlaybookDir: raw.Get("VELOX_ANSIBLE_PLAYBOOK_DIR"),
	}
	if c.PlaybookDir == "" {
		c.PlaybookDir = filepath.Join(dataDir, "ansible", "playbooks")
	}
	return c
}

// ── RenderConfig ───────────────────────────────────────────────────────

func loadRenderConfig(raw RawConfig) RenderConfig {
	c := RenderConfig{
		RemoteEngineURL:   raw.Get("VELOX_REMOTE_ENGINE_URL"),
		RemoteEngineToken: raw.Get("VELOX_REMOTE_ENGINE_TOKEN"),
	}
	c.RemoteEngineTimeoutMS = raw.Int("VELOX_REMOTE_ENGINE_TIMEOUT_MS", 60000, 1)
	c.RemoteEngineRetries = raw.Int("VELOX_REMOTE_ENGINE_RETRIES", 3, 1)
	c.RemoteEnginePollInterval = raw.Int("VELOX_REMOTE_ENGINE_POLL_INTERVAL", 30, 5)
	return c
}
