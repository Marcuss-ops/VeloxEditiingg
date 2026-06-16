package config

import (
	"os"
	"path/filepath"
)

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
