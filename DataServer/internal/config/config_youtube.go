package config

import (
	"os"
	"path/filepath"
)

func loadYouTubeConfig(secretsDir, dataDir string) YouTubeConfig {
	c := YouTubeConfig{
		APIKey:      os.Getenv("VELOX_YOUTUBE_API_KEY"),
		TokensDir:   os.Getenv("VELOX_YOUTUBE_TOKENS_DIR"),
		PostingPath: os.Getenv("VELOX_YOUTUBE_POSTING_PATH"),
	}
	c.CredentialsDir = os.Getenv("VELOX_YOUTUBE_CREDENTIALS_DIR")
	if c.TokensDir == "" {
		c.TokensDir = firstExistingDir([]string{
			filepath.Join(secretsDir, "youtube", "tokens"),
			filepath.Join(dataDir, "youtube", "tokens"),
		})
		if c.TokensDir == "" {
			c.TokensDir = filepath.Join(secretsDir, "youtube", "tokens")
		}
	}
	if c.CredentialsDir == "" {
		c.CredentialsDir = firstExistingDir([]string{
			filepath.Join(secretsDir, "youtube", "credentials"),
			filepath.Join(dataDir, "youtube", "credentials"),
		})
		if c.CredentialsDir == "" {
			c.CredentialsDir = filepath.Join(secretsDir, "youtube", "credentials")
		}
	}
	return c
}
