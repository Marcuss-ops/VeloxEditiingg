// Package youtube provides YouTube API integration for the Velox server.
// This file contains the AuthManager type, constructor, and OAuth configuration.
package youtube

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

type oauthCredentials struct {
	ClientID     string
	ClientSecret string
	RedirectURI  string
}

func parseOAuthCredentialsFile(data []byte) (*oauthCredentials, error) {
	var clientSecret struct {
		Installed struct {
			ClientID     string   `json:"client_id"`
			ClientSecret string   `json:"client_secret"`
			RedirectUris []string `json:"redirect_uris"`
		} `json:"installed"`
		Web struct {
			ClientID     string   `json:"client_id"`
			ClientSecret string   `json:"client_secret"`
			RedirectUris []string `json:"redirect_uris"`
		} `json:"web"`
	}

	if err := json.Unmarshal(data, &clientSecret); err != nil {
		return nil, fmt.Errorf("parse client_secret.json: %w", err)
	}

	creds := &oauthCredentials{
		RedirectURI: "http://localhost:8080/oauth2callback",
	}

	if clientSecret.Installed.ClientID != "" {
		creds.ClientID = clientSecret.Installed.ClientID
		creds.ClientSecret = clientSecret.Installed.ClientSecret
		if len(clientSecret.Installed.RedirectUris) > 0 {
			creds.RedirectURI = clientSecret.Installed.RedirectUris[0]
		}
	} else if clientSecret.Web.ClientID != "" {
		creds.ClientID = clientSecret.Web.ClientID
		creds.ClientSecret = clientSecret.Web.ClientSecret
		if len(clientSecret.Web.RedirectUris) > 0 {
			creds.RedirectURI = clientSecret.Web.RedirectUris[0]
		}
	} else {
		return nil, fmt.Errorf("no valid OAuth credentials found")
	}

	return creds, nil
}

func findOAuthSecretFile(cfg *ServiceConfig) (string, []byte, error) {
	secretPaths := []string{
		filepath.Join(cfg.YoutubePostingPath, "Modules", "client_secret.json"),
		filepath.Join(cfg.YoutubePostingPath, "client_secret.json"),
		filepath.Join(cfg.TokensDir, "client_secret.json"),
	}
	if cfg.CredentialsDir != "" {
		secretPaths = append([]string{
			filepath.Join(cfg.CredentialsDir, "client_secret.json"),
			filepath.Join(cfg.CredentialsDir, "credentials.json"),
		}, secretPaths...)
	}
	if cfg.DataDir != "" {
		secretPaths = append(secretPaths,
			filepath.Join(cfg.DataDir, "secrets", "youtube", "credentials", "client_secret.json"),
			filepath.Join(cfg.DataDir, "youtube", "credentials", "client_secret.json"),
			filepath.Join(cfg.DataDir, "youtube", "Credentials", "client_secret.json"),
			filepath.Join(cfg.DataDir, "youtube", "Credentials", "credentials.json"),
		)
	}

	for _, path := range secretPaths {
		data, err := os.ReadFile(path)
		if err == nil {
			return path, data, nil
		}
	}
	return "", nil, fmt.Errorf("client_secret.json not found in any known location")
}

// AuthManager handles OAuth authentication and token management for YouTube channels
type AuthManager struct {
	service     *Service
	oauthConfig *oauth2.Config
	tokenCache  map[string]*oauth2.Token
}

// NewAuthManager creates a new AuthManager
func NewAuthManager(s *Service) *AuthManager {
	return &AuthManager{
		service:    s,
		tokenCache: make(map[string]*oauth2.Token),
	}
}

// LoadOAuthConfig loads OAuth2 configuration from client_secret.json
func (am *AuthManager) LoadOAuthConfig() error {
	cfg := am.service.config

	secretPath, secretData, err := findOAuthSecretFile(cfg)
	if err != nil {
		return err
	}

	creds, err := parseOAuthCredentialsFile(secretData)
	if err != nil {
		return fmt.Errorf("load OAuth config: %w", err)
	}

	if cfg.ClientID != "" {
		creds.ClientID = cfg.ClientID
	}
	if cfg.ClientSecret != "" {
		creds.ClientSecret = cfg.ClientSecret
	}
	if cfg.RedirectURL != "" {
		creds.RedirectURI = cfg.RedirectURL
	}

	am.oauthConfig = &oauth2.Config{
		ClientID:     creds.ClientID,
		ClientSecret: creds.ClientSecret,
		RedirectURL:  creds.RedirectURI,
		Scopes: []string{
			"https://www.googleapis.com/auth/youtube",
			"https://www.googleapis.com/auth/youtube.upload",
			"https://www.googleapis.com/auth/youtube.readonly",
			"https://www.googleapis.com/auth/yt-analytics.readonly",
			"https://www.googleapis.com/auth/yt-analytics-monetary.readonly",
		},
		Endpoint: google.Endpoint,
	}

	log.Printf("[OK] YouTube OAuth config loaded from %s", secretPath)
	return nil
}

// GetOAuthStartURL returns the URL to start OAuth flow
func (am *AuthManager) GetOAuthStartURL(channelName string) string {
	if am.oauthConfig == nil {
		return ""
	}

	state := fmt.Sprintf("youtube_%s_%d", channelName, time.Now().Unix())
	return am.oauthConfig.AuthCodeURL(
		state,
		oauth2.AccessTypeOffline,
		oauth2.SetAuthURLParam("prompt", "consent select_account"),
	)
}

// GetOAuthConfig returns the OAuth configuration
func (am *AuthManager) GetOAuthConfig() *oauth2.Config {
	return am.oauthConfig
}
