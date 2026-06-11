// Package drive provides Google Drive integration for Velox server.
// It handles OAuth2 authentication, file upload/download, and folder management.
package drive

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// OAuth2Config holds the OAuth2 configuration for Google Drive
type OAuth2Config struct {
	ClientID     string   `json:"client_id"`
	ClientSecret string   `json:"client_secret"`
	RedirectURI  string   `json:"redirect_uri"`
	Scopes       []string `json:"scopes"`
}

// Token represents an OAuth2 token with metadata
type Token struct {
	AccessToken  string    `json:"token"`
	RefreshToken string    `json:"refresh_token"`
	TokenType    string    `json:"token_type,omitempty"`
	Expiry       time.Time `json:"expiry,omitempty"`
	Scope        string    `json:"scope,omitempty"`
	AccountEmail string    `json:"account_email,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
}

// UnmarshalJSON implements custom unmarshaling for Token to handle different formats
func (t *Token) UnmarshalJSON(data []byte) error {
	type Alias Token
	aux := &struct {
		Expiry interface{} `json:"expiry"`
		Scope  interface{} `json:"scope"`
		Scopes interface{} `json:"scopes"`
		*Alias
	}{
		Alias: (*Alias)(t),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	// Handle Expiry
	if aux.Expiry != nil {
		if s, ok := aux.Expiry.(string); ok {
			origS := s
			if !strings.Contains(s, "Z") && !strings.Contains(s, "+") {
				s += "Z"
			}
			// Try RFC3339
			if parsed, err := time.Parse(time.RFC3339, s); err == nil {
				t.Expiry = parsed
			} else if parsed, err := time.Parse("2006-01-02T15:04:05Z", s); err == nil {
				// Try without fractional seconds if it had Z added
				t.Expiry = parsed
			} else if parsed, err := time.Parse("2006-01-02 15:04:05", origS); err == nil {
				// Try space separated
				t.Expiry = parsed
			}
		}
	}

	// Handle Scope (string or list)
	parseScope := func(v interface{}) string {
		if s, ok := v.(string); ok {
			return s
		}
		if ss, ok := v.([]interface{}); ok {
			var scopes []string
			for _, s := range ss {
				if str, ok := s.(string); ok {
					scopes = append(scopes, str)
				}
			}
			return strings.Join(scopes, " ")
		}
		return ""
	}

	if aux.Scope != nil {
		t.Scope = parseScope(aux.Scope)
	}
	if t.Scope == "" && aux.Scopes != nil {
		t.Scope = parseScope(aux.Scopes)
	}

	return nil
}

// TokenManager handles token storage and retrieval
type TokenManager struct {
	tokensDir string
	mu        sync.RWMutex
}

// NewTokenManager creates a new token manager
func NewTokenManager(tokensDir string) (*TokenManager, error) {
	if err := os.MkdirAll(tokensDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create tokens directory: %w", err)
	}
	return &TokenManager{tokensDir: tokensDir}, nil
}

// SaveToken saves a token to a file
func (tm *TokenManager) SaveToken(name string, token *Token) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	token.CreatedAt = time.Now()
	tokenPath := filepath.Join(tm.tokensDir, name+".json")
	data, err := json.MarshalIndent(token, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal token: %w", err)
	}

	if err := os.WriteFile(tokenPath, data, 0600); err != nil {
		return fmt.Errorf("failed to write token file: %w", err)
	}

	log.Printf("[AUTH] Token saved: %s", tokenPath)
	return nil
}

// LoadToken loads a token from a file
func (tm *TokenManager) LoadToken(name string) (*Token, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	tokenPath := filepath.Join(tm.tokensDir, name+".json")
	data, err := os.ReadFile(tokenPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read token file: %w", err)
	}

	var token Token
	if err := json.Unmarshal(data, &token); err != nil {
		return nil, fmt.Errorf("failed to unmarshal token: %w", err)
	}

	return &token, nil
}

// ListTokens lists all saved tokens
func (tm *TokenManager) ListTokens() ([]string, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	entries, err := os.ReadDir(tm.tokensDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read tokens directory: %w", err)
	}

	var tokens []string
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".json" {
			tokens = append(tokens, entry.Name()[:len(entry.Name())-5])
		}
	}
	return tokens, nil
}

// DeleteToken deletes a token file
func (tm *TokenManager) DeleteToken(name string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tokenPath := filepath.Join(tm.tokensDir, name+".json")
	if err := os.Remove(tokenPath); err != nil {
		return fmt.Errorf("failed to delete token: %w", err)
	}
	return nil
}

// GetAuthURL generates the OAuth2 authorization URL
func GetAuthURL(cfg *OAuth2Config, state string) string {
	scope := ""
	for i, s := range cfg.Scopes {
		if i > 0 {
			scope += " "
		}
		scope += s
	}

	return fmt.Sprintf(
		"https://accounts.google.com/o/oauth2/v2/auth?client_id=%s&redirect_uri=%s&response_type=code&scope=%s&access_type=offline&prompt=consent%%20select_account&state=%s",
		cfg.ClientID,
		cfg.RedirectURI,
		scope,
		state,
	)
}

// ExchangeCode exchanges an authorization code for tokens
func ExchangeCode(ctx context.Context, cfg *OAuth2Config, code string) (*Token, error) {
	// Use HTTP client to exchange code for tokens
	url := "https://oauth2.googleapis.com/token"

	data := map[string]string{
		"client_id":     cfg.ClientID,
		"client_secret": cfg.ClientSecret,
		"code":          code,
		"grant_type":    "authorization_code",
		"redirect_uri":  cfg.RedirectURI,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to exchange code: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&errResp)
		return nil, fmt.Errorf("token exchange failed: %v", errResp)
	}

	var tokenResp struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int    `json:"expires_in"`
		TokenType    string `json:"token_type"`
		Scope        string `json:"scope"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return nil, fmt.Errorf("failed to decode token response: %w", err)
	}

	return &Token{
		AccessToken:  tokenResp.AccessToken,
		RefreshToken: tokenResp.RefreshToken,
		TokenType:    tokenResp.TokenType,
		Expiry:       time.Now().Add(time.Duration(tokenResp.ExpiresIn) * time.Second),
		Scope:        tokenResp.Scope,
	}, nil
}

// RefreshToken refreshes an access token using a refresh token
func RefreshToken(ctx context.Context, cfg *OAuth2Config, refreshToken string) (*Token, error) {
	url := "https://oauth2.googleapis.com/token"

	data := map[string]string{
		"client_id":     cfg.ClientID,
		"client_secret": cfg.ClientSecret,
		"refresh_token": refreshToken,
		"grant_type":    "refresh_token",
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to refresh token: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&errResp)
		return nil, fmt.Errorf("token refresh failed: %v", errResp)
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
		TokenType   string `json:"token_type"`
		Scope       string `json:"scope"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return nil, fmt.Errorf("failed to decode token response: %w", err)
	}

	return &Token{
		AccessToken:  tokenResp.AccessToken,
		RefreshToken: refreshToken, // Refresh token stays the same
		TokenType:    tokenResp.TokenType,
		Expiry:       time.Now().Add(time.Duration(tokenResp.ExpiresIn) * time.Second),
		Scope:        tokenResp.Scope,
	}, nil
}

// DefaultScopes returns the default OAuth2 scopes for Drive access
func DefaultScopes() []string {
	return []string{
		"https://www.googleapis.com/auth/drive.file",
		"https://www.googleapis.com/auth/drive",
	}
}
