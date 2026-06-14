// Package drive provides Google Drive API service operations.
package drive

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)


// NewService creates a new Drive service
func NewService(cfg *ServiceConfig) (*Service, error) {
	if cfg.TokensDir == "" {
		cfg.TokensDir = "data/drive_tokens"
	}

	tokenManager, err := NewTokenManager(cfg.TokensDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create token manager: %w", err)
	}

	scopes := DefaultScopes()
	if len(cfg.RedirectURI) == 0 {
		cfg.RedirectURI = "https://veloxmanager.duckdns.org/api/drive/oauth/callback"
	}

	return &Service{
		oauthCfg: &OAuth2Config{
			ClientID:     cfg.ClientID,
			ClientSecret: cfg.ClientSecret,
			RedirectURI:  cfg.RedirectURI,
			Scopes:       scopes,
		},
		tokenManager: tokenManager,
		httpClient:   &http.Client{Timeout: 120 * time.Second},
	}, nil
}

// SetToken sets the current access token for API calls
func (s *Service) SetToken(token *Token) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentToken = token
}

// getToken returns the current token, refreshing if necessary
func (s *Service) getToken(ctx context.Context) (*Token, error) {
	s.mu.RLock()
	token := s.currentToken
	s.mu.RUnlock()

	if token == nil {
		return nil, fmt.Errorf("no token set - authenticate first")
	}

	// Check if token needs refresh (5 minutes before expiry)
	if time.Until(token.Expiry) < 5*time.Minute {
		log.Printf("[AUTH] Token expired or expiring soon, refreshing...")
		newToken, err := RefreshToken(ctx, s.oauthCfg, token.RefreshToken)
		if err != nil {
			log.Printf("[AUTH] Token refresh failed: %v", err)
			return nil, fmt.Errorf("failed to refresh token: %w", err)
		}
		newToken.AccountEmail = token.AccountEmail
		s.SetToken(newToken)
		log.Printf("[AUTH] Token refreshed successfully, expires: %v", newToken.Expiry)
		return newToken, nil
	}

	return token, nil
}

// doAPIRequest performs an authenticated API request
func (s *Service) doAPIRequest(ctx context.Context, method, endpoint string, body io.Reader, result interface{}) error {
	token, err := s.getToken(ctx)
	if err != nil {
		return err
	}

	baseURL := "https://www.googleapis.com/drive/v3"
	if strings.HasPrefix(endpoint, "http") {
		baseURL = ""
	}

	req, err := http.NewRequestWithContext(ctx, method, baseURL+endpoint, body)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token.AccessToken))
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		var errResp map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&errResp)
		return fmt.Errorf("API error (%d): %v", resp.StatusCode, errResp)
	}

	if result != nil {
		if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
			return fmt.Errorf("failed to decode response: %w", err)
		}
	}

	return nil
}

// GetAbout gets information about the authenticated user
func (s *Service) GetAbout(ctx context.Context) (map[string]interface{}, error) {
	var result map[string]interface{}
	err := s.doAPIRequest(ctx, "GET", "/about?fields=user(displayName,emailAddress),storageQuota", nil, &result)
	return result, err
}

// ListFiles lists files in a folder
func (s *Service) ListFiles(ctx context.Context, folderID string, pageSize int) ([]File, error) {
	if pageSize == 0 {
		pageSize = 100
	}
	folderID = strings.TrimSpace(folderID)
	if folderID == "" || folderID == "." {
		folderID = "root"
	}

	query := url.QueryEscape(fmt.Sprintf("'%s' in parents and trashed=false", folderID))
	endpoint := fmt.Sprintf("/files?q=%s&pageSize=%d&fields=files(id,name,mimeType,iconLink,webViewLink,size,createdTime,modifiedTime)&orderBy=folder,name", query, pageSize)

	var result struct {
		Files []File `json:"files"`
	}

	if err := s.doAPIRequest(ctx, "GET", endpoint, nil, &result); err != nil {
		return nil, err
	}

	return result.Files, nil
}

// GetFolder finds a folder by name within a parent folder
func (s *Service) GetFolder(ctx context.Context, name string, parentID string) (*Folder, error) {
	if parentID == "" {
		parentID = "root"
	}

	safeName := strings.ReplaceAll(name, "'", "\\'")
	query := url.QueryEscape(fmt.Sprintf("mimeType='application/vnd.google-apps.folder' and name='%s' and '%s' in parents and trashed=false", safeName, parentID))
	endpoint := fmt.Sprintf("/files?q=%s&fields=files(id,name)&pageSize=1", query)

	var result struct {
		Files []File `json:"files"`
	}

	if err := s.doAPIRequest(ctx, "GET", endpoint, nil, &result); err != nil {
		return nil, err
	}

	if len(result.Files) == 0 {
		return nil, nil
	}

	return &Folder{
		ID:   result.Files[0].ID,
		Name: result.Files[0].Name,
	}, nil
}

// CreateFolder creates a new folder
func (s *Service) CreateFolder(ctx context.Context, name string, parentID string) (*Folder, error) {
	if parentID == "" {
		parentID = "root"
	}

	log.Printf("[DRIVE] Creating folder '%s' in parent '%s'", name, parentID)

	folderMeta := map[string]interface{}{
		"name":     name,
		"mimeType": "application/vnd.google-apps.folder",
		"parents":  []string{parentID},
	}

	body, err := json.Marshal(folderMeta)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal folder metadata: %w", err)
	}

	var result File
	if err := s.doAPIRequest(ctx, "POST", "/files?fields=id,name", bytes.NewReader(body), &result); err != nil {
		log.Printf("[DRIVE] Failed to create folder: %v", err)
		return nil, err
	}

	log.Printf("[DRIVE] Created folder '%s' (ID: %s)", name, result.ID)

	// Verify folder exists by listing it
	verifyCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	verifyQuery := url.QueryEscape(fmt.Sprintf("'%s' in parents and trashed=false", result.ID))
	verifyEndpoint := fmt.Sprintf("/files?q=%s&fields=files(id,name)&pageSize=1", verifyQuery)
	var verifyResult struct {
		Files []File `json:"files"`
	}
	if err := s.doAPIRequest(verifyCtx, "GET", verifyEndpoint, nil, &verifyResult); err != nil {
		log.Printf("[WARN] Warning: could not verify folder: %v", err)
	} else if len(verifyResult.Files) == 0 {
		log.Printf("[WARN] Warning: folder verification returned empty (folder may not be accessible)")
	} else {
		log.Printf("[DRIVE] Folder verified: %s", verifyResult.Files[0].ID)
	}

	return &Folder{
		ID:   result.ID,
		Name: result.Name,
	}, nil
}

// GetOrCreateFolder gets an existing folder or creates it if it doesn't exist
func (s *Service) GetOrCreateFolder(ctx context.Context, name string, parentID string) (*Folder, error) {
	folder, err := s.GetFolder(ctx, name, parentID)
	if err != nil {
		return nil, err
	}

	if folder != nil {
		return folder, nil
	}

	return s.CreateFolder(ctx, name, parentID)
}


// ShareFile shares a file with specific permissions
func (s *Service) ShareFile(ctx context.Context, fileID string, email string, role string) error {
	permission := map[string]interface{}{
		"type":         "user",
		"role":         role, // "reader", "writer", "owner"
		"emailAddress": email,
	}

	body, err := json.Marshal(permission)
	if err != nil {
		return fmt.Errorf("failed to marshal permission: %w", err)
	}

	endpoint := fmt.Sprintf("/files/%s/permissions", fileID)
	return s.doAPIRequest(ctx, "POST", endpoint, bytes.NewReader(body), nil)
}

// GetFileLink gets the shareable link for a file
func (s *Service) GetFileLink(ctx context.Context, fileID string) (string, error) {
	endpoint := fmt.Sprintf("/files/%s?fields=webViewLink", fileID)

	var result File
	if err := s.doAPIRequest(ctx, "GET", endpoint, nil, &result); err != nil {
		return "", err
	}

	return result.WebViewLink, nil
}

// DeleteFile moves a file to trash
func (s *Service) DeleteFile(ctx context.Context, fileID string) error {
	endpoint := fmt.Sprintf("/files/%s", fileID)
	return s.doAPIRequest(ctx, "DELETE", endpoint, nil, nil)
}

// GetTokenManager returns the token manager for authentication operations
func (s *Service) GetTokenManager() *TokenManager {
	return s.tokenManager
}

// GetOAuthConfig returns the OAuth2 configuration
func (s *Service) GetOAuthConfig() *OAuth2Config {
	return s.oauthCfg
}
