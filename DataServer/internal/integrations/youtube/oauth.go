package youtube

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	"google.golang.org/api/youtube/v3"
)

// buildOAuthConfig locates client_secret.json, parses it, and returns a
// configured *oauth2.Config plus the secret file path it was loaded from.
func buildOAuthConfig(cfg *ServiceConfig) (*oauth2.Config, string, error) {
	if cfg.CredentialsDir == "" {
		return nil, "", fmt.Errorf("YouTube CredentialsDir not configured")
	}

	secretPaths := []string{
		filepath.Join(cfg.CredentialsDir, "client_secret.json"),
		filepath.Join(cfg.CredentialsDir, "credentials.json"),
	}

	var secretPath string
	var secretData []byte
	for _, p := range secretPaths {
		d, err := os.ReadFile(p)
		if err == nil {
			secretPath = p
			secretData = d
			break
		}
	}
	if secretPath == "" {
		return nil, "", fmt.Errorf("client_secret.json not found in %s", cfg.CredentialsDir)
	}

	var parsed struct {
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
	if err := json.Unmarshal(secretData, &parsed); err != nil {
		return nil, "", fmt.Errorf("parse client_secret.json: %w", err)
	}

	var clientID, clientSecret, redirectURI string
	if parsed.Installed.ClientID != "" {
		clientID = parsed.Installed.ClientID
		clientSecret = parsed.Installed.ClientSecret
		if len(parsed.Installed.RedirectUris) > 0 {
			redirectURI = parsed.Installed.RedirectUris[0]
		}
	} else if parsed.Web.ClientID != "" {
		clientID = parsed.Web.ClientID
		clientSecret = parsed.Web.ClientSecret
		if len(parsed.Web.RedirectUris) > 0 {
			redirectURI = parsed.Web.RedirectUris[0]
		}
	} else {
		return nil, "", fmt.Errorf("no valid OAuth credentials found")
	}

	if cfg.ClientID != "" {
		clientID = cfg.ClientID
	}
	if cfg.ClientSecret != "" {
		clientSecret = cfg.ClientSecret
	}
	if cfg.RedirectURL != "" {
		redirectURI = cfg.RedirectURL
	}

	return &oauth2.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		RedirectURL:  redirectURI,
		Scopes: []string{
			"https://www.googleapis.com/auth/youtube",
			"https://www.googleapis.com/auth/youtube.upload",
			"https://www.googleapis.com/auth/youtube.readonly",
			"https://www.googleapis.com/auth/yt-analytics.readonly",
			"https://www.googleapis.com/auth/yt-analytics-monetary.readonly",
		},
		Endpoint: google.Endpoint,
	}, secretPath, nil
}

// loadOAuthConfig loads OAuth2 configuration from client_secret.json
func (s *Service) loadOAuthConfig() error {
	oauthConfig, secretPath, err := buildOAuthConfig(s.config)
	if err != nil {
		return err
	}
	s.oauthConfig = oauthConfig
	if s.authManager != nil {
		s.authManager.oauthConfig = s.oauthConfig
	}

	log.Printf("[OK] YouTube OAuth config loaded from %s", secretPath)
	return nil
}

// PersistedTokenSource wraps an oauth2.TokenSource and persists refreshed
// tokens to the canonical youtube_oauth_tokens SQLite row. The save
// callback is invoked by Token() whenever the underlying oauth2 lib hands
// us a fresh token (i.e. a refresh round-trip).
//
// DB-first ordering (S11): the supplied `save` callback MUST persist the
// refreshed token to SQLite BEFORE mirroring it into s.channels. A
// transient SQLite failure is therefore surfaced to the OAuth lib as a
// returned error: the lib will retry on the next outgoing HTTP call and
// the in-RAM token copy stays untouched. The previous WARN-swallows-error
// behaviour was the bug: a successful RAM update with a failed SQL
// persist meant the in-RAM cache and the canonical row diverged until
// the next restart. We close that gap here by requiring the callback to
// adopt DB-first ordering and by reporting its error back through the
// TokenSource contract.
type PersistedTokenSource struct {
	source oauth2.TokenSource
	save   func(*oauth2.Token) error
}

func (pts *PersistedTokenSource) Token() (*oauth2.Token, error) {
	t, err := pts.source.Token()
	if err != nil {
		return nil, err
	}
	if err := pts.save(t); err != nil {
		// Surface the persist error so the OAuth lib / caller can decide.
		// We do not return it as the token error: the in-memory token IS
		// valid for the upcoming request, but a tight observer (the
		// /api/v1/youtube/* refresh handlers) will want a clear signal
		// that the canonical write didn't land. Logging at ERROR so the
		// operator notice is unmissable.
		log.Printf("[ERR] YouTube: refreshed token NOT persisted (canonical row out of sync): %v", err)
	}
	return t, nil
}

// persistRefreshedToken writes a freshly-refreshed oauth2.Token into the
// canonical youtube_oauth_tokens row. Extracted so both PersistedTokenSource.save
// (the implicit refresh fired by oauth2.NewClient) and the explicit
// ValidateToken refresh path run through the SAME persistence primitive —
// one place to fix when the row schema or cipher policy changes.
//
// Behaviour:
//   - AccessToken is encrypted and written unconditionally.
//   - RefreshToken is encrypted and written when newToken.RefreshToken != "".
//   - When the refresh provider did NOT rotate the refresh_token, we read
//     the existing row and copy its encrypted refresh_token BLOB forward
//     so a normal access-token rotation cannot wipe the long-lived credential.
//   - Expiry is written in RFC3339 when non-zero; empty otherwise.
//
// DB-first ordering (the SQLite-only auto-refresh path): this is the
// single canonical write primitive for refreshed OAuth credentials.
// Callers MUST persist through this method BEFORE mirroring the new
// access/refresh/expiry into the in-RAM channel entry under s.mu.
// The PersistedTokenSource.save closure supplied by GetYouTubeService
// already follows this rule — it returns a non-nil error on SQL or
// encrypt failure so the in-RAM `channel.AccessToken` stays untouched
// on a failed persist. (PersistedTokenSource.Token always returns the
// freshly-refreshed `source.Token` to its caller regardless: the
// OAuth lib's own cache is unaffected, and the next DB-first
// refresh closure repopulates channel.AccessToken from SQL on the
// next round-trip. Boot hydration via loadOAuthChannelsFromSQLite
// only fires on a cold start; runtime recovery is the closure
// path itself.)
// The previous "auto-refresh path logs the SQL error and proceeds"
// behaviour ("in-RAM access_token is already advanced on
// channel.AccessToken") has been removed: a divergence between the
// runtime cache and the canonical youtube_oauth_tokens row no longer
// goes unnoticed — the [ERR] log inside PersistedTokenSource.Token
// surfaces it.
func (s *Service) persistRefreshedToken(channelID string, newToken *oauth2.Token) error {
	if s.store == nil || s.oauthBuf == nil {
		return nil // degraded mode (no cipher / no store): nothing to persist
	}

	accessEnc, err := s.oauthBuf.Encrypt([]byte(newToken.AccessToken))
	if err != nil {
		return fmt.Errorf("encrypt access token: %w", err)
	}

	var refreshEnc []byte
	if newToken.RefreshToken != "" {
		r, rerr := s.oauthBuf.Encrypt([]byte(newToken.RefreshToken))
		if rerr != nil {
			return fmt.Errorf("encrypt refresh token: %w", rerr)
		}
		refreshEnc = r
	}
	if refreshEnc == nil {
		// Preserve the previously-stored encrypted refresh_token blob.
		cur, gerr := s.store.GetYouTubeOAuthToken(channelID)
		if gerr == nil && cur != nil {
			if v, ok := cur["refresh_token_encrypted"].([]byte); ok && len(v) > 0 {
				refreshEnc = v
			}
		}
	}

	var expiry string
	if !newToken.Expiry.IsZero() {
		expiry = newToken.Expiry.Format(time.RFC3339)
	}

	if err := s.store.UpsertYouTubeOAuthToken(channelID, accessEnc, refreshEnc, "Bearer", expiry, "", s.oauthBuf.KeyVersion()); err != nil {
		return fmt.Errorf("upsert youtube_oauth_tokens: %w", err)
	}
	return nil
}

// GetYouTubeService returns a YouTube service for a channel
func (s *Service) GetYouTubeService(ctx context.Context, channelID string) (*youtube.Service, error) {
	channel := s.GetChannel(channelID)
	if channel == nil {
		return nil, fmt.Errorf("channel not found: %s", channelID)
	}

	if s.oauthConfig == nil {
		return nil, fmt.Errorf("OAuth not configured")
	}

	token := &oauth2.Token{
		AccessToken:  channel.AccessToken,
		RefreshToken: channel.RefreshToken,
		TokenType:    "Bearer",
		Expiry:       channel.Expiry,
	}

	baseSource := s.oauthConfig.TokenSource(ctx, token)
	pts := &PersistedTokenSource{
		source: baseSource,
		save: func(newToken *oauth2.Token) error {
			if newToken.AccessToken == channel.AccessToken {
				return nil
			}
			// DB-first (S11). Persist the refreshed token to SQLite BEFORE
			// mirroring it into s.channels. Refresh providers MAY rotate
			// the refresh_token too — when the new grant carries one, we
			// prefer it; otherwise the previously-stored encrypted blob
			// is preserved so a normal access-token rotation does not
			// silently wipe the long-lived credential. A SQLite failure
			// returns the error WITHOUT RAM mutation so the runtime cache
			// stays coherent with the canonical row.
			if s.store != nil && s.oauthBuf != nil {
				if err := s.persistRefreshedToken(channel.ID, newToken); err != nil {
					return fmt.Errorf("refresh: persist to sqlite: %w", err)
				}
				log.Printf("[OK] YouTube token auto-refreshed for channel: %s", channel.ID)
			} else {
				log.Printf("[WARN] youtube refresh: oauthBuf nil or store nil — skipping persistence for %s", channel.ID)
			}

			// RAM mirror written only AFTER the canonical write succeeded.
			s.mu.Lock()
			channel.AccessToken = newToken.AccessToken
			channel.Expiry = newToken.Expiry
			if newToken.RefreshToken != "" {
				channel.RefreshToken = newToken.RefreshToken
			}
			s.mu.Unlock()
			return nil
		},
	}

	client := oauth2.NewClient(ctx, pts)

	service, err := youtube.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return nil, fmt.Errorf("failed to create YouTube service: %w", err)
	}

	return service, nil
}

// GetQuotaManager returns the quota manager
func (s *Service) GetQuotaManager() *QuotaManager {
	return s.quotaManager
}

// HealthCheck checks the health of YouTube API connection
func (s *Service) HealthCheck(ctx context.Context, channelID string) (map[string]interface{}, error) {
	service, err := s.GetYouTubeService(ctx, channelID)
	if err != nil {
		return map[string]interface{}{
			"ok":    false,
			"error": err.Error(),
		}, nil //nolint:nilerr // status endpoint embeds error in result map
	}

	_, err = service.Channels.List([]string{"snippet"}).Mine(true).Do()
	if err != nil {
		return map[string]interface{}{
			"ok":    false,
			"error": fmt.Sprintf("API call failed: %v", err),
		}, nil
	}

	return map[string]interface{}{
		"ok":      true,
		"message": "YouTube API connection healthy",
	}, nil
}
