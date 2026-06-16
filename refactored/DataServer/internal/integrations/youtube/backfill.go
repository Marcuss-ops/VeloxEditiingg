package youtube

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// BackfillOAuthTokensFromJSON scans <TokensDir>/account_*.json files
// (the legacy JSON write path closed by Fix B / Fix B-refresh) and
// migrates each into youtube_oauth_tokens encrypted with the configured
// AES-GCM cipher.
//
// Run once at server startup, AFTER SetOAuthSecretCipher is wired so the
// cipher is mounted. Skipped silently with a WARN log when (store == nil)
// or (oauthBuf == nil) - degraded mode has no place to encrypt.
//
// Idempotency: rows already present in SQLite are PRESERVED. From Fix B
// onward HandleOAuthCallback writes the canonical row directly to SQLite
// and a re-run of this backfill must not clobber a freshly-written
// canonical row with a stale JSON snapshot from a much older release.
// Channels minted before Fix B get imported exactly once (the first
// install to run this code with a cipher mounted).
//
// Behaviour:
//   - Missing TokensDir or unreadable -> returns (0, nil)
//   - Per-file parse error, missing access_token / channel_id, broken expiry -> log WARN, continue with next file
//   - Per-file encrypt/SQL error -> log WARN, continue with next file
//
// Returns the count of rows actually upserted.
func (s *Service) BackfillOAuthTokensFromJSON(ctx context.Context) (int, error) {
	if s.store == nil {
		log.Printf("[YT] backfill skipped: no store handle")
		return 0, nil
	}
	if s.oauthBuf == nil {
		log.Printf("[YT] backfill skipped: no encryption cipher mounted (set VELOX_YT_OAUTH_TOKEN_KEY)")
		return 0, nil
	}
	tokensDir := s.config.TokensDir
	if tokensDir == "" {
		return 0, nil
	}
	entries, err := os.ReadDir(tokensDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("backfill: read tokens dir %q: %w", tokensDir, err)
	}

	imported := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".json") || !strings.HasPrefix(name, "account_") {
			continue
		}
		if strings.Contains(name, "drive") || strings.Contains(name, "_token") {
			continue
		}
		tokenPath := filepath.Join(tokensDir, name)
		ok, berr := s.backfillOneFile(tokenPath, name)
		if berr != nil {
			log.Printf("[WARN] backfill: %s: %v", tokenPath, berr)
			continue
		}
		if ok {
			imported++
		}
	}

	log.Printf("[YT] backfill: imported %d oauth token row(s) from %s", imported, tokensDir)
	return imported, nil
}

// backfillOneFile processes a single account_<channelID>.json file.
// Returns (true, nil) on successful upsert, (false, nil) for a benign
// skip (already in SQLite), or (false, err) for an unrecoverable error.
func (s *Service) backfillOneFile(tokenPath, name string) (bool, error) {
	data, err := os.ReadFile(tokenPath)
	if err != nil {
		return false, fmt.Errorf("read: %w", err)
	}

	var secrets struct {
		Token        string `json:"token"`
		RefreshToken string `json:"refresh_token"`
		TokenURI     string `json:"token_uri"`
		ClientID     string `json:"client_id"`
		ClientSecret string `json:"client_secret"`
		Scopes       string `json:"scopes"`
		Expiry       string `json:"expiry"`
		ChannelID    string `json:"channel_id"`
		AccessToken  string `json:"access_token"`
	}
	if err := json.Unmarshal(data, &secrets); err != nil {
		return false, fmt.Errorf("parse: %w", err)
	}

	channelID := secrets.ChannelID
	if channelID == "" {
		channelID = strings.TrimSuffix(strings.TrimPrefix(name, "account_"), ".json")
	}
	if channelID == "" {
		return false, fmt.Errorf("no channel_id in JSON or filename")
	}

	accessToken := secrets.Token
	if accessToken == "" {
		accessToken = secrets.AccessToken
	}
	if accessToken == "" {
		return false, fmt.Errorf("no access_token")
	}

	// Idempotency guard: do NOT overwrite an existing canonical row.
	existing, err := s.store.GetYouTubeOAuthToken(channelID)
	if err != nil {
		return false, fmt.Errorf("lookup existing: %w", err)
	}
	if existing != nil {
		log.Printf("[YT] backfill: %s already in youtube_oauth_tokens, skipping", channelID)
		return false, nil
	}

	accessEnc, err := s.oauthBuf.Encrypt([]byte(accessToken))
	if err != nil {
		return false, fmt.Errorf("encrypt access: %w", err)
	}
	var refreshEnc []byte
	if secrets.RefreshToken != "" {
		r, rerr := s.oauthBuf.Encrypt([]byte(secrets.RefreshToken))
		if rerr != nil {
			return false, fmt.Errorf("encrypt refresh: %w", rerr)
		}
		refreshEnc = r
	}

	var expiry string
	if secrets.Expiry != "" {
		if _, perr := time.Parse(expiryTimeLayout, secrets.Expiry); perr != nil {
			log.Printf("[WARN] backfill: %s has invalid expiry %q, ignoring", channelID, secrets.Expiry)
		} else {
			expiry = secrets.Expiry
		}
	}

	scopes := secrets.Scopes

	if err := s.store.UpsertYouTubeOAuthToken(channelID, accessEnc, refreshEnc, "Bearer", expiry, scopes, s.oauthBuf.KeyVersion()); err != nil {
		return false, fmt.Errorf("upsert: %w", err)
	}
	log.Printf("[OK] backfill: imported %s from %s", channelID, name)
	return true, nil
}
