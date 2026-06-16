package youtube

import (
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"velox-server/internal/secrets/aesgcm"
)

func writeAccountJSON(t *testing.T, dir, channelID, access, refresh, expiry string) string {
	t.Helper()
	payload := `{
  "token": "` + access + `",
  "refresh_token": "` + refresh + `",
  "token_uri": "https://oauth2.googleapis.com/token",
  "expiry": "` + expiry + `",
  "channel_id": "` + channelID + `"
}`
	path := filepath.Join(dir, "account_"+channelID+".json")
	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func TestYouTubeBackfillImportsAccountJSON(t *testing.T) {
	tmp := t.TempDir()
	tokensDir := filepath.Join(tmp, "tokens")
	if err := os.MkdirAll(tokensDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	type seed struct{ channelID, access, refresh, expiry string }
	seeds := []seed{
		{"UC_alpha", "alpha-access-AAA", "alpha-refresh-AAA", "2026-12-31T23:59:59Z"},
		{"UC_beta", "beta-access-BBB", "beta-refresh-BBB", "2027-01-15T00:00:00Z"},
		{"UC_gamma", "gamma-access-CCC", "", "2027-02-01T12:00:00Z"},
	}
	for _, s := range seeds {
		writeAccountJSON(t, tokensDir, s.channelID, s.access, s.refresh, s.expiry)
	}
	if err := os.WriteFile(filepath.Join(tokensDir, "account_corrupt.json"), []byte("{not valid JSON"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tokensDir, "OTHER.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	keyBytes := make([]byte, aesgcm.KeySizeBytes)
	if _, err := rand.Read(keyBytes); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	enc, err := aesgcm.NewEncryptor(keyBytes)
	if err != nil {
		t.Fatalf("aesgcm.NewEncryptor: %v", err)
	}

	fake := &fakeYTStore{getReturns: map[string]map[string]interface{}{}}
	srv := &Service{
		config:   &ServiceConfig{DataDir: tmp, TokensDir: tokensDir},
		channels: map[string]*AuthChannel{},
		mu:       sync.RWMutex{},
		store:    fake,
		oauthBuf: enc,
	}
	imported, err := srv.BackfillOAuthTokensFromJSON(context.Background())
	if err != nil {
		t.Fatalf("Backfill: %v", err)
	}
	if imported != 3 {
		t.Errorf("expected 3 imported, got %d (upsertCalls=%d)", imported, len(fake.upsertCalls))
	}
	if len(fake.upsertCalls) != 3 {
		t.Fatalf("expected 3 upsert calls, got %d", len(fake.upsertCalls))
	}
	wantByCh := map[string]seed{}
	for _, s := range seeds {
		wantByCh[s.channelID] = s
	}
	gotCh := map[string]bool{}
	for _, uc := range fake.upsertCalls {
		want, ok := wantByCh[uc.channelID]
		if !ok {
			t.Errorf("upsert for unexpected channel %q", uc.channelID)
			continue
		}
		gotCh[uc.channelID] = true
		if uc.keyVersion != enc.KeyVersion() {
			t.Errorf("%s: keyVersion: got %d, want %d", uc.channelID, uc.keyVersion, enc.KeyVersion())
		}
		if uc.expiry != want.expiry {
			t.Errorf("%s: expiry: got %q, want %q", uc.channelID, uc.expiry, want.expiry)
		}
		plainAccess, err := enc.Decrypt(uc.access)
		if err != nil {
			t.Errorf("%s: decrypt access: %v", uc.channelID, err)
			continue
		}
		if string(plainAccess) != want.access {
			t.Errorf("%s: access blob: got %q, want %q", uc.channelID, plainAccess, want.access)
		}
		if want.refresh != "" {
			if uc.refresh == nil {
				t.Errorf("%s: refresh blob missing", uc.channelID)
				continue
			}
			plainRefresh, err := enc.Decrypt(uc.refresh)
			if err != nil {
				t.Errorf("%s: decrypt refresh: %v", uc.channelID, err)
				continue
			}
			if string(plainRefresh) != want.refresh {
				t.Errorf("%s: refresh blob: got %q, want %q", uc.channelID, plainRefresh, want.refresh)
			}
		} else if uc.refresh != nil {
			t.Errorf("%s: refresh blob materialised despite empty source refresh", uc.channelID)
		}
	}
	for _, s := range seeds {
		if !gotCh[s.channelID] {
			t.Errorf("expected upsert for %s, none found", s.channelID)
		}
	}
}

func TestYouTubeBackfillSkipsAlreadyInSQLite(t *testing.T) {
	tmp := t.TempDir()
	tokensDir := filepath.Join(tmp, "tokens")
	if err := os.MkdirAll(tokensDir, 0o755); err != nil {
		t.Fatal(err)
	}
	channelID := "UC_existing"
	writeAccountJSON(t, tokensDir, channelID, "stale-access-from-json", "stale-refresh", "2025-01-01T00:00:00Z")

	keyBytes := make([]byte, aesgcm.KeySizeBytes)
	if _, err := rand.Read(keyBytes); err != nil {
		t.Fatal(err)
	}
	enc, err := aesgcm.NewEncryptor(keyBytes)
	if err != nil {
		t.Fatal(err)
	}

	fake := &fakeYTStore{
		getReturns: map[string]map[string]interface{}{
			channelID: {
				"channel_id":              channelID,
				"access_token_encrypted":  []byte("already-in-sqlite"),
				"refresh_token_encrypted": []byte("already-in-sqlite"),
				"key_version":             int64(enc.KeyVersion()),
			},
		},
	}

	srv := &Service{
		config:   &ServiceConfig{DataDir: tmp, TokensDir: tokensDir},
		channels: map[string]*AuthChannel{},
		mu:       sync.RWMutex{},
		store:    fake,
		oauthBuf: enc,
	}
	imported, err := srv.BackfillOAuthTokensFromJSON(context.Background())
	if err != nil {
		t.Fatalf("Backfill: %v", err)
	}
	if imported != 0 {
		t.Errorf("expected 0 imports (pre-existing row), got %d", imported)
	}
	if len(fake.upsertCalls) != 0 {
		t.Errorf("expected 0 upsert calls (pre-existing row), got %d", len(fake.upsertCalls))
	}
}

func TestYouTubeBackfillSkipsWhenCipherMissing(t *testing.T) {
	tmp := t.TempDir()
	tokensDir := filepath.Join(tmp, "tokens")
	_ = os.MkdirAll(tokensDir, 0o755)
	writeAccountJSON(t, tokensDir, "UC_x", "a", "r", "2026-01-01T00:00:00Z")

	fake := &fakeYTStore{getReturns: map[string]map[string]interface{}{}}
	srv := &Service{
		config:   &ServiceConfig{DataDir: tmp, TokensDir: tokensDir},
		channels: map[string]*AuthChannel{},
		mu:       sync.RWMutex{},
		store:    fake,
		oauthBuf: nil,
	}
	imported, _ := srv.BackfillOAuthTokensFromJSON(context.Background())
	if imported != 0 {
		t.Errorf("expected 0 imports when cipher missing, got %d", imported)
	}
	if len(fake.upsertCalls) != 0 {
		t.Errorf("expected 0 upsert calls when cipher missing, got %d", len(fake.upsertCalls))
	}
}

func TestYouTubeBackfillSkipsWhenStoreMissing(t *testing.T) {
	tmp := t.TempDir()
	tokensDir := filepath.Join(tmp, "tokens")
	_ = os.MkdirAll(tokensDir, 0o755)
	writeAccountJSON(t, tokensDir, "UC_y", "a", "r", "2026-01-01T00:00:00Z")

	keyBytes := make([]byte, aesgcm.KeySizeBytes)
	_, _ = rand.Read(keyBytes)
	enc, _ := aesgcm.NewEncryptor(keyBytes)

	srv := &Service{
		config:   &ServiceConfig{DataDir: tmp, TokensDir: tokensDir},
		channels: map[string]*AuthChannel{},
		mu:       sync.RWMutex{},
		store:    nil,
		oauthBuf: enc,
	}
	imported, _ := srv.BackfillOAuthTokensFromJSON(context.Background())
	if imported != 0 {
		t.Errorf("expected 0 imports when store missing, got %d", imported)
	}
}

func TestYouTubeBackfillEmptyDirectory(t *testing.T) {
	tmp := t.TempDir()
	tokensDir := filepath.Join(tmp, "never-created")
	keyBytes := make([]byte, aesgcm.KeySizeBytes)
	_, _ = rand.Read(keyBytes)
	enc, _ := aesgcm.NewEncryptor(keyBytes)
	fake := &fakeYTStore{getReturns: map[string]map[string]interface{}{}}
	srv := &Service{
		config:   &ServiceConfig{DataDir: tmp, TokensDir: tokensDir},
		channels: map[string]*AuthChannel{},
		mu:       sync.RWMutex{},
		store:    fake,
		oauthBuf: enc,
	}
	imported, err := srv.BackfillOAuthTokensFromJSON(context.Background())
	if err != nil {
		t.Errorf("Backfill on non-existent dir: got err %v, want nil", err)
	}
	if imported != 0 {
		t.Errorf("Backfill on non-existent dir: imported=%d, want 0", imported)
	}
}
