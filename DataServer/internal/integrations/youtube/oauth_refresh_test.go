package youtube

import (
	"bytes"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"golang.org/x/oauth2"

	"velox-server/internal/secrets/aesgcm"
	"velox-server/internal/store/youtubetypes"
)

// fakeYTStore implements YouTubeStore by embedding the interface as a nil
// field. Any YouTubeStore method we forget to override panics loudly
// rather than silently becoming a no-op. The tests using fakeYTStore only
// call the methods they explicitly override.
type fakeYTStore struct {
	YouTubeStore

	upsertCalls []fakeUpsert
	getReturns  map[string]*youtubetypes.YouTubeOAuthToken
	getErr      error
	getCalled   int

	// Boot-hydrator inputs/outputs used by TestLoadOAuthChannelsFromSQLiteHydratesCache.
	listReturns    []youtubetypes.YouTubeOAuthToken
	listErr        error
	orphanReturns  []youtubetypes.YouTubeTokenOrphan
	orphanErr      error
	channelRows    []youtubetypes.YouTubeChannel
	channelRowsErr error
}

type fakeUpsert struct {
	channelID  string
	access     []byte
	refresh    []byte
	expiry     string
	keyVersion int
}

func (f *fakeYTStore) UpsertYouTubeOAuthToken(channelID string, accessEnc, refreshEnc []byte, tokenType, expiry, scopes string, keyVersion int) error {
	f.upsertCalls = append(f.upsertCalls, fakeUpsert{
		channelID:  channelID,
		access:     accessEnc,
		refresh:    refreshEnc,
		expiry:     expiry,
		keyVersion: keyVersion,
	})
	return nil
}

func (f *fakeYTStore) GetYouTubeOAuthToken(channelID string) (*youtubetypes.YouTubeOAuthToken, error) {
	f.getCalled++
	if row, ok := f.getReturns[channelID]; ok {
		return row, f.getErr
	}
	return nil, f.getErr
}

func TestYouTubeRefreshPersistsNewAccessTokenToSQLite(t *testing.T) {
	keyBytes := make([]byte, aesgcm.KeySizeBytes)
	if _, err := rand.Read(keyBytes); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	enc, err := aesgcm.NewEncryptor(keyBytes)
	if err != nil {
		t.Fatalf("aesgcm.NewEncryptor: %v", err)
	}

	channelID := "UC_refresh_test"
	oldAccess := "access-token-initial-AAA"
	oldRefresh := "refresh-token-initial-BBB"
	oldExpiry := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	oldAccessEnc, err := enc.Encrypt([]byte(oldAccess))
	if err != nil {
		t.Fatalf("seed encrypt access: %v", err)
	}
	oldRefreshEnc, err := enc.Encrypt([]byte(oldRefresh))
	if err != nil {
		t.Fatalf("seed encrypt refresh: %v", err)
	}

	fake := &fakeYTStore{
		getReturns: map[string]*youtubetypes.YouTubeOAuthToken{
			channelID: {
				ChannelID:            channelID,
				AccessTokenEncrypted: oldAccessEnc,
				RefreshTokenEncrypted: oldRefreshEnc,
				TokenType:            "Bearer",
				Expiry:               oldExpiry.Format(time.RFC3339),
				KeyVersion:           1,
			},
		},
	}

	srv := &Service{
		store:    fake,
		oauthBuf: enc,
	}

	newAccess := "access-token-refreshed-CCC"
	newExpiry := time.Date(2027, 6, 15, 12, 0, 0, 0, time.UTC)
	refreshedNoRotation := &oauth2.Token{
		AccessToken:  newAccess,
		RefreshToken: "",
		TokenType:    "Bearer",
		Expiry:       newExpiry,
	}

	if err := srv.persistRefreshedToken(channelID, refreshedNoRotation); err != nil {
		t.Fatalf("persistRefreshedToken: %v", err)
	}
	if len(fake.upsertCalls) != 1 {
		t.Fatalf("expected 1 UpsertYouTubeOAuthToken call, got %d", len(fake.upsertCalls))
	}
	uc := fake.upsertCalls[0]
	if uc.channelID != channelID {
		t.Errorf("upsert channelID: got %q, want %q", uc.channelID, channelID)
	}
	if uc.keyVersion != 1 {
		t.Errorf("upsert key_version: got %d, want %d", uc.keyVersion, enc.KeyVersion())
	}
	plainAccess, err := enc.Decrypt(uc.access)
	if err != nil {
		t.Fatalf("decrypt access blob: %v", err)
	}
	if string(plainAccess) != newAccess {
		t.Errorf("post-refresh access blob did not decrypt to the new access_token: got %q, want %q", plainAccess, newAccess)
	}
	if !bytes.Equal(uc.refresh, oldRefreshEnc) {
		t.Errorf("refresh blob was not preserved when new grant omitted refresh_token")
	}
	if fake.getCalled < 1 {
		t.Errorf("GetYouTubeOAuthToken was not consulted; calls=%d", fake.getCalled)
	}

	pts := &PersistedTokenSource{
		source: oauth2.StaticTokenSource(refreshedNoRotation),
		save: func(nt *oauth2.Token) error {
			if nt.AccessToken == oldAccess {
				return nil
			}
			return srv.persistRefreshedToken(channelID, nt)
		},
	}
	if _, err := pts.Token(); err != nil {
		t.Fatalf("pts.Token: %v", err)
	}
	if len(fake.upsertCalls) != 2 {
		t.Fatalf("expected 2 upsert calls after pts.Token(), got %d", len(fake.upsertCalls))
	}
	last := fake.upsertCalls[1]
	plainAccess2, err := enc.Decrypt(last.access)
	if err != nil {
		t.Fatalf("decrypt access blob after pts.Token: %v", err)
	}
	if string(plainAccess2) != newAccess {
		t.Errorf("post-pts access blob did not decrypt to new access_token: got %q, want %q", plainAccess2, newAccess)
	}

	rotatedRefresh := "refresh-token-rotated-DDD"
	rotated := &oauth2.Token{
		AccessToken:  "access-token-second-refresh-EEE",
		RefreshToken: rotatedRefresh,
		TokenType:    "Bearer",
		Expiry:       time.Date(2028, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	if err := srv.persistRefreshedToken(channelID, rotated); err != nil {
		t.Fatalf("persistRefreshedToken (rotated): %v", err)
	}
	if len(fake.upsertCalls) != 3 {
		t.Fatalf("expected 3 upsert calls after rotation, got %d", len(fake.upsertCalls))
	}
	rotCall := fake.upsertCalls[2]
	plainRotRefresh, err := enc.Decrypt(rotCall.refresh)
	if err != nil {
		t.Fatalf("decrypt rotated refresh blob: %v", err)
	}
	if string(plainRotRefresh) != rotatedRefresh {
		t.Errorf("rotated refresh token was not propagated: got %q, want %q", plainRotRefresh, rotatedRefresh)
	}
}

func TestYouTubeRefreshIdempotentOnSameAccessToken(t *testing.T) {
	keyBytes := make([]byte, aesgcm.KeySizeBytes)
	if _, err := rand.Read(keyBytes); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	enc, err := aesgcm.NewEncryptor(keyBytes)
	if err != nil {
		t.Fatalf("aesgcm.NewEncryptor: %v", err)
	}
	channelID := "UC_idem"
	currentAccess := "current-access"
	sameToken := &oauth2.Token{
		AccessToken: currentAccess,
		Expiry:      time.Now().Add(1 * time.Hour),
	}
	srv := &Service{
		store:    &fakeYTStore{},
		oauthBuf: enc,
	}
	pts := &PersistedTokenSource{
		source: oauth2.StaticTokenSource(sameToken),
		save: func(nt *oauth2.Token) error {
			if nt.AccessToken == currentAccess {
				return nil
			}
			return srv.persistRefreshedToken(channelID, nt)
		},
	}
	pts.Token()
	store := srv.store.(*fakeYTStore)
	if len(store.upsertCalls) != 0 {
		t.Errorf("expected zero upserts when access_token unchanged; got %d", len(store.upsertCalls))
	}
}

// TestYouTubeRefreshPreservesNullRefreshToken pins the NULL-refresh invariant:
// when the youtube_oauth_tokens row has no refresh_token_encrypted blob (a
// service-account OAuth grant may issue NO refresh_token), persistRefreshedToken
// called with an empty newToken.RefreshToken MUST NOT introduce a non-NULL
// blob. Otherwise a regression in the read-modify-write loop could silently
// overwrite genuine NULL rows with empty []byte values, breaking downstream
// code that distinguishes NULL from empty.
func TestYouTubeRefreshPreservesNullRefreshToken(t *testing.T) {
	keyBytes := make([]byte, aesgcm.KeySizeBytes)
	if _, err := rand.Read(keyBytes); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	enc, err := aesgcm.NewEncryptor(keyBytes)
	if err != nil {
		t.Fatalf("aesgcm.NewEncryptor: %v", err)
	}

	channelID := "UC_null_refresh"
	oldAccess := "access-token-no-refresh-AAA"
	oldExpiry := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	oldAccessEnc, err := enc.Encrypt([]byte(oldAccess))
	if err != nil {
		t.Fatalf("seed encrypt access: %v", err)
	}

	// Seed fake store with refresh_token_encrypted ABSENT
	// (mimicking a SQL NULL row when the store scan drops NULL keys).
	fake := &fakeYTStore{
		getReturns: map[string]*youtubetypes.YouTubeOAuthToken{
			channelID: {
				ChannelID:            channelID,
				AccessTokenEncrypted: oldAccessEnc,
				// NOTE: no RefreshTokenEncrypted
				TokenType:  "Bearer",
				Expiry:     oldExpiry.Format(time.RFC3339),
				KeyVersion: 1,
			},
		},
	}

	srv := &Service{
		store:    fake,
		oauthBuf: enc,
	}

	newAccess := "access-token-refreshed-no-refresh-BBB"
	refreshed := &oauth2.Token{
		AccessToken:  newAccess,
		RefreshToken: "",
		TokenType:    "Bearer",
		Expiry:       time.Date(2027, 6, 15, 12, 0, 0, 0, time.UTC),
	}

	if err := srv.persistRefreshedToken(channelID, refreshed); err != nil {
		t.Fatalf("persistRefreshedToken (null refresh): %v", err)
	}
	if len(fake.upsertCalls) != 1 {
		t.Fatalf("expected 1 upsert call, got %d", len(fake.upsertCalls))
	}
	uc := fake.upsertCalls[0]

	if uc.refresh != nil {
		t.Errorf("refresh blob was wrongly materialised: got len=%d, want nil", len(uc.refresh))
	}

	plainAccess, err := enc.Decrypt(uc.access)
	if err != nil {
		t.Fatalf("decrypt access blob: %v", err)
	}
	if string(plainAccess) != newAccess {
		t.Errorf("access blob did not decrypt to new access_token: got %q, want %q", plainAccess, newAccess)
	}
}

// errors usage is reserved for future tests in this file.
var _ = errors.New

// fakeYTStore extensions for tests that need them.

func (f *fakeYTStore) ListActiveYouTubeOAuthTokens() ([]youtubetypes.YouTubeOAuthToken, error) {
	if f.listReturns == nil {
		return nil, nil
	}
	return f.listReturns, f.listErr
}

func (f *fakeYTStore) AuditYouTubeOAuthTokenOrphans() ([]youtubetypes.YouTubeTokenOrphan, error) {
	if f.orphanReturns == nil {
		return nil, nil
	}
	return f.orphanReturns, f.orphanErr
}

func (f *fakeYTStore) ListYouTubeChannels() ([]youtubetypes.YouTubeChannel, error) {
	if f.channelRows == nil {
		return nil, nil
	}
	return f.channelRows, f.channelRowsErr
}
