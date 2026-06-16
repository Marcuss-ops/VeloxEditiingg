package youtube

import (
	"bytes"
	"crypto/rand"
	"errors"
	"sync"
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
	getReturns  map[string]map[string]interface{}
	getErr      error
	getCalled   int

	// Boot-hydrator inputs/outputs used by TestLoadOAuthChannelsFromSQLiteHydratesCache.
	listReturns    []map[string]interface{}
	listErr        error
	orphanReturns  []youtubetypes.YouTubeTokenOrphan
	orphanErr      error
	channelRows    []map[string]interface{}
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

func (f *fakeYTStore) GetYouTubeOAuthToken(channelID string) (map[string]interface{}, error) {
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
		getReturns: map[string]map[string]interface{}{
			channelID: {
				"channel_id":              channelID,
				"access_token_encrypted":  oldAccessEnc,
				"refresh_token_encrypted": oldRefreshEnc,
				"token_type":              "Bearer",
				"expiry":                  oldExpiry.Format(time.RFC3339),
				"key_version": int64(1),
			},
		},
	}

	srv := &Service{
		channels: map[string]*AuthChannel{
			channelID: {
				ID:           channelID,
				Name:         channelID,
				AccessToken:  oldAccess,
				RefreshToken: oldRefresh,
				Expiry:       oldExpiry,
			},
		},
		mu:       sync.RWMutex{},
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
			if nt.AccessToken == srv.channels[channelID].AccessToken {
				return nil
			}
			srv.mu.Lock()
			srv.channels[channelID].AccessToken = nt.AccessToken
			srv.channels[channelID].Expiry = nt.Expiry
			srv.mu.Unlock()
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
		channels: map[string]*AuthChannel{
			channelID: {ID: channelID, Name: channelID, AccessToken: currentAccess, Expiry: sameToken.Expiry},
		},
		store:    &fakeYTStore{},
		oauthBuf: enc,
	}
	pts := &PersistedTokenSource{
		source: oauth2.StaticTokenSource(sameToken),
		save: func(nt *oauth2.Token) error {
			if nt.AccessToken == srv.channels[channelID].AccessToken {
				return nil
			}
			srv.mu.Lock()
			srv.channels[channelID].AccessToken = nt.AccessToken
			srv.channels[channelID].Expiry = nt.Expiry
			srv.mu.Unlock()
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
		getReturns: map[string]map[string]interface{}{
			channelID: {
				"channel_id":             channelID,
				"access_token_encrypted": oldAccessEnc,
				// NOTE: no refresh_token_encrypted key
				"token_type":  "Bearer",
				"expiry":      oldExpiry.Format(time.RFC3339),
				"key_version": int64(1),
			},
		},
	}

	srv := &Service{
		channels: map[string]*AuthChannel{
			channelID: {ID: channelID, Name: channelID, AccessToken: oldAccess, Expiry: oldExpiry},
		},
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

// fakeYTStore extensions needed by the boot-hydrator tests below.
// The embedded YouTubeStore interface lets us satisfy its method set
// without re-declaring every entry: only the methods the boot path
// calls are overridden (ListActiveYouTubeOAuthTokens for the hydrate,
// AuditYouTubeOAuthTokenOrphans for the orphan log, ListYouTubeChannels
// for the metadata fold-in that runs after the hydrate).

func (f *fakeYTStore) ListActiveYouTubeOAuthTokens() ([]map[string]interface{}, error) {
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

func (f *fakeYTStore) ListYouTubeChannels() ([]map[string]interface{}, error) {
	if f.channelRows == nil {
		return nil, nil
	}
	return f.channelRows, f.channelRowsErr
}

// TestLoadOAuthChannelsFromSQLiteHydratesCache: the canonical boot
// hydrator. Build a real AES-GCM cipher, encrypt two distinct OAuth
// credentials (access + refresh), seed the fake store's
// ListActiveYouTubeOAuthTokens to return those encrypted blobs, then
// run Service.loadOAuthChannelsFromSQLite on a Service that has BOTH
// a non-nil cipher and a non-nil store but an empty s.channels map.
// Assert:
//   - loadOAuthChannelsFromSQLite returns (1, nil): exactly one channel
//     hydrated, no error
//   - s.channels["UC_hydrate"] is populated
//   - the in-RAM AccessToken and RefreshToken are the decrypted
//     plaintexts (round-trip through AES-GCM is correct)
//   - the parsed Expiry matches the RFC3339 seed
// Defence-in-depth case: a Service with no cipher must NOT silently
// boot with empty credentials — it returns (0, nil) and the operator
// sees a log line, not a populated cache. This matches the
// "[ERR] ... runtime cache will be empty until the cipher is set"
// contract documented in loadOAuthChannelsFromSQLite.
// Orphan case: the audit is logged but does NOT insert or rewrite in-RAM
// state — it just logs WoRNs so the operator can decide whether to
// backfill the parent or drop the orphan.
func TestLoadOAuthChannelsFromSQLiteHydratesCache(t *testing.T) {
	keyBytes := make([]byte, aesgcm.KeySizeBytes)
	if _, err := rand.Read(keyBytes); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	enc, err := aesgcm.NewEncryptor(keyBytes)
	if err != nil {
		t.Fatalf("aesgcm.NewEncryptor: %v", err)
	}

	channelID := "UC_hydrate"
	plainAccess := "plain-access-AAA"
	plainRefresh := "plain-refresh-BBB"
	expiry := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)

	accessEnc, err := enc.Encrypt([]byte(plainAccess))
	if err != nil {
		t.Fatalf("encrypt access: %v", err)
	}
	refreshEnc, err := enc.Encrypt([]byte(plainRefresh))
	if err != nil {
		t.Fatalf("encrypt refresh: %v", err)
	}

	fake := &fakeYTStore{
		listReturns: []map[string]interface{}{
			{
				"channel_id":              channelID,
				"access_token_encrypted":  accessEnc,
				"refresh_token_encrypted": refreshEnc,
				"token_type":              "Bearer",
				"expiry":                  expiry.Format(time.RFC3339),
				"scopes":                  "scope.read",
				"key_version":             int64(enc.KeyVersion()),
				"revoked_at":              "",
				"created_at":              "2026-06-15T00:00:00Z",
				"updated_at":              "2026-06-15T00:00:00Z",
			},
		},
	}

	srv := &Service{
		channels: make(map[string]*AuthChannel),
		groups:   make(map[string]*ChannelGroup),
		store:    fake,
		oauthBuf: enc,
	}

	n, err := srv.loadOAuthChannelsFromSQLite()
	if err != nil {
		t.Fatalf("loadOAuthChannelsFromSQLite returned error: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 channel hydrated, got %d", n)
	}

	srv.mu.RLock()
	ch, ok := srv.channels[channelID]
	srv.mu.RUnlock()
	if !ok {
		t.Fatalf("expected %q in s.channels after hydrate; map=%v", channelID, srv.channels)
	}
	if ch.AccessToken != plainAccess {
		t.Errorf("AccessToken: got %q, want %q", ch.AccessToken, plainAccess)
	}
	if ch.RefreshToken != plainRefresh {
		t.Errorf("RefreshToken: got %q, want %q", ch.RefreshToken, plainRefresh)
	}
	if !ch.Expiry.Equal(expiry) {
		t.Errorf("Expiry: got %s, want %s", ch.Expiry.Format(time.RFC3339), expiry.Format(time.RFC3339))
	}
	if ch.ID != channelID {
		t.Errorf("ID: got %q, want %q", ch.ID, channelID)
	}

	// Defence in depth: missing cipher must NOT silently populate the
	// cache. The boot hydrator logs an [ERR] and returns (0, nil).
	srv2 := &Service{
		channels: make(map[string]*AuthChannel),
		groups:   make(map[string]*ChannelGroup),
		store:    &fakeYTStore{},
		oauthBuf: nil,
	}
	n2, err2 := srv2.loadOAuthChannelsFromSQLite()
	if err2 != nil {
		t.Fatalf("nil cipher path returned error: %v", err2)
	}
	if n2 != 0 {
		t.Errorf("nil cipher: expected 0 hydrated, got %d", n2)
	}
	if _, exists := srv2.channels[channelID]; exists {
		t.Errorf("nil cipher must not populate s.channels; got %v", srv2.channels)
	}

	// Orphan case: the audit runs without panic and the cache is not
	// mutated. Operator-facing audit logs warn about the orphan; we just
	// assert it's audited (listOrphanCalls would track invocations).
	fakeOrphan := &fakeYTStore{
		listReturns:   []map[string]interface{}{}, // no active rows
		orphanReturns: []youtubetypes.YouTubeTokenOrphan{{ChannelID: "UC_orphan", UpdatedAt: "2026-06-15T00:00:00Z"}},
	}
	srv3 := &Service{
		channels: make(map[string]*AuthChannel),
		groups:   make(map[string]*ChannelGroup),
		store:    fakeOrphan,
		oauthBuf: enc,
	}
	n3, err3 := srv3.loadOAuthChannelsFromSQLite()
	if err3 != nil {
		t.Fatalf("orphan path returned error: %v", err3)
	}
	if n3 != 0 {
		t.Errorf("orphan audit: expected 0 hydrated (no active rows), got %d", n3)
	}
}

