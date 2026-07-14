package store

import (
	"bytes"
	"testing"
	"time"
)

// ============================================================
// youtube_tracked_niches tests
// ============================================================
//
// Covers the youtube_tracked_niches table: idempotent upsert (ON
// CONFLICT DO NOTHING on the niche PK), ordered list, delete.

func TestTrackedNichesAddAndList(t *testing.T) {
	s := newYouTubeTestStore(t)

	// List empty
	niches, err := s.ListYouTubeTrackedNiches()
	if err != nil {
		t.Fatalf("ListYouTubeTrackedNiches (empty) failed: %v", err)
	}
	if len(niches) != 0 {
		t.Fatalf("expected 0 niches, got %d", len(niches))
	}

	// Add niches
	if err := s.UpsertYouTubeTrackedNiche("wnba"); err != nil {
		t.Fatalf("UpsertYouTubeTrackedNiche(wnba) failed: %v", err)
	}
	if err := s.UpsertYouTubeTrackedNiche("nba"); err != nil {
		t.Fatalf("UpsertYouTubeTrackedNiche(nba) failed: %v", err)
	}

	niches, err = s.ListYouTubeTrackedNiches()
	if err != nil {
		t.Fatalf("ListYouTubeTrackedNiches failed: %v", err)
	}
	if len(niches) != 2 {
		t.Fatalf("expected 2 niches, got %d", len(niches))
	}
	// Ordered by niche
	if niches[0] != "nba" || niches[1] != "wnba" {
		t.Errorf("expected order nba, wnba; got %v", niches)
	}
}

func TestTrackedNichesIdempotent(t *testing.T) {
	s := newYouTubeTestStore(t)

	s.UpsertYouTubeTrackedNiche("sports")
	// Duplicate — ON CONFLICT DO NOTHING
	if err := s.UpsertYouTubeTrackedNiche("sports"); err != nil {
		t.Fatalf("duplicate UpsertYouTubeTrackedNiche failed: %v", err)
	}

	niches, _ := s.ListYouTubeTrackedNiches()
	if len(niches) != 1 {
		t.Errorf("expected 1 niche (idempotent), got %d", len(niches))
	}
}

func TestTrackedNichesDelete(t *testing.T) {
	s := newYouTubeTestStore(t)

	s.UpsertYouTubeTrackedNiche("delete-me")
	if err := s.DeleteYouTubeTrackedNiche("delete-me"); err != nil {
		t.Fatalf("DeleteYouTubeTrackedNiche failed: %v", err)
	}

	niches, _ := s.ListYouTubeTrackedNiches()
	if len(niches) != 0 {
		t.Errorf("expected 0 niches after delete, got %d", len(niches))
	}
}

// ============================================================
// YouTube API Cache tests
// ============================================================
//
// Covers the youtube_api_cache table: Set / Get round-trip, miss
// behavior, update on same key, age-based cleanup, full clear.

func TestYouTubeCacheSetGet(t *testing.T) {
	s := newYouTubeTestStore(t)

	if err := s.SetYouTubeCache("channel:UC_test", 1000, `{"title":"Test"}`); err != nil {
		t.Fatalf("SetYouTubeCache failed: %v", err)
	}

	ts, data, err := s.GetYouTubeCache("channel:UC_test")
	if err != nil {
		t.Fatalf("GetYouTubeCache failed: %v", err)
	}
	if ts != 1000 {
		t.Errorf("timestamp: got %d, want 1000", ts)
	}
	if data != `{"title":"Test"}` {
		t.Errorf("data: got %q, want %q", data, `{"title":"Test"}`)
	}
}

func TestYouTubeCacheMiss(t *testing.T) {
	s := newYouTubeTestStore(t)

	ts, data, err := s.GetYouTubeCache("nonexistent")
	if err != nil {
		t.Fatalf("GetYouTubeCache(miss) failed: %v", err)
	}
	if ts != 0 || data != "" {
		t.Errorf("expected zero value on miss, got ts=%d data=%q", ts, data)
	}
}

func TestYouTubeCacheUpdate(t *testing.T) {
	s := newYouTubeTestStore(t)

	key := "channel:UC_update"
	s.SetYouTubeCache(key, 1000, `{"old": true}`)
	s.SetYouTubeCache(key, 2000, `{"new": true}`)

	ts, data, _ := s.GetYouTubeCache(key)
	if ts != 2000 {
		t.Errorf("timestamp: got %d, want 2000", ts)
	}
	if data != `{"new": true}` {
		t.Errorf("data: got %q, want %q", data, `{"new": true}`)
	}
}

func TestYouTubeCacheCleanup(t *testing.T) {
	s := newYouTubeTestStore(t)

	s.SetYouTubeCache("stale", 1000, `{"stale": true}`)
	s.SetYouTubeCache("fresh", 90000, `{"fresh": true}`)

	// Cleanup uses time.Now().Unix() internally, so both entries will be deleted
	// since their timestamps (1000, 90000) are in the past.
	_, err := s.CleanupYouTubeCache(50000)
	if err != nil {
		t.Fatalf("CleanupYouTubeCache failed: %v", err)
	}
}

func TestYouTubeCacheClear(t *testing.T) {
	s := newYouTubeTestStore(t)

	s.SetYouTubeCache("a", 1, `{}`)
	s.SetYouTubeCache("b", 2, `{}`)

	if err := s.ClearYouTubeCache(); err != nil {
		t.Fatalf("ClearYouTubeCache failed: %v", err)
	}

	_, data, _ := s.GetYouTubeCache("a")
	if data != "" {
		t.Error("expected empty data after clear")
	}
}

// ============================================================
// YouTube OAuth Token storage tests
// ============================================================
//
// Covers the youtube_oauth_tokens table: upsert of encrypted blobs,
// Get returns nil for missing channels, MarkRevoked is idempotent
// (does not bump revoked_at on a second call), and the FK cascade
// from youtube_channels.on_delete.

func TestYouTubeOAuthTokenUpsertGetRevoke(t *testing.T) {
	s := newYouTubeTestStore(t)

	// Seed a parent channel so the FK constraint allows the token row.
	if err := s.UpsertYouTubeChannel("UC_oauth_test", "OAuth Test", "", "", "", "", "", 0, 0, "", "", ""); err != nil {
		t.Fatalf("seed channel: %v", err)
	}

	access1 := []byte("encrypted-access-1")
	refresh1 := []byte("encrypted-refresh-1")
	if err := s.UpsertYouTubeOAuthToken("UC_oauth_test", access1, refresh1, "Bearer", "2026-12-31T23:59:59Z", "scope.read scope.write", 1); err != nil {
		t.Fatalf("first UpsertYouTubeOAuthToken: %v", err)
	}

	row, err := s.GetYouTubeOAuthToken("UC_oauth_test")
	if err != nil {
		t.Fatalf("first GetYouTubeOAuthToken: %v", err)
	}
	if row == nil {
		t.Fatal("expected non-nil row on existing channel")
	}
	if row.ChannelID != "UC_oauth_test" {
		t.Errorf("channel_id: got %v, want UC_oauth_test", row.ChannelID)
	}
	if !bytes.Equal(row.AccessTokenEncrypted, access1) {
		t.Errorf("access blob not stored verbatim: got %v, want %v", row.AccessTokenEncrypted, access1)
	}
	if !bytes.Equal(row.RefreshTokenEncrypted, refresh1) {
		t.Errorf("refresh blob not stored verbatim: got %v, want %v", row.RefreshTokenEncrypted, refresh1)
	}
	if row.KeyVersion != int64(1) {
		t.Errorf("key_version: got %v, want 1", row.KeyVersion)
	}
	if row.RevokedAt != "" {
		t.Errorf("revoked_at should start empty, got %v", row.RevokedAt)
	}

	// Upsert path (replace blobs and bump updated_at)
	access2 := []byte("encrypted-access-2")
	refresh2 := []byte("encrypted-refresh-2")
	if err := s.UpsertYouTubeOAuthToken("UC_oauth_test", access2, refresh2, "Bearer", "2027-01-01T00:00:00Z", "scope.read", 1); err != nil {
		t.Fatalf("second UpsertYouTubeOAuthToken: %v", err)
	}
	row, _ = s.GetYouTubeOAuthToken("UC_oauth_test")
	if !bytes.Equal(row.AccessTokenEncrypted, access2) {
		t.Error("access blob not updated on second upsert")
	}
	if !bytes.Equal(row.RefreshTokenEncrypted, refresh2) {
		t.Error("refresh blob not updated on second upsert")
	}
	if row.Expiry != "2027-01-01T00:00:00Z" {
		t.Errorf("expiry: got %v, want 2027-01-01T00:00:00Z", row.Expiry)
	}
	if row.RevokedAt != "" {
		t.Error("upsert must not touch revoked_at")
	}

	// MarkRevoked stamps a timestamp; idempotent on re-call.
	if err := s.MarkYouTubeOAuthTokenRevoked("UC_oauth_test"); err != nil {
		t.Fatalf("MarkYouTubeOAuthTokenRevoked: %v", err)
	}
	row, _ = s.GetYouTubeOAuthToken("UC_oauth_test")
	if row.RevokedAt == "" {
		t.Error("revoked_at should be set after MarkRevoked")
	}
	prevRevoked := row.RevokedAt
	time.Sleep(20 * time.Millisecond)
	if err := s.MarkYouTubeOAuthTokenRevoked("UC_oauth_test"); err != nil {
		t.Fatalf("second MarkRevoked: %v", err)
	}
	row2, _ := s.GetYouTubeOAuthToken("UC_oauth_test")
	if row2.RevokedAt != prevRevoked {
		t.Errorf("second MarkRevoked changed revoked_at: before=%v, after=%v", prevRevoked, row2.RevokedAt)
	}
}

func TestYouTubeOAuthTokenMissingChannel(t *testing.T) {
	s := newYouTubeTestStore(t)

	row, err := s.GetYouTubeOAuthToken("UC_nonexistent")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if row != nil {
		t.Errorf("expected nil row for missing channel, got %v", row)
	}
}

func TestYouTubeOAuthTokenChannelFKDeleteCascade(t *testing.T) {
	s := newYouTubeTestStore(t)

	if err := s.UpsertYouTubeChannel("UC_cascade", "", "", "", "", "", "", 0, 0, "", "", ""); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertYouTubeOAuthToken("UC_cascade", []byte("a"), []byte("r"), "Bearer", "", "", 1); err != nil {
		t.Fatal(err)
	}
	row, err := s.GetYouTubeOAuthToken("UC_cascade")
	if err != nil || row == nil {
		t.Fatalf("expected token row to exist before channel delete: err=%v row=%v", err, row)
	}
	if err := s.DeleteYouTubeChannel("UC_cascade"); err != nil {
		t.Fatal(err)
	}
	row, err = s.GetYouTubeOAuthToken("UC_cascade")
	if err != nil {
		t.Fatalf("unexpected error after delete: %v", err)
	}
	if row != nil {
		t.Errorf("expected token row to be cascade-deleted, got %v", row)
	}
}

// ============================================================
// TestUpsertYouTubeOAuthToken_PreservesRevokedAt
// ============================================================
//
// Converse pin to TestConnectChannelAtomic_ResetsRevokedAtOnReauth
// (which lives in sqlite_youtube_channels_test.go).
//
// The OAuth-leg UPDATE in ConnectChannelAtomic resets revoked_at to
// NULL because that path is the explicit new-auth flow (a user
// redoing OAuth must be reactivated). The auto-refresh path goes
// through UpsertYouTubeOAuthToken, which must NOT reset revoked_at
// — otherwise a routine token refresh would silently un-revoke a
// credential an operator explicitly revoked. This test pins that
// invariant by calling UpsertYouTubeOAuthToken on a previously-
// revoked channel and asserting revoked_at is preserved verbatim.
func TestUpsertYouTubeOAuthToken_PreservesRevokedAt(t *testing.T) {
	s := newYouTubeTestStore(t)

	const channel = "UC_refresh_preserves_revoke"
	if err := s.UpsertYouTubeChannel(channel, "Refresh Preserves Revoke", "", "", "", "", "", 0, 0, "", "", ""); err != nil {
		t.Fatalf("seed channel: %v", err)
	}
	if err := s.UpsertYouTubeOAuthToken(channel, []byte("orig-access"), []byte("orig-refresh"), "Bearer", "2026-01-01T00:00:00Z", "scope.read", 1); err != nil {
		t.Fatalf("seed oauth: %v", err)
	}

	// Mark the row revoked; persistRefreshedToken will later be called
	// on this row to simulate a token refresh.
	if err := s.MarkYouTubeOAuthTokenRevoked(channel); err != nil {
		t.Fatalf("MarkYouTubeOAuthTokenRevoked: %v", err)
	}
	row, _ := s.GetYouTubeOAuthToken(channel)
	if row == nil {
		t.Fatal("setup: expected oauth row present after revoke")
	}
	originalRevokedAt := row.RevokedAt
	if originalRevokedAt == "" {
		t.Fatal("setup: revoked_at empty after MarkRevoked")
	}
	originalUpdatedAt := row.UpdatedAt
	if originalUpdatedAt == "" {
		t.Fatal("setup: updated_at empty")
	}

	// Simulate the refresh path: new encrypted access blob, same key
	// version, new expiry. The other columns that persistRefreshedToken
	// updates (refresh_token blob, scopes) are exercised too.
	time.Sleep(20 * time.Millisecond) // ensure updated_at advances
	newAccess := []byte("refresh-access-encrypted")
	newRefresh := []byte("refresh-rotated-encrypted")
	if err := s.UpsertYouTubeOAuthToken(channel, newAccess, newRefresh, "Bearer", "2030-12-31T23:59:59Z", "scope.read", 1); err != nil {
		t.Fatalf("Upsert after revoke: %v", err)
	}

	row, _ = s.GetYouTubeOAuthToken(channel)
	if row == nil {
		t.Fatal("post: expected oauth row to remain after refresh upsert")
	}

	// The crucial invariant: revoked_at is NOT touched by the refresh path.
	if gotRevokedAt := row.RevokedAt; gotRevokedAt != originalRevokedAt {
		t.Errorf("revoked_at was reset on refresh path (was %q, now %q); auto-refresh MUST NOT un-revoke",
			originalRevokedAt, gotRevokedAt)
	}

	// Sanity: the access blob did get rotated (this is the whole point
	// of the refresh), so the test is exercising the right call.
	if !bytes.Equal(row.AccessTokenEncrypted, newAccess) {
		t.Errorf("access blob not rotated by refresh: got %v, want %v", row.AccessTokenEncrypted, newAccess)
	}
	if !bytes.Equal(row.RefreshTokenEncrypted, newRefresh) {
		t.Errorf("refresh blob not rotated by refresh: got %v, want %v", row.RefreshTokenEncrypted, newRefresh)
	}
	if row.Expiry != "2030-12-31T23:59:59Z" {
		t.Errorf("expiry not updated by refresh: got %v, want 2030-12-31T23:59:59Z", row.Expiry)
	}
	if upd := row.UpdatedAt; upd == "" {
		t.Error("updated_at is empty after refresh")
	} else if upd < originalUpdatedAt {
		t.Errorf("updated_at did not advance (was %s, now %s)", originalUpdatedAt, upd)
	}

	// And the boot hydrator's view: ListActiveYouTubeOAuthTokens must
	// still skip this channel even though the access blob was rotated.
	active, err := s.ListActiveYouTubeOAuthTokens()
	if err != nil {
		t.Fatalf("ListActive: %v", err)
	}
	for _, r := range active {
		if r.ChannelID == channel {
			t.Errorf("ListActiveYouTubeOAuthTokens should still skip the revoked-and-refreshed channel; got %v", r)
			break
		}
	}
}
