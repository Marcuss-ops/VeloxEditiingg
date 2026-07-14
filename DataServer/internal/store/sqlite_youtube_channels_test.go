package store

import (
	"bytes"
	"testing"
	"time"

	"velox-server/internal/store/youtubetypes"
)

// ============================================================
// youtube_channels (canonical) tests
// ============================================================
//
// Covers the youtube_channels table: CRUD, the canonical-update
// guard rails (added_at preservation, metadata-only refresh, empty
// defaults), the channel delete-atomic path, and the
// ConnectChannelAtomic transactional upsert that joins channels
// and oauth rows.

func TestYouTubeChannelCRUD(t *testing.T) {
	s := newYouTubeTestStore(t)

	err := s.UpsertYouTubeChannel("UC_test123", "Test Channel", "Test Display", "https://youtube.com/@test", "https://img.example.com/thumb.jpg", "en", "A test channel", 1500, 500, "2024-01-01T00:00:00Z", "2024-06-01T00:00:00Z", "")
	if err != nil {
		t.Fatalf("UpsertYouTubeChannel failed: %v", err)
	}

	// Get
	ch, err := s.GetYouTubeChannel("UC_test123")
	if err != nil {
		t.Fatalf("GetYouTubeChannel failed: %v", err)
	}
	if ch == nil {
		t.Fatal("expected non-nil channel")
	}
	if ch.ChannelID != "UC_test123" {
		t.Errorf("channel_id: got %v, want UC_test123", ch.ChannelID)
	}
	if ch.Title != "Test Channel" {
		t.Errorf("title: got %v, want %q", ch.Title, "Test Channel")
	}
	if ch.DisplayName != "Test Display" {
		t.Errorf("display_name: got %v, want %q", ch.DisplayName, "Test Display")
	}
	if ch.Language != "en" {
		t.Errorf("language: got %v, want %q", ch.Language, "en")
	}
	if ch.ViewCount != int64(1500) {
		t.Errorf("view_count: got %v, want 1500", ch.ViewCount)
	}
	if ch.SubscriberCount != int64(500) {
		t.Errorf("subscriber_count: got %v, want 500", ch.SubscriberCount)
	}
}

func TestYouTubeChannelUpdatePreservesAddedAt(t *testing.T) {
	s := newYouTubeTestStore(t)

	// First insert with specific added_at
	s.UpsertYouTubeChannel("UC_test456", "Original Title", "", "", "", "", "", 0, 0, "2024-01-15T00:00:00Z", "", "")

	// Update title and stats, pass empty added_at — should preserve original
	s.UpsertYouTubeChannel("UC_test456", "Updated Title", "", "", "", "", "", 200, 100, "", "2024-06-15T00:00:00Z", "")

	ch, err := s.GetYouTubeChannel("UC_test456")
	if err != nil {
		t.Fatalf("GetYouTubeChannel failed: %v", err)
	}
	if ch.Title != "Updated Title" {
		t.Errorf("title: got %v, want %q", ch.Title, "Updated Title")
	}
	if ch.AddedAt != "2024-01-15T00:00:00Z" {
		t.Errorf("added_at was overwritten: got %v, want 2024-01-15T00:00:00Z", ch.AddedAt)
	}
	if ch.LastSyncAt != "2024-06-15T00:00:00Z" {
		t.Errorf("last_sync_at: got %v, want 2024-06-15T00:00:00Z", ch.LastSyncAt)
	}
}

func TestYouTubeChannelListAndDelete(t *testing.T) {
	s := newYouTubeTestStore(t)

	// List empty
	channels, err := s.ListYouTubeChannels()
	if err != nil {
		t.Fatalf("ListYouTubeChannels (empty) failed: %v", err)
	}
	if len(channels) != 0 {
		t.Fatalf("expected 0 channels, got %d", len(channels))
	}

	s.UpsertYouTubeChannel("UC_a", "Alpha", "", "", "", "", "", 0, 0, "", "", "")
	s.UpsertYouTubeChannel("UC_b", "Beta", "", "", "", "", "", 0, 0, "", "", "")

	channels, err = s.ListYouTubeChannels()
	if err != nil {
		t.Fatalf("ListYouTubeChannels failed: %v", err)
	}
	if len(channels) != 2 {
		t.Fatalf("expected 2 channels, got %d", len(channels))
	}
	// Ordered by title
	if channels[0].Title != "Alpha" || channels[1].Title != "Beta" {
		t.Errorf("expected order Alpha, Beta; got %v, %v", channels[0].Title, channels[1].Title)
	}

	// Delete
	if err := s.DeleteYouTubeChannel("UC_a"); err != nil {
		t.Fatalf("DeleteYouTubeChannel failed: %v", err)
	}
	channels, _ = s.ListYouTubeChannels()
	if len(channels) != 1 {
		t.Fatalf("expected 1 channel after delete, got %d", len(channels))
	}

	// Get deleted should fail
	_, err = s.GetYouTubeChannel("UC_a")
	if err == nil {
		t.Error("expected error for deleted channel")
	}
}

func TestYouTubeChannelUpdateMetadataRefresh(t *testing.T) {
	s := newYouTubeTestStore(t)

	// Seed with rich data so we can assert WHICH columns the refresh
	// changes vs which it preserves. Refresh is metadata-only: title and
	// thumbnail. User-edited columns (display_name, language, notes,
	// channel_url, view_count, subscriber_count) MUST NOT be touched.
	// (metadata_json was dropped by migration 014; no longer a column.)
	if err := s.UpsertYouTubeChannel("UC_refresh_test", "Original Title", "Original Display", "https://youtube.com/@orig", "https://img.example.com/orig.jpg", "en", "user notes", 1234, 567, "2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z", ""); err != nil {
		t.Fatalf("seed UpsertYouTubeChannel: %v", err)
	}

	if err := s.UpdateYouTubeChannelMetadata(
		"UC_refresh_test", "Refreshed Title", "https://img.example.com/refresh.jpg",
	); err != nil {
		t.Fatalf("UpdateYouTubeChannelMetadata: %v", err)
	}

	got, err := s.GetYouTubeChannel("UC_refresh_test")
	if err != nil {
		t.Fatalf("GetYouTubeChannel: %v", err)
	}
	if got.Title != "Refreshed Title" {
		t.Errorf("title: got %v, want Refreshed Title", got.Title)
	}
	if got.ThumbnailURL != "https://img.example.com/refresh.jpg" {
		t.Errorf("thumbnail_url: got %v, want refreshed thumbnail", got.ThumbnailURL)
	}
	for _, c := range []struct {
		col  string
		want string
	}{
		{"display_name", "Original Display"},
		{"language", "en"},
		{"notes", "user notes"},
		{"channel_url", "https://youtube.com/@orig"},
	} {
		var gotVal string
		switch c.col {
		case "display_name":
			gotVal = got.DisplayName
		case "language":
			gotVal = got.Language
		case "notes":
			gotVal = got.Notes
		case "channel_url":
			gotVal = got.ChannelURL
		}
		if gotVal != c.want {
			t.Errorf("%s was clobbered by refresh: got %v, want %q", c.col, gotVal, c.want)
		}
	}
	if got.ViewCount != int64(1234) {
		t.Errorf("view_count was clobbered: got %v, want 1234", got.ViewCount)
	}
	if got.SubscriberCount != int64(567) {
		t.Errorf("subscriber_count was clobbered: got %v, want 567", got.SubscriberCount)
	}
	if lastSyncAt := got.LastSyncAt; lastSyncAt == "" {
		t.Errorf("last_sync_at is empty after refresh; want recent RFC3339 timestamp")
	}
}

func TestYouTubeChannelEmptyDefaultValues(t *testing.T) {
	s := newYouTubeTestStore(t)

	s.UpsertYouTubeChannel("UC_minimal", "", "", "", "", "", "", 0, 0, "", "", "")

	ch, err := s.GetYouTubeChannel("UC_minimal")
	if err != nil {
		t.Fatalf("GetYouTubeChannel failed: %v", err)
	}
	// title should default to empty string
	if title := ch.Title; title != "" {
		t.Errorf("title: got %v, want empty string", ch.Title)
	}
	// added_at should be auto-set
	if added := ch.AddedAt; added == "" {
		t.Error("expected added_at to be auto-set")
	}
}

// ============================================================
// youtube_channel DeleteAtomic tests
// ============================================================
//
// DeleteChannelAtomic removes the group-channel memberships and the
// youtube_channels row in one transaction; the youtube_oauth_tokens
// row is cascade-deleted by the FK. We pin that contract here so a
// future refactor cannot silently leave a token row orphaned when a
// channel is deleted.

func TestYouTubeChannelDeleteAtomic(t *testing.T) {
	s := newYouTubeTestStore(t)

	if err := s.UpsertYouTubeChannel("UC_atomic", "Atomic Test", "", "", "", "", "", 0, 0, "", "", ""); err != nil {
		t.Fatalf("seed channel: %v", err)
	}
	// Seed oauth token row directly (without encryption — the row only needs
	// to exist for FK cascade verification).
	if err := s.UpsertYouTubeOAuthToken("UC_atomic", []byte("enc-access"), []byte("enc-refresh"), "Bearer", "", "", 1); err != nil {
		t.Fatalf("seed oauth token: %v", err)
	}

	groupID, err := s.UpsertYouTubeGroup("Atomic Group", "manager", "", "")
	if err != nil {
		t.Fatalf("seed group: %v", err)
	}
	if err := s.AddChannelToGroup(groupID, "UC_atomic"); err != nil {
		t.Fatalf("seed membership: %v", err)
	}

	// Sanity: all three rows present before the call.
	if row, _ := s.GetYouTubeChannel("UC_atomic"); row == nil {
		t.Fatal("setup: youtube_channels UC_atomic missing")
	}
	if row, _ := s.GetYouTubeOAuthToken("UC_atomic"); row == nil {
		t.Fatal("setup: youtube_oauth_tokens UC_atomic missing")
	}
	memberships, _ := s.ListGroupChannels(groupID)
	if len(memberships) != 1 || memberships[0] != "UC_atomic" {
		t.Fatalf("setup: expected 1 membership for group, got %v", memberships)
	}

	deleted, err := s.DeleteChannelAtomic("UC_atomic")
	if err != nil {
		t.Fatalf("DeleteChannelAtomic: %v", err)
	}
	if deleted != 1 {
		t.Errorf("expected 1 membership deleted, got %d", deleted)
	}

	// Assert: youtube_channels row gone
	if row, _ := s.GetYouTubeChannel("UC_atomic"); row != nil {
		t.Errorf("expected youtube_channels UC_atomic gone, got %v", row)
	}
	// Assert: youtube_oauth_tokens row gone (FK cascade from channels)
	if row, _ := s.GetYouTubeOAuthToken("UC_atomic"); row != nil {
		t.Errorf("expected youtube_oauth_tokens UC_atomic cascade-deleted, got %v", row)
	}
	// Assert: membership row gone
	memberships, _ = s.ListGroupChannels(groupID)
	for _, m := range memberships {
		if m == "UC_atomic" {
			t.Errorf("expected membership UC_atomic to be gone in group %d, still present", groupID)
		}
	}
	// Group row itself remains (we only removed the membership, not the group).
	groups, _ := s.ListYouTubeGroups()
	if len(groups) != 1 {
		t.Errorf("expected group row to remain, total groups = %d", len(groups))
	}
}

func TestYouTubeChannelDeleteAtomicMissingChannel(t *testing.T) {
	s := newYouTubeTestStore(t)

	deleted, err := s.DeleteChannelAtomic("UC_nonexistent")
	if err != nil {
		t.Errorf("expected no error on missing channel, got %v", err)
	}
	if deleted != 0 {
		t.Errorf("expected 0 memberships deleted, got %d", deleted)
	}
}

// ============================================================
// --- YouTubeChannelSeed / ConnectChannelAtomic tests (S5a+) ---
// ============================================================
//
// ConnectChannelAtomic must (a) commit both legs in one SQLite transaction
// on first-time connect (the bug that motivated the method: FK violation
// from upserting into youtube_oauth_tokens before any youtube_channels row
// exists), and (b) on re-auth / re-connect, only overwrite the seed-owned
// metadata columns (title / thumbnail_url / last_sync_at) and preserve
// user-edited typed columns (notes / language / view_count /
// subscriber_count / display_name / channel_url). The tests below pin
// every contract; regressions either way should flip a CI guard.

// TestConnectChannelAtomic_FirstTimeConnect: the parent channel row is
// missing at the moment the method is called. We assert that:
//   - the call returns no error
//   - the youtube_channels row exists with the seeded columns
//   - the youtube_oauth_tokens row exists with the seeded encrypted blobs
//   - the FK on youtube_oauth_tokens is respected: a child row could
//     not have been written had the parent not committed in the same
//     transaction. We then delete the channel row and assert the oauth
//     row was cascade-deleted, confirming the parent-child FK committed
//     through the same transaction.
func TestConnectChannelAtomic_FirstTimeConnect(t *testing.T) {
	s := newYouTubeTestStore(t)

	// Parent row absent at start.
	if row, _ := s.GetYouTubeChannel("UC_first_connect"); row != nil {
		t.Fatalf("setup: expected UC_first_connect absent, got %v", row)
	}

	access := []byte("encrypted-access-bb")
	refresh := []byte("encrypted-refresh-cc")
	expiry := "2027-01-15T00:00:00Z"
	seed := &youtubetypes.YouTubeChannelSeed{
		ChannelID:    "UC_first_connect",
		Title:        "Atomic First Connect",
		DisplayName:  "atomic-channel",
		ChannelURL:   "https://youtube.com/@atomic",
		ThumbnailURL: "https://img.example.com/atomic.jpg",
		Language:     "en",
		Notes:        "first connect notes",
		ViewCount:    100,
		SubCount:     10,
		LastSyncAt:   expiry,
	}
	if err := s.ConnectChannelAtomic(seed, access, refresh, "Bearer", expiry, "scope.read scope.write", 1); err != nil {
		t.Fatalf("ConnectChannelAtomic (first time): %v", err)
	}

	ch, err := s.GetYouTubeChannel("UC_first_connect")
	if err != nil || ch == nil {
		t.Fatalf("expected channel row after atomic upsert: err=%v row=%v", err, ch)
	}
	if ch.Title != "Atomic First Connect" {
		t.Errorf("title: got %v, want %q", ch.Title, "Atomic First Connect")
	}
	if ch.Language != "en" {
		t.Errorf("language: got %v, want %q", ch.Language, "en")
	}

	tok, err := s.GetYouTubeOAuthToken("UC_first_connect")
	if err != nil || tok == nil {
		t.Fatalf("expected oauth row after atomic upsert: err=%v row=%v", err, tok)
	}
	if !bytes.Equal(tok.AccessTokenEncrypted, access) {
		t.Errorf("access blob mismatch: got %v, want %v", tok.AccessTokenEncrypted, access)
	}
	if !bytes.Equal(tok.RefreshTokenEncrypted, refresh) {
		t.Errorf("refresh blob mismatch: got %v, want %v", tok.RefreshTokenEncrypted, refresh)
	}
	if tok.Expiry != expiry {
		t.Errorf("expiry: got %v, want %s", tok.Expiry, expiry)
	}

	// Cascade sanity: deleting the channel row must cascade the oauth
	// row off, confirming the FK was committed atomically with the parent.
	if err := s.DeleteYouTubeChannel("UC_first_connect"); err != nil {
		t.Fatalf("DeleteYouTubeChannel: %v", err)
	}
	if row, _ := s.GetYouTubeOAuthToken("UC_first_connect"); row != nil {
		t.Errorf("expected oauth row cascade-deleted after channel delete, got %v", row)
	}
}

// TestConnectChannelAtomic_PreservesUserEdits: pre-seed the channel row
// with values an operator would set (notes, language, view_count,
// subscriber_count, display_name, channel_url) and a known added_at. Call
// ConnectChannelAtomic again with a NEW title/thumbnail_url/last_sync_at
// (the only columns the OAuth grant can legitimately overwrite). Assert:
//   - title/thumbnail_url/last_sync_at/updated_at are updated
//   - notes/language/view_count/subscriber_count/display_name/channel_url
//     are preserved verbatim
//   - added_at and created_at are preserved (they are NOT in the UPDATE SET)
func TestConnectChannelAtomic_PreservesUserEdits(t *testing.T) {
	s := newYouTubeTestStore(t)

	// Seed an "existing" channel the way an operator would have filled
	// it via the post-OAuth edit path (notes/language/view/sub counts are
	// hand-curated values an operator can set, not OAuth-grant values).
	if err := s.UpsertYouTubeChannel("UC_edits", "Original Title", "Operator Label", "https://youtube.com/@original", "https://img.example.com/orig.jpg", "it", "operator-curated notes", 99999, 5555, "2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z", ""); err != nil {
		t.Fatalf("seed UpsertYouTubeChannel: %v", err)
	}
	// Seed an OAuth blob so the test exercises the FULL atomic path
	// (token upsert included).
	if err := s.UpsertYouTubeOAuthToken("UC_edits", []byte("orig-access"), []byte("orig-refresh"), "Bearer", "2026-01-01T00:00:00Z", "", 1); err != nil {
		t.Fatalf("seed oauth token: %v", err)
	}

	// Snapshot pre-update values for comparison.
	before, err := s.GetYouTubeChannel("UC_edits")
	if err != nil || before == nil {
		t.Fatalf("snapshot read: err=%v row=%v", err, before)
	}
	origAddedAt := before.AddedAt
	origCreatedAt := before.CreatedAt
	origNotes := before.Notes
	origLanguage := before.Language
	origDisplayName := before.DisplayName
	origChannelURL := before.ChannelURL
	origViewCount := before.ViewCount
	origSubCount := before.SubscriberCount
	if origAddedAt == "" {
		t.Fatalf("pre: added_at must be set")
	}

	// Slept to ensure updated_at advances past second-resolution so the
	// lexicographic RFC3339 compare at the end is well-defined.
	time.Sleep(20 * time.Millisecond)

	// New grant arrives with new title / thumbnail / last_sync_at and a
	// different oauth blob. Other seed columns are intentionally noisy /
	// contradictory to make the test fail loudly if any of them leaks
	// into the UPDATE branch.
	newExpiry := "2030-06-30T00:00:00Z"
	seed := &youtubetypes.YouTubeChannelSeed{
		ChannelID:    "UC_edits",
		Title:        "Refreshed By OAuth",
		DisplayName:  "OVERWRITTEN-LABEL-MUST-NOT-WIN",
		ChannelURL:   "https://youtube.com/@overwritten",
		ThumbnailURL: "https://img.example.com/refresh.jpg",
		Language:     "OVERWRITTEN-LANG-MUST-NOT-WIN",
		Notes:        "OVERWRITTEN-NOTES-MUST-NOT-WIN",
		ViewCount:    1,
		SubCount:     1,
		AddedAt:      "OVERWRITTEN-ADDED-MUST-NOT-WIN", // sentinel
		LastSyncAt:   newExpiry,
	}
	newAccess := []byte("new-access-encrypted")
	newRefresh := []byte("new-refresh-encrypted")
	if err := s.ConnectChannelAtomic(seed, newAccess, newRefresh, "Bearer", newExpiry, "scope.read", 1); err != nil {
		t.Fatalf("ConnectChannelAtomic (re-auth): %v", err)
	}

	after, err := s.GetYouTubeChannel("UC_edits")
	if err != nil || after == nil {
		t.Fatalf("post read: err=%v row=%v", err, after)
	}

	// Seed-owned columns SHOULD have been overwritten.
	if after.Title != "Refreshed By OAuth" {
		t.Errorf("title was not updated: got %v, want %q", after.Title, "Refreshed By OAuth")
	}
	if after.ThumbnailURL != "https://img.example.com/refresh.jpg" {
		t.Errorf("thumbnail_url was not updated: got %v, want refreshed thumbnail", after.ThumbnailURL)
	}
	if last := after.LastSyncAt; last != newExpiry {
		t.Errorf("last_sync_at was not updated: got %v, want %s", last, newExpiry)
	}

	// User-edited typed columns SHOULD be preserved verbatim.
	if after.Notes != origNotes {
		t.Errorf("notes was clobbered: got %v, want preserved %q", after.Notes, origNotes)
	}
	if after.Language != origLanguage {
		t.Errorf("language was clobbered: got %v, want preserved %q", after.Language, origLanguage)
	}
	if after.DisplayName != origDisplayName {
		t.Errorf("display_name was clobbered: got %v, want preserved %q", after.DisplayName, origDisplayName)
	}
	if after.ChannelURL != origChannelURL {
		t.Errorf("channel_url was clobbered: got %v, want preserved %q", after.ChannelURL, origChannelURL)
	}
	if got := after.ViewCount; got != origViewCount {
		t.Errorf("view_count was clobbered: got %d, want preserved %d", got, origViewCount)
	}
	if got := after.SubscriberCount; got != origSubCount {
		t.Errorf("subscriber_count was clobbered: got %d, want preserved %d", got, origSubCount)
	}

	// added_at / created_at SHOULD be preserved (neither is in the
	// UPDATE SET clause).
	if after.AddedAt != origAddedAt {
		t.Errorf("added_at was clobbered: got %v, want preserved %s", after.AddedAt, origAddedAt)
	}
	if after.CreatedAt != origCreatedAt {
		t.Errorf("created_at was clobbered: got %v, want preserved %s", after.CreatedAt, origCreatedAt)
	}

	// OAuth blob side: the atomic call SHOULD have updated the new
	// encrypted blobs. (Same channel_id, so the OAuth leg's UPDATE SET
	// applies in full.)
	tok, err := s.GetYouTubeOAuthToken("UC_edits")
	if err != nil || tok == nil {
		t.Fatalf("oauth read after atomic: err=%v", err)
	}
	if !bytes.Equal(tok.AccessTokenEncrypted, newAccess) {
		t.Errorf("oauth access blob not updated: got %v, want %v", tok.AccessTokenEncrypted, newAccess)
	}
	if !bytes.Equal(tok.RefreshTokenEncrypted, newRefresh) {
		t.Errorf("oauth refresh blob not updated: got %v, want %v", tok.RefreshTokenEncrypted, newRefresh)
	}
	if tok.Expiry != newExpiry {
		t.Errorf("oauth expiry not updated: got %v, want %s", tok.Expiry, newExpiry)
	}

	// updated_at must advance past created_at (we slept 20ms above to
	// dodge the sub-second edge case). Lexicographic compare is safe for
	// fixed-width RFC3339 with Z-suffixed times.
	if updatedAfter := after.UpdatedAt; updatedAfter == "" {
		t.Errorf("updated_at is empty after re-auth")
	} else if updatedAfter < origCreatedAt {
		t.Errorf("updated_at did not advance: created_at=%s updated_at=%s", origCreatedAt, updatedAfter)
	}
}

// ============================================================
// TestConnectChannelAtomic_ResetsRevokedAtOnReauth:
// ============================================================
//
// Pinned-issue: a previously-revoked channel that the user re-authenticates
// must NOT be silently filtered by ListActiveYouTubeOAuthTokens after the
// next server restart. Before this fix, the OAuth leg's UPDATE clause did
// not touch revoked_at, so a re-auth flow produced a "row exists, but
// revoked_at != NULL, so loader skips it" limbo. The fix is a single
// `revoked_at = NULL` line in the SQL UPDATE branch; the test pins it.
//
// Outline:
//  1. Seed channel + oauth row in a SQL transaction.
//  2. MarkYouTubeOAuthTokenRevoked → assert revoked_at != "".
//  3. ListActiveYouTubeOAuthTokens → assert channel is omitted (revoked).
//  4. Call ConnectChannelAtomic with a fresh grant (this is the re-auth
//     path the OAuth callback now exercises).
//  5. Assert revoked_at is now "" on the stored row.
//  6. Assert ListActiveYouTubeOAuthTokens now returns the channel.
func TestConnectChannelAtomic_ResetsRevokedAtOnReauth(t *testing.T) {
	s := newYouTubeTestStore(t)

	const channel = "UC_reauth_after_revoke"
	if err := s.UpsertYouTubeChannel(channel, "Reauth Test", "", "", "", "", "", 0, 0, "", "", ""); err != nil {
		t.Fatalf("seed channel: %v", err)
	}
	if err := s.UpsertYouTubeOAuthToken(channel, []byte("orig-access"), []byte("orig-refresh"), "Bearer", "2026-01-01T00:00:00Z", "scope.read", 1); err != nil {
		t.Fatalf("seed oauth: %v", err)
	}

	// Mark revoked; the row must appear "non-active" to the loader.
	if err := s.MarkYouTubeOAuthTokenRevoked(channel); err != nil {
		t.Fatalf("MarkYouTubeOAuthTokenRevoked: %v", err)
	}
	row, _ := s.GetYouTubeOAuthToken(channel)
	if row == nil {
		t.Fatal("setup: expected oauth row present after revoke")
	}
	if row.RevokedAt == "" {
		t.Fatalf("setup: revoked_at empty after MarkYouTubeOAuthTokenRevoked: %v", row)
	}
	active, err := s.ListActiveYouTubeOAuthTokens()
	if err != nil {
		t.Fatalf("ListActive: %v", err)
	}
	for _, r := range active {
		if r.ChannelID == channel {
			t.Fatalf("setup: ListActiveYouTubeOAuthTokens should NOT include a revoked channel; got %v", r)
		}
	}

	// Step 4: explicit re-auth (HandleOAuthCallback -> ConnectChannelAtomic)
	// with a fresh grant.
	newAccess := []byte("reauth-access-encrypted")
	newRefresh := []byte("reauth-refresh-encrypted")
	newExpiry := "2030-09-09T00:00:00Z"
	seed := &youtubetypes.YouTubeChannelSeed{
		ChannelID:    channel,
		Title:        "Channel After Reauth",
		DisplayName:  "",
		ChannelURL:   "",
		ThumbnailURL: "https://img.example.com/reauth.jpg",
		Language:     "",
		Notes:        "",
		ViewCount:    0,
		SubCount:     0,
		AddedAt:      "",
		LastSyncAt:   newExpiry,
	}
	if err := s.ConnectChannelAtomic(seed, newAccess, newRefresh, "Bearer", newExpiry, "scope.read", 1); err != nil {
		t.Fatalf("ConnectChannelAtomic on re-auth: %v", err)
	}

	// Step 5: revoked_at MUST be NULL again.
	row, _ = s.GetYouTubeOAuthToken(channel)
	if row == nil {
		t.Fatal("post: expected oauth row still present after re-auth atomic")
	}
	if row.RevokedAt != "" {
		t.Errorf("revoked_at should be reset on re-auth; got %v", row.RevokedAt)
	}
	if !bytes.Equal(row.AccessTokenEncrypted, newAccess) {
		t.Errorf("access blob not refreshed: got %v, want %v", row.AccessTokenEncrypted, newAccess)
	}
	if !bytes.Equal(row.RefreshTokenEncrypted, newRefresh) {
		t.Errorf("refresh blob not refreshed: got %v, want %v", row.RefreshTokenEncrypted, newRefresh)
	}
	if row.Expiry != newExpiry {
		t.Errorf("expiry not refreshed: got %v, want %s", row.Expiry, newExpiry)
	}

	// Step 6: ListActiveYouTubeOAuthTokens MUST now return the channel.
	active, _ = s.ListActiveYouTubeOAuthTokens()
	found := false
	for _, r := range active {
		if r.ChannelID == channel {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("ListActiveYouTubeOAuthTokens should include the re-authed channel; full list: %v", active)
	}

	// Sanity: the channel-row leg of the atomic should have updated only
	// seed-owned columns (title, thumbnail_url, last_sync_at). User-edited
	// typed columns were never set in this test (everything was empty), so
	// the only assertion we can make is that the channel row's title/thumb
	// got the new values.
	ch, _ := s.GetYouTubeChannel(channel)
	if ch.Title != "Channel After Reauth" {
		t.Errorf("title not updated by re-auth atomic: got %v, want %q", ch.Title, "Channel After Reauth")
	}
	if ch.ThumbnailURL != "https://img.example.com/reauth.jpg" {
		t.Errorf("thumbnail_url not updated by re-auth atomic: got %v", ch.ThumbnailURL)
	}
}
