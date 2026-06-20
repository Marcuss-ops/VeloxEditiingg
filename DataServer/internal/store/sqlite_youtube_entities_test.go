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

func TestYouTubeChannelCRUD(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

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
	if ch["channel_id"] != "UC_test123" {
		t.Errorf("channel_id: got %v, want UC_test123", ch["channel_id"])
	}
	if ch["title"] != "Test Channel" {
		t.Errorf("title: got %v, want %q", ch["title"], "Test Channel")
	}
	if ch["display_name"] != "Test Display" {
		t.Errorf("display_name: got %v, want %q", ch["display_name"], "Test Display")
	}
	if ch["language"] != "en" {
		t.Errorf("language: got %v, want %q", ch["language"], "en")
	}
	if ch["view_count"] != int64(1500) {
		t.Errorf("view_count: got %v, want 1500", ch["view_count"])
	}
	if ch["subscriber_count"] != int64(500) {
		t.Errorf("subscriber_count: got %v, want 500", ch["subscriber_count"])
	}
}

func TestYouTubeChannelUpdatePreservesAddedAt(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	// First insert with specific added_at
	s.UpsertYouTubeChannel("UC_test456", "Original Title", "", "", "", "", "", 0, 0, "2024-01-15T00:00:00Z", "", "")

	// Update title and stats, pass empty added_at — should preserve original
	s.UpsertYouTubeChannel("UC_test456", "Updated Title", "", "", "", "", "", 200, 100, "", "2024-06-15T00:00:00Z", "")

	ch, err := s.GetYouTubeChannel("UC_test456")
	if err != nil {
		t.Fatalf("GetYouTubeChannel failed: %v", err)
	}
	if ch["title"] != "Updated Title" {
		t.Errorf("title: got %v, want %q", ch["title"], "Updated Title")
	}
	if ch["added_at"] != "2024-01-15T00:00:00Z" {
		t.Errorf("added_at was overwritten: got %v, want 2024-01-15T00:00:00Z", ch["added_at"])
	}
	if ch["last_sync_at"] != "2024-06-15T00:00:00Z" {
		t.Errorf("last_sync_at: got %v, want 2024-06-15T00:00:00Z", ch["last_sync_at"])
	}
}

func TestYouTubeChannelListAndDelete(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

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
	if channels[0]["title"] != "Alpha" || channels[1]["title"] != "Beta" {
		t.Errorf("expected order Alpha, Beta; got %v, %v", channels[0]["title"], channels[1]["title"])
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
	s := openTestDB(t)
	defer s.Close()

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
	if got["title"] != "Refreshed Title" {
		t.Errorf("title: got %v, want Refreshed Title", got["title"])
	}
	if got["thumbnail_url"] != "https://img.example.com/refresh.jpg" {
		t.Errorf("thumbnail_url: got %v, want refreshed thumbnail", got["thumbnail_url"])
	}
	for _, c := range []struct {
		col, want string
	}{
		{"display_name", "Original Display"},
		{"language", "en"},
		{"notes", "user notes"},
		{"channel_url", "https://youtube.com/@orig"},
	} {
		if got[c.col] != c.want {
			t.Errorf("%s was clobbered by refresh: got %v, want %q", c.col, got[c.col], c.want)
		}
	}
	if got["view_count"] != int64(1234) {
		t.Errorf("view_count was clobbered: got %v, want 1234", got["view_count"])
	}
	if got["subscriber_count"] != int64(567) {
		t.Errorf("subscriber_count was clobbered: got %v, want 567", got["subscriber_count"])
	}
	if lastSyncAt, _ := got["last_sync_at"].(string); lastSyncAt == "" {
		t.Errorf("last_sync_at is empty after refresh; want recent RFC3339 timestamp")
	}
}

func TestYouTubeChannelEmptyDefaultValues(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	s.UpsertYouTubeChannel("UC_minimal", "", "", "", "", "", "", 0, 0, "", "", "")

	ch, err := s.GetYouTubeChannel("UC_minimal")
	if err != nil {
		t.Fatalf("GetYouTubeChannel failed: %v", err)
	}
	// title should default to empty string
	if title, ok := ch["title"].(string); !ok || title != "" {
		t.Errorf("title: got %v, want empty string", ch["title"])
	}
	// added_at should be auto-set
	if added, ok := ch["added_at"].(string); !ok || added == "" {
		t.Error("expected added_at to be auto-set")
	}
}

// ============================================================
// youtube_groups (canonical) tests
// ============================================================

func TestYouTubeGroupCRUD(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	// Create a group
	id, err := s.UpsertYouTubeGroup("WNBA Zone", "manager", "WNBA basketball content", "unlisted")
	if err != nil {
		t.Fatalf("UpsertYouTubeGroup failed: %v", err)
	}
	if id <= 0 {
		t.Errorf("expected positive group ID, got %d", id)
	}

	// Get by ID
	gotID, err := s.GetYouTubeGroupID("WNBA Zone", "manager")
	if err != nil {
		t.Fatalf("GetYouTubeGroupID failed: %v", err)
	}
	if gotID != id {
		t.Errorf("ID mismatch: got %d, want %d", gotID, id)
	}

	// List
	groups, err := s.ListYouTubeGroups()
	if err != nil {
		t.Fatalf("ListYouTubeGroups failed: %v", err)
	}
	if len(groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(groups))
	}
	if groups[0]["name"] != "WNBA Zone" {
		t.Errorf("name: got %v, want %q", groups[0]["name"], "WNBA Zone")
	}
	if groups[0]["group_type"] != "manager" {
		t.Errorf("group_type: got %v, want %q", groups[0]["group_type"], "manager")
	}
}

func TestYouTubeGroupDifferentTypesSameName(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	// Same name, different types — allowed by UNIQUE(name, group_type)
	id1, err := s.UpsertYouTubeGroup("MyGroup", "manager", "Manager group", "")
	if err != nil {
		t.Fatalf("first UpsertYouTubeGroup failed: %v", err)
	}
	id2, err := s.UpsertYouTubeGroup("MyGroup", "upload", "Upload group", "public")
	if err != nil {
		t.Fatalf("second UpsertYouTubeGroup failed: %v", err)
	}
	if id1 == id2 {
		t.Error("expected different IDs for different group types")
	}

	groups, err := s.ListYouTubeGroups()
	if err != nil {
		t.Fatalf("ListYouTubeGroups failed: %v", err)
	}
	if len(groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(groups))
	}

	// Verify each type has correct data
	var mgrID, uploadID int64
	for _, g := range groups {
		name := g["name"].(string)
		gtype := g["group_type"].(string)
		if name == "MyGroup" && gtype == "manager" {
			mgrID = g["id"].(int64)
		}
		if name == "MyGroup" && gtype == "upload" {
			uploadID = g["id"].(int64)
			if g["privacy"] != "public" {
				t.Errorf("upload privacy: got %v, want public", g["privacy"])
			}
		}
	}
	if mgrID == 0 || uploadID == 0 {
		t.Error("expected both group types to be present")
	}
}

func TestYouTubeGroupDelete(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	id, _ := s.UpsertYouTubeGroup("DeleteMe", "manager", "", "")
	if err := s.DeleteYouTubeGroup(id); err != nil {
		t.Fatalf("DeleteYouTubeGroup failed: %v", err)
	}

	// Should not be listed
	groups, _ := s.ListYouTubeGroups()
	if len(groups) != 0 {
		t.Errorf("expected 0 groups after delete, got %d", len(groups))
	}
}

// ============================================================
// youtube_group_channels tests
// ============================================================

func TestGroupChannelsAddAndList(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	// Create channel and group
	s.UpsertYouTubeChannel("UC_group_a", "Channel A", "", "", "", "", "", 0, 0, "", "", "")
	groupID, _ := s.UpsertYouTubeGroup("Test Group", "manager", "", "")

	// Add channel to group
	if err := s.AddChannelToGroup(groupID, "UC_group_a"); err != nil {
		t.Fatalf("AddChannelToGroup failed: %v", err)
	}

	// List group channels
	channels, err := s.ListGroupChannels(groupID)
	if err != nil {
		t.Fatalf("ListGroupChannels failed: %v", err)
	}
	if len(channels) != 1 || channels[0] != "UC_group_a" {
		t.Errorf("expected [UC_group_a], got %v", channels)
	}
}

func TestGroupChannelsRemove(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	s.UpsertYouTubeChannel("UC_remove", "Remove Me", "", "", "", "", "", 0, 0, "", "", "")
	groupID, _ := s.UpsertYouTubeGroup("Remove Group", "manager", "", "")
	s.AddChannelToGroup(groupID, "UC_remove")

	// Remove
	if err := s.RemoveChannelFromGroup(groupID, "UC_remove"); err != nil {
		t.Fatalf("RemoveChannelFromGroup failed: %v", err)
	}

	channels, _ := s.ListGroupChannels(groupID)
	if len(channels) != 0 {
		t.Errorf("expected 0 channels after remove, got %d", len(channels))
	}
}

func TestGroupChannelsIdempotentAdd(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	s.UpsertYouTubeChannel("UC_idem", "Idempotent", "", "", "", "", "", 0, 0, "", "", "")
	groupID, _ := s.UpsertYouTubeGroup("Idem Group", "manager", "", "")

	// Add twice — ON CONFLICT DO NOTHING
	s.AddChannelToGroup(groupID, "UC_idem")
	s.AddChannelToGroup(groupID, "UC_idem")

	channels, _ := s.ListGroupChannels(groupID)
	if len(channels) != 1 {
		t.Errorf("expected 1 channel (idempotent), got %d", len(channels))
	}
}

func TestGroupChannelsPositionAutoIncrement(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	s.UpsertYouTubeChannel("UC_pos1", "Pos1", "", "", "", "", "", 0, 0, "", "", "")
	s.UpsertYouTubeChannel("UC_pos2", "Pos2", "", "", "", "", "", 0, 0, "", "", "")
	groupID, _ := s.UpsertYouTubeGroup("Pos Group", "manager", "", "")

	s.AddChannelToGroup(groupID, "UC_pos1")
	s.AddChannelToGroup(groupID, "UC_pos2")

	// List all memberships to verify position
	memberships, err := s.ListAllGroupMemberships()
	if err != nil {
		t.Fatalf("ListAllGroupMemberships failed: %v", err)
	}
	if len(memberships) != 2 {
		t.Fatalf("expected 2 memberships, got %d", len(memberships))
	}
	// Position should increment (0, 1)
	if memberships[0]["position"] != 0 || memberships[0]["channel_id"] != "UC_pos1" {
		t.Errorf("expected first position 0 for UC_pos1, got %v", memberships[0])
	}
	if memberships[1]["position"] != 1 || memberships[1]["channel_id"] != "UC_pos2" {
		t.Errorf("expected second position 1 for UC_pos2, got %v", memberships[1])
	}
}

func TestGroupChannelsAllMembershipsJoin(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	// Two groups with channels
	s.UpsertYouTubeChannel("UC_g1a", "G1A", "", "", "", "", "", 0, 0, "", "", "")
	s.UpsertYouTubeChannel("UC_g1b", "G1B", "", "", "", "", "", 0, 0, "", "", "")
	s.UpsertYouTubeChannel("UC_g2a", "G2A", "", "", "", "", "", 0, 0, "", "", "")

	g1, _ := s.UpsertYouTubeGroup("Group One", "manager", "", "")
	g2, _ := s.UpsertYouTubeGroup("Group Two", "upload", "", "")

	s.AddChannelToGroup(g1, "UC_g1a")
	s.AddChannelToGroup(g1, "UC_g1b")
	s.AddChannelToGroup(g2, "UC_g2a")

	memberships, err := s.ListAllGroupMemberships()
	if err != nil {
		t.Fatalf("ListAllGroupMemberships failed: %v", err)
	}
	if len(memberships) != 3 {
		t.Fatalf("expected 3 memberships, got %d", len(memberships))
	}

	// Verify group names in join results
	found := map[string]bool{}
	for _, m := range memberships {
		key := m["group_name"].(string) + "/" + m["channel_id"].(string)
		found[key] = true
	}
	if !found["Group One/UC_g1a"] || !found["Group One/UC_g1b"] || !found["Group Two/UC_g2a"] {
		t.Errorf("missing expected memberships: got %v", memberships)
	}
}

// ============================================================
// youtube_tracked_niches tests
// ============================================================

func TestTrackedNichesAddAndList(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

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
	s := openTestDB(t)
	defer s.Close()

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
	s := openTestDB(t)
	defer s.Close()

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

func TestYouTubeCacheSetGet(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

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
	s := openTestDB(t)
	defer s.Close()

	ts, data, err := s.GetYouTubeCache("nonexistent")
	if err != nil {
		t.Fatalf("GetYouTubeCache(miss) failed: %v", err)
	}
	if ts != 0 || data != "" {
		t.Errorf("expected zero value on miss, got ts=%d data=%q", ts, data)
	}
}

func TestYouTubeCacheUpdate(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

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
	s := openTestDB(t)
	defer s.Close()

	s.SetYouTubeCache("stale", 1000, `{"stale": true}`)
	s.SetYouTubeCache("fresh", 90000, `{"fresh": true}`)

	// Cleanup uses time.Now().Unix() internally, so both entries will be deleted
	// since their timestamps (1000, 90000) are in the past.
	_, err := s.CleanupYouTubeCache(50000)
	if err != nil {
		t.Fatalf("CleanupYouTubeCache failed: %v", err)
	}
}

func TestYouTubeOAuthTokenUpsertGetRevoke(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()
	// Ensure FK is enforced for the cascade test below, since the helper
	// in this file does not always turn foreign_keys on.
	_, _ = s.db.Exec("PRAGMA foreign_keys = ON")

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
	if row["channel_id"] != "UC_oauth_test" {
		t.Errorf("channel_id: got %v, want UC_oauth_test", row["channel_id"])
	}
	if !bytes.Equal(row["access_token_encrypted"].([]byte), access1) {
		t.Errorf("access blob not stored verbatim: got %v, want %v", row["access_token_encrypted"], access1)
	}
	if !bytes.Equal(row["refresh_token_encrypted"].([]byte), refresh1) {
		t.Errorf("refresh blob not stored verbatim: got %v, want %v", row["refresh_token_encrypted"], refresh1)
	}
	if row["key_version"] != int64(1) {
		t.Errorf("key_version: got %v, want 1", row["key_version"])
	}
	if row["revoked_at"] != "" {
		t.Errorf("revoked_at should start empty, got %v", row["revoked_at"])
	}

	// Upsert path (replace blobs and bump updated_at)
	access2 := []byte("encrypted-access-2")
	refresh2 := []byte("encrypted-refresh-2")
	if err := s.UpsertYouTubeOAuthToken("UC_oauth_test", access2, refresh2, "Bearer", "2027-01-01T00:00:00Z", "scope.read", 1); err != nil {
		t.Fatalf("second UpsertYouTubeOAuthToken: %v", err)
	}
	row, _ = s.GetYouTubeOAuthToken("UC_oauth_test")
	if !bytes.Equal(row["access_token_encrypted"].([]byte), access2) {
		t.Error("access blob not updated on second upsert")
	}
	if !bytes.Equal(row["refresh_token_encrypted"].([]byte), refresh2) {
		t.Error("refresh blob not updated on second upsert")
	}
	if row["expiry"] != "2027-01-01T00:00:00Z" {
		t.Errorf("expiry: got %v, want 2027-01-01T00:00:00Z", row["expiry"])
	}
	if row["revoked_at"] != "" {
		t.Error("upsert must not touch revoked_at")
	}

	// MarkRevoked stamps a timestamp; idempotent on re-call.
	if err := s.MarkYouTubeOAuthTokenRevoked("UC_oauth_test"); err != nil {
		t.Fatalf("MarkYouTubeOAuthTokenRevoked: %v", err)
	}
	row, _ = s.GetYouTubeOAuthToken("UC_oauth_test")
	if row["revoked_at"] == "" || row["revoked_at"] == nil {
		t.Error("revoked_at should be set after MarkRevoked")
	}
	prevRevoked := row["revoked_at"]
	time.Sleep(20 * time.Millisecond)
	if err := s.MarkYouTubeOAuthTokenRevoked("UC_oauth_test"); err != nil {
		t.Fatalf("second MarkRevoked: %v", err)
	}
	row2, _ := s.GetYouTubeOAuthToken("UC_oauth_test")
	if row2["revoked_at"] != prevRevoked {
		t.Errorf("second MarkRevoked changed revoked_at: before=%v, after=%v", prevRevoked, row2["revoked_at"])
	}
}

func TestYouTubeOAuthTokenMissingChannel(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	row, err := s.GetYouTubeOAuthToken("UC_nonexistent")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if row != nil {
		t.Errorf("expected nil row for missing channel, got %v", row)
	}
}

func TestYouTubeOAuthTokenChannelFKDeleteCascade(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()
	_, _ = s.db.Exec("PRAGMA foreign_keys = ON")

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

func TestYouTubeChannelDeleteAtomic(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()
	_, _ = s.db.Exec("PRAGMA foreign_keys = ON")

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
	s := openTestDB(t)
	defer s.Close()
	_, _ = s.db.Exec("PRAGMA foreign_keys = ON")

	deleted, err := s.DeleteChannelAtomic("UC_nonexistent")
	if err != nil {
		t.Errorf("expected no error on missing channel, got %v", err)
	}
	if deleted != 0 {
		t.Errorf("expected 0 memberships deleted, got %d", deleted)
	}
}

func TestYouTubeCacheClear(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

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
// --- YouTubeChannelSeed / ConnectChannelAtomic tests (S5a+) ---
// ============================================================
//
// ConnectChannelAtomic must (a) commit both legs in one SQLite transaction
// on first-time connect (the bug that motivated the method: FK violation
// from upserting into youtube_oauth_tokens before any youtube_channels row
// exists), and (b) on re-auth / re-connect, only overwrite the seed-owned
// metadata columns (title / thumbnail_url / last_sync_at) and preserve
// user-edited typed columns (notes / language / view_count /
// subscriber_count / display_name / channel_url). These two tests pin
// both contracts; regressions either way should flip a CI guard.

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
	s := openTestDB(t)
	defer s.Close()
	// Force FK = ON so we exercise the missing-parent failure mode
	// the method is meant to repair.
	if _, err := s.db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("enable FK pragma: %v", err)
	}

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
	if ch["title"] != "Atomic First Connect" {
		t.Errorf("title: got %v, want %q", ch["title"], "Atomic First Connect")
	}
	if ch["language"] != "en" {
		t.Errorf("language: got %v, want %q", ch["language"], "en")
	}

	tok, err := s.GetYouTubeOAuthToken("UC_first_connect")
	if err != nil || tok == nil {
		t.Fatalf("expected oauth row after atomic upsert: err=%v row=%v", err, tok)
	}
	if !bytes.Equal(tok["access_token_encrypted"].([]byte), access) {
		t.Errorf("access blob mismatch: got %v, want %v", tok["access_token_encrypted"], access)
	}
	if !bytes.Equal(tok["refresh_token_encrypted"].([]byte), refresh) {
		t.Errorf("refresh blob mismatch: got %v, want %v", tok["refresh_token_encrypted"], refresh)
	}
	if tok["expiry"] != expiry {
		t.Errorf("expiry: got %v, want %s", tok["expiry"], expiry)
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
	s := openTestDB(t)
	defer s.Close()
	if _, err := s.db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("enable FK pragma: %v", err)
	}

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
	origAddedAt, _ := before["added_at"].(string)
	origCreatedAt, _ := before["created_at"].(string)
	origNotes, _ := before["notes"].(string)
	origLanguage, _ := before["language"].(string)
	origDisplayName, _ := before["display_name"].(string)
	origChannelURL, _ := before["channel_url"].(string)
	origViewCount := before["view_count"].(int64)
	origSubCount := before["subscriber_count"].(int64)
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
	if after["title"] != "Refreshed By OAuth" {
		t.Errorf("title was not updated: got %v, want %q", after["title"], "Refreshed By OAuth")
	}
	if after["thumbnail_url"] != "https://img.example.com/refresh.jpg" {
		t.Errorf("thumbnail_url was not updated: got %v, want refreshed thumbnail", after["thumbnail_url"])
	}
	if last, _ := after["last_sync_at"].(string); last != newExpiry {
		t.Errorf("last_sync_at was not updated: got %v, want %s", last, newExpiry)
	}

	// User-edited typed columns SHOULD be preserved verbatim.
	if after["notes"] != origNotes {
		t.Errorf("notes was clobbered: got %v, want preserved %q", after["notes"], origNotes)
	}
	if after["language"] != origLanguage {
		t.Errorf("language was clobbered: got %v, want preserved %q", after["language"], origLanguage)
	}
	if after["display_name"] != origDisplayName {
		t.Errorf("display_name was clobbered: got %v, want preserved %q", after["display_name"], origDisplayName)
	}
	if after["channel_url"] != origChannelURL {
		t.Errorf("channel_url was clobbered: got %v, want preserved %q", after["channel_url"], origChannelURL)
	}
	if got := after["view_count"].(int64); got != origViewCount {
		t.Errorf("view_count was clobbered: got %d, want preserved %d", got, origViewCount)
	}
	if got := after["subscriber_count"].(int64); got != origSubCount {
		t.Errorf("subscriber_count was clobbered: got %d, want preserved %d", got, origSubCount)
	}

	// added_at / created_at SHOULD be preserved (neither is in the
	// UPDATE SET clause).
	if after["added_at"] != origAddedAt {
		t.Errorf("added_at was clobbered: got %v, want preserved %s", after["added_at"], origAddedAt)
	}
	if after["created_at"] != origCreatedAt {
		t.Errorf("created_at was clobbered: got %v, want preserved %s", after["created_at"], origCreatedAt)
	}

	// OAuth blob side: the atomic call SHOULD have updated the new
	// encrypted blobs. (Same channel_id, so the OAuth leg's UPDATE SET
	// applies in full.)
	tok, err := s.GetYouTubeOAuthToken("UC_edits")
	if err != nil || tok == nil {
		t.Fatalf("oauth read after atomic: err=%v", err)
	}
	if !bytes.Equal(tok["access_token_encrypted"].([]byte), newAccess) {
		t.Errorf("oauth access blob not updated: got %v, want %v", tok["access_token_encrypted"], newAccess)
	}
	if !bytes.Equal(tok["refresh_token_encrypted"].([]byte), newRefresh) {
		t.Errorf("oauth refresh blob not updated: got %v, want %v", tok["refresh_token_encrypted"], newRefresh)
	}
	if tok["expiry"] != newExpiry {
		t.Errorf("oauth expiry not updated: got %v, want %s", tok["expiry"], newExpiry)
	}

	// updated_at must advance past created_at (we slept 20ms above to
	// dodge the sub-second edge case). Lexicographic compare is safe for
	// fixed-width RFC3339 with Z-suffixed times.
	if updatedAfter, _ := after["updated_at"].(string); updatedAfter == "" {
		t.Errorf("updated_at is empty after re-auth")
	} else if updatedAfter < origCreatedAt {
		t.Errorf("updated_at did not advance: created_at=%s updated_at=%s", origCreatedAt, updatedAfter)
	}
}

// ============================================================
// TestUpsertYouTubeOAuthToken_PreservesRevokedAt
// ============================================================
//
// Converse pin to TestConnectChannelAtomic_ResetsRevokedAtOnReauth.
// The OAuth-leg UPDATE in ConnectChannelAtomic resets revoked_at to
// NULL because that path is the explicit new-auth flow (a user
// redoing OAuth must be reactivated). The auto-refresh path goes
// through UpsertYouTubeOAuthToken, which must NOT reset revoked_at
// — otherwise a routine token refresh would silently un-revoke a
// credential an operator explicitly revoked. This test pins that
// invariant by calling UpsertYouTubeOAuthToken on a previously-
// revoked channel and asserting revoked_at is preserved verbatim.
func TestUpsertYouTubeOAuthToken_PreservesRevokedAt(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()
	_, _ = s.db.Exec("PRAGMA foreign_keys = ON")

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
	originalRevokedAt, _ := row["revoked_at"].(string)
	if originalRevokedAt == "" {
		t.Fatal("setup: revoked_at empty after MarkRevoked")
	}
	originalUpdatedAt, _ := row["updated_at"].(string)
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
	if gotRevokedAt, _ := row["revoked_at"].(string); gotRevokedAt != originalRevokedAt {
		t.Errorf("revoked_at was reset on refresh path (was %q, now %q); auto-refresh MUST NOT un-revoke",
			originalRevokedAt, gotRevokedAt)
	}

	// Sanity: the access blob did get rotated (this is the whole point
	// of the refresh), so the test is exercising the right call.
	if !bytes.Equal(row["access_token_encrypted"].([]byte), newAccess) {
		t.Errorf("access blob not rotated by refresh: got %v, want %v", row["access_token_encrypted"], newAccess)
	}
	if !bytes.Equal(row["refresh_token_encrypted"].([]byte), newRefresh) {
		t.Errorf("refresh blob not rotated by refresh: got %v, want %v", row["refresh_token_encrypted"], newRefresh)
	}
	if row["expiry"] != "2030-12-31T23:59:59Z" {
		t.Errorf("expiry not updated by refresh: got %v, want 2030-12-31T23:59:59Z", row["expiry"])
	}
	if upd, _ := row["updated_at"].(string); upd == "" {
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
		if r["channel_id"] == channel {
			t.Errorf("ListActiveYouTubeOAuthTokens should still skip the revoked-and-refreshed channel; got %v", r)
			break
		}
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
	s := openTestDB(t)
	defer s.Close()
	_, _ = s.db.Exec("PRAGMA foreign_keys = ON")

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
	if row["revoked_at"] == "" {
		t.Fatalf("setup: revoked_at empty after MarkYouTubeOAuthTokenRevoked: %v", row)
	}
	active, err := s.ListActiveYouTubeOAuthTokens()
	if err != nil {
		t.Fatalf("ListActive: %v", err)
	}
	for _, r := range active {
		if r["channel_id"] == channel {
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
	if row["revoked_at"] != "" {
		t.Errorf("revoked_at should be reset on re-auth; got %v", row["revoked_at"])
	}
	if !bytes.Equal(row["access_token_encrypted"].([]byte), newAccess) {
		t.Errorf("access blob not refreshed: got %v, want %v", row["access_token_encrypted"], newAccess)
	}
	if !bytes.Equal(row["refresh_token_encrypted"].([]byte), newRefresh) {
		t.Errorf("refresh blob not refreshed: got %v, want %v", row["refresh_token_encrypted"], newRefresh)
	}
	if row["expiry"] != newExpiry {
		t.Errorf("expiry not refreshed: got %v, want %s", row["expiry"], newExpiry)
	}

	// Step 6: ListActiveYouTubeOAuthTokens MUST now return the channel.
	active, _ = s.ListActiveYouTubeOAuthTokens()
	found := false
	for _, r := range active {
		if r["channel_id"] == channel {
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
	if ch["title"] != "Channel After Reauth" {
		t.Errorf("title not updated by re-auth atomic: got %v, want %q", ch["title"], "Channel After Reauth")
	}
	if ch["thumbnail_url"] != "https://img.example.com/reauth.jpg" {
		t.Errorf("thumbnail_url not updated by re-auth atomic: got %v", ch["thumbnail_url"])
	}
}

// ============================================================
// (Legacy tables youtube_channel_metadata and youtube_groups
//  were dropped by migration 008. Use canonical tables
//  youtube_channels and youtube_groups instead.)
// ============================================================

func TestYouTubeGroupsLegacy(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	// Skip if legacy table doesn't exist (dropped by migration 008)
	var exists int
	_ = s.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='youtube_groups'`).Scan(&exists)
	if exists == 0 {
		t.Skip("youtube_groups table dropped by migration 008")
	}

	// Create via canonical UpsertYouTubeGroup
	groupID, err := s.UpsertYouTubeGroup("Legacy Group", "manager", "Old description", "public")
	if err != nil {
		t.Fatalf("UpsertYouTubeGroup failed: %v", err)
	}
	_ = s.AddChannelToGroup(groupID, "UC_a")
	_ = s.AddChannelToGroup(groupID, "UC_b")

	groups, err := s.ListYouTubeGroups()
	if err != nil {
		t.Fatalf("ListYouTubeGroups failed: %v", err)
	}
	if len(groups) != 1 {
		t.Fatalf("expected 1 legacy group, got %d", len(groups))
	}
	if groups[0]["name"] != "Legacy Group" {
		t.Errorf("name: got %v, want %q", groups[0]["name"], "Legacy Group")
	}

	// Delete via canonical DeleteYouTubeGroup
	if err := s.DeleteYouTubeGroup(groupID); err != nil {
		t.Fatalf("DeleteYouTubeGroup failed: %v", err)
	}
	groups, _ = s.ListYouTubeGroups()
	if len(groups) != 0 {
		t.Errorf("expected 0 groups after delete, got %d", len(groups))
	}
}
