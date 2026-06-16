package store

import (
	"bytes"
	"testing"
	"time"
)

// ============================================================
// youtube_channels (canonical) tests
// ============================================================

func TestYouTubeChannelCRUD(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	err := s.UpsertYouTubeChannel("UC_test123", "Test Channel", "Test Display", "https://youtube.com/@test",
		"https://img.example.com/thumb.jpg", "en", "A test channel", 1500, 500,
		"2024-01-01T00:00:00Z", "2024-06-01T00:00:00Z", `{"source": "import"}`)
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
	s.UpsertYouTubeChannel("UC_test456", "Original Title", "", "", "", "", "", 0, 0,
		"2024-01-15T00:00:00Z", "", `{}`)

	// Update title and stats, pass empty added_at — should preserve original
	s.UpsertYouTubeChannel("UC_test456", "Updated Title", "", "", "", "", "", 200, 100,
		"", "2024-06-15T00:00:00Z", `{"updated": true}`)

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

	// Insert two channels
	s.UpsertYouTubeChannel("UC_a", "Alpha", "", "", "", "", "", 0, 0, "", "", `{}`)
	s.UpsertYouTubeChannel("UC_b", "Beta", "", "", "", "", "", 0, 0, "", "", `{}`)

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
	// channel_url, view_count, subscriber_count, metadata_json) MUST
	// NOT be touched.
	if err := s.UpsertYouTubeChannel(
		"UC_refresh_test", "Original Title", "Original Display",
		"https://youtube.com/@orig", "https://img.example.com/orig.jpg",
		"en", "user notes", 1234, 567,
		"2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z", `{"user_meta": true}`,
	); err != nil {
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
	if got["metadata_json"] != `{"user_meta": true}` {
		t.Errorf("metadata_json was clobbered: got %v, want preserved user_meta JSON", got["metadata_json"])
	}
	if lastSyncAt, _ := got["last_sync_at"].(string); lastSyncAt == "" {
		t.Errorf("last_sync_at is empty after refresh; want recent RFC3339 timestamp")
	}
}

func TestYouTubeChannelEmptyDefaultValues(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	// Insert with minimal fields
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
// youtube_groups_v2 tests
// ============================================================

func TestYouTubeGroupV2CRUD(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	// Create a group
	id, err := s.UpsertYouTubeGroupV2("WNBA Zone", "manager", "WNBA basketball content", "unlisted")
	if err != nil {
		t.Fatalf("UpsertYouTubeGroupV2 failed: %v", err)
	}
	if id <= 0 {
		t.Errorf("expected positive group ID, got %d", id)
	}

	// Get by ID
	gotID, err := s.GetYouTubeGroupV2ID("WNBA Zone", "manager")
	if err != nil {
		t.Fatalf("GetYouTubeGroupV2ID failed: %v", err)
	}
	if gotID != id {
		t.Errorf("ID mismatch: got %d, want %d", gotID, id)
	}

	// List
	groups, err := s.ListYouTubeGroupsV2()
	if err != nil {
		t.Fatalf("ListYouTubeGroupsV2 failed: %v", err)
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

func TestYouTubeGroupV2DifferentTypesSameName(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	// Same name, different types — allowed by UNIQUE(name, group_type)
	id1, err := s.UpsertYouTubeGroupV2("MyGroup", "manager", "Manager group", "")
	if err != nil {
		t.Fatalf("first UpsertYouTubeGroupV2 failed: %v", err)
	}
	id2, err := s.UpsertYouTubeGroupV2("MyGroup", "upload", "Upload group", "public")
	if err != nil {
		t.Fatalf("second UpsertYouTubeGroupV2 failed: %v", err)
	}
	if id1 == id2 {
		t.Error("expected different IDs for different group types")
	}

	groups, err := s.ListYouTubeGroupsV2()
	if err != nil {
		t.Fatalf("ListYouTubeGroupsV2 failed: %v", err)
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

func TestYouTubeGroupV2Delete(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	id, _ := s.UpsertYouTubeGroupV2("DeleteMe", "manager", "", "")
	if err := s.DeleteYouTubeGroupV2(id); err != nil {
		t.Fatalf("DeleteYouTubeGroupV2 failed: %v", err)
	}

	// Should not be listed
	groups, _ := s.ListYouTubeGroupsV2()
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
	s.UpsertYouTubeChannel("UC_group_a", "Channel A", "", "", "", "", "", 0, 0, "", "", `{}`)
	groupID, _ := s.UpsertYouTubeGroupV2("Test Group", "manager", "", "")

	// Add channel to group
	if err := s.AddChannelToGroupV2(groupID, "UC_group_a"); err != nil {
		t.Fatalf("AddChannelToGroupV2 failed: %v", err)
	}

	// List group channels
	channels, err := s.ListGroupChannelsV2(groupID)
	if err != nil {
		t.Fatalf("ListGroupChannelsV2 failed: %v", err)
	}
	if len(channels) != 1 || channels[0] != "UC_group_a" {
		t.Errorf("expected [UC_group_a], got %v", channels)
	}
}

func TestGroupChannelsRemove(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	s.UpsertYouTubeChannel("UC_remove", "Remove Me", "", "", "", "", "", 0, 0, "", "", `{}`)
	groupID, _ := s.UpsertYouTubeGroupV2("Remove Group", "manager", "", "")
	s.AddChannelToGroupV2(groupID, "UC_remove")

	// Remove
	if err := s.RemoveChannelFromGroupV2(groupID, "UC_remove"); err != nil {
		t.Fatalf("RemoveChannelFromGroupV2 failed: %v", err)
	}

	channels, _ := s.ListGroupChannelsV2(groupID)
	if len(channels) != 0 {
		t.Errorf("expected 0 channels after remove, got %d", len(channels))
	}
}

func TestGroupChannelsIdempotentAdd(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	s.UpsertYouTubeChannel("UC_idem", "Idempotent", "", "", "", "", "", 0, 0, "", "", `{}`)
	groupID, _ := s.UpsertYouTubeGroupV2("Idem Group", "manager", "", "")

	// Add twice — ON CONFLICT DO NOTHING
	s.AddChannelToGroupV2(groupID, "UC_idem")
	s.AddChannelToGroupV2(groupID, "UC_idem")

	channels, _ := s.ListGroupChannelsV2(groupID)
	if len(channels) != 1 {
		t.Errorf("expected 1 channel (idempotent), got %d", len(channels))
	}
}

func TestGroupChannelsPositionAutoIncrement(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	s.UpsertYouTubeChannel("UC_pos1", "Pos1", "", "", "", "", "", 0, 0, "", "", `{}`)
	s.UpsertYouTubeChannel("UC_pos2", "Pos2", "", "", "", "", "", 0, 0, "", "", `{}`)
	groupID, _ := s.UpsertYouTubeGroupV2("Pos Group", "manager", "", "")

	s.AddChannelToGroupV2(groupID, "UC_pos1")
	s.AddChannelToGroupV2(groupID, "UC_pos2")

	// List all memberships to verify position
	memberships, err := s.ListAllGroupMembershipsV2()
	if err != nil {
		t.Fatalf("ListAllGroupMembershipsV2 failed: %v", err)
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
	s.UpsertYouTubeChannel("UC_g1a", "G1A", "", "", "", "", "", 0, 0, "", "", `{}`)
	s.UpsertYouTubeChannel("UC_g1b", "G1B", "", "", "", "", "", 0, 0, "", "", `{}`)
	s.UpsertYouTubeChannel("UC_g2a", "G2A", "", "", "", "", "", 0, 0, "", "", `{}`)

	g1, _ := s.UpsertYouTubeGroupV2("Group One", "manager", "", "")
	g2, _ := s.UpsertYouTubeGroupV2("Group Two", "upload", "", "")

	s.AddChannelToGroupV2(g1, "UC_g1a")
	s.AddChannelToGroupV2(g1, "UC_g1b")
	s.AddChannelToGroupV2(g2, "UC_g2a")

	memberships, err := s.ListAllGroupMembershipsV2()
	if err != nil {
		t.Fatalf("ListAllGroupMembershipsV2 failed: %v", err)
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
	if err := s.UpsertYouTubeChannel("UC_oauth_test", "OAuth Test", "", "", "", "", "", 0, 0, "", "", `{}`); err != nil {
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

	if err := s.UpsertYouTubeChannel("UC_cascade", "", "", "", "", "", "", 0, 0, "", "", `{}`); err != nil {
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

	if err := s.UpsertYouTubeChannel("UC_atomic", "Atomic Test", "", "", "", "", "", 0, 0, "", "", `{}`); err != nil {
		t.Fatalf("seed channel: %v", err)
	}
	// Seed oauth token row directly (without encryption — the row only needs
	// to exist for FK cascade verification).
	if err := s.UpsertYouTubeOAuthToken("UC_atomic", []byte("enc-access"), []byte("enc-refresh"), "Bearer", "", "", 1); err != nil {
		t.Fatalf("seed oauth token: %v", err)
	}

	groupID, err := s.UpsertYouTubeGroupV2("Atomic Group", "manager", "", "")
	if err != nil {
		t.Fatalf("seed group: %v", err)
	}
	if err := s.AddChannelToGroupV2(groupID, "UC_atomic"); err != nil {
		t.Fatalf("seed membership: %v", err)
	}

	// Sanity: all three rows present before the call.
	if row, _ := s.GetYouTubeChannel("UC_atomic"); row == nil {
		t.Fatal("setup: youtube_channels UC_atomic missing")
	}
	if row, _ := s.GetYouTubeOAuthToken("UC_atomic"); row == nil {
		t.Fatal("setup: youtube_oauth_tokens UC_atomic missing")
	}
	memberships, _ := s.ListGroupChannelsV2(groupID)
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
	memberships, _ = s.ListGroupChannelsV2(groupID)
	for _, m := range memberships {
		if m == "UC_atomic" {
			t.Errorf("expected membership UC_atomic to be gone in group %d, still present", groupID)
		}
	}
	// Group row itself remains (we only removed the membership, not the group).
	groups, _ := s.ListYouTubeGroupsV2()
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
// (Legacy tables youtube_channel_metadata and youtube_groups
//  were dropped by migration 008. Use canonical tables
//  youtube_channels and youtube_groups_v2 instead.)
// ============================================================
