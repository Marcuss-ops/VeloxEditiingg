package store

import (
	"testing"
)

// ============================================================
// youtube_group_channels tests
// ============================================================
//
// Covers the youtube_group_channels junction table: Add / Remove
// membership, idempotency under (group_id, channel_id) uniqueness,
// position auto-increment, and the join query (ListAllGroupMemberships)
// that pairs group names with their channel rows via JOIN on
// youtube_groups. Position is per-group, not global.

func TestGroupChannelsAddAndList(t *testing.T) {
	s := newYouTubeTestStore(t)

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
	s := newYouTubeTestStore(t)

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
	s := newYouTubeTestStore(t)

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
	s := newYouTubeTestStore(t)

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
	if memberships[0].Position != 0 || memberships[0].ChannelID != "UC_pos1" {
		t.Errorf("expected first position 0 for UC_pos1, got %v", memberships[0])
	}
	if memberships[1].Position != 1 || memberships[1].ChannelID != "UC_pos2" {
		t.Errorf("expected second position 1 for UC_pos2, got %v", memberships[1])
	}
}

func TestGroupChannelsAllMembershipsJoin(t *testing.T) {
	s := newYouTubeTestStore(t)

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
		key := m.GroupName + "/" + m.ChannelID
		found[key] = true
	}
	if !found["Group One/UC_g1a"] || !found["Group One/UC_g1b"] || !found["Group Two/UC_g2a"] {
		t.Errorf("missing expected memberships: got %v", memberships)
	}
}
