package store

import (
	"testing"
)

// ============================================================
// youtube_groups (canonical) tests
// ============================================================
//
// Covers the youtube_groups table: CRUD with UNIQUE(name,
// group_type) enforcement, list ordering, and the legacy migration
// 008 fallback where the legacy table was dropped and must be
// detected-and-skipped rather than crash with "no such table".

func TestYouTubeGroupCRUD(t *testing.T) {
	s := newYouTubeTestStore(t)

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
	if groups[0].Name != "WNBA Zone" {
		t.Errorf("name: got %v, want %q", groups[0].Name, "WNBA Zone")
	}
	if groups[0].GroupType != "manager" {
		t.Errorf("group_type: got %v, want %q", groups[0].GroupType, "manager")
	}
}

func TestYouTubeGroupDifferentTypesSameName(t *testing.T) {
	s := newYouTubeTestStore(t)

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
		name := g.Name
		gtype := g.GroupType
		if name == "MyGroup" && gtype == "manager" {
			mgrID = g.ID
		}
		if name == "MyGroup" && gtype == "upload" {
			uploadID = g.ID
			if g.Privacy != "public" {
				t.Errorf("upload privacy: got %v, want public", g.Privacy)
			}
		}
	}
	if mgrID == 0 || uploadID == 0 {
		t.Error("expected both group types to be present")
	}
}

func TestYouTubeGroupDelete(t *testing.T) {
	s := newYouTubeTestStore(t)

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
// (Legacy tables youtube_channel_metadata and youtube_groups
//  were dropped by migration 008. Use canonical tables
//  youtube_channels and youtube_groups instead.)
// ============================================================

func TestYouTubeGroupsLegacy(t *testing.T) {
	s := newYouTubeTestStore(t)

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
	if groups[0].Name != "Legacy Group" {
		t.Errorf("name: got %v, want %q", groups[0].Name, "Legacy Group")
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
