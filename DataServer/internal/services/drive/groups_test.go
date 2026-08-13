package drive

import "testing"

func TestIndexMasterFoldersByNormalizedNameEquivalentToLinearScan(t *testing.T) {
	folders := []DriveFolder{
		{ID: "root-wwe", Name: "WWE", ParentID: ""},
		{ID: "child-wwe", Name: "WWE", ParentID: "root-wwe"},
		{ID: "root-hiphop", Name: "Hip Hop", ParentID: ""},
		{ID: "root-news", Name: "News", ParentID: ""},
		{ID: "root-tech", Name: "Tech", ParentID: ""},
		{ID: "root-voice", Name: "WWE Voice", ParentID: ""},
		{ID: "root-stock", Name: "WWE Stock", ParentID: ""},
		{ID: "root-punct", Name: "!!!", ParentID: ""}, // normalizes to "" — quirk preserved
		{ID: "root-dup", Name: "Tech", ParentID: ""},  // duplicate normalized name, first wins
	}

	index := indexMasterFoldersByNormalizedName(folders)

	cases := [][]string{
		{"WWE"},
		{"wwe"},
		{"Hip Hop", "hiphop"},
		{"missing", "News"},
		{"Tech"},
		{"Stock", "WWE Stock"},
		{"!!!", "News"}, // empty normalized alias matches the empty-key folder (quirk preserved)
		{"nothing", "absent"},
		{},
	}

	for _, names := range cases {
		want := findMasterIDByName(folders, names)
		got := findMasterIDInIndex(index, names...)
		if got != want {
			t.Errorf("names=%v: index lookup = %q, want linear scan = %q", names, got, want)
		}
	}
}

func TestIndexMasterFoldersByNormalizedNameSkipsChildren(t *testing.T) {
	folders := []DriveFolder{
		{ID: "parent", Name: "Parent", ParentID: ""},
		{ID: "child", Name: "Child", ParentID: "parent"},
	}

	index := indexMasterFoldersByNormalizedName(folders)

	if id := index["child"]; id != "" {
		t.Fatalf("child folder indexed under normalized name, want only root folders: %q", id)
	}
	if id := index["parent"]; id != "parent" {
		t.Fatalf("root folder index = %q, want parent", id)
	}
}

func TestFindMasterIDInIndexFirstMatchWins(t *testing.T) {
	folders := []DriveFolder{
		{ID: "first", Name: "Tech", ParentID: ""},
		{ID: "second", Name: "Tech", ParentID: ""},
	}
	index := indexMasterFoldersByNormalizedName(folders)
	if got := findMasterIDInIndex(index, "tech"); got != "first" {
		t.Fatalf("first-match lookup = %q, want first", got)
	}
}
