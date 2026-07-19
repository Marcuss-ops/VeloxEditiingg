package enqueue

import "testing"

func TestExtractDriveFolderIDAcceptsUserScopedURL(t *testing.T) {
	got := extractDriveFolderID("https://drive.google.com/drive/u/2/folders/folder-123?usp=sharing")
	if got != "folder-123" {
		t.Fatalf("folder id = %q, want folder-123", got)
	}
}
