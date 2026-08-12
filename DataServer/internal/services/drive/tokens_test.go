package drive

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestListDriveTokensPropagatesDirectoryErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tokens-file")
	if err := os.WriteFile(path, []byte("not a directory"), 0600); err != nil {
		t.Fatalf("write token path: %v", err)
	}

	service := New(path, t.TempDir(), nil, nil)
	files, err := service.ListDriveTokens()
	if err == nil || !strings.Contains(err.Error(), "list tokens directory") {
		t.Fatalf("ListDriveTokens() error = %v, want directory error", err)
	}
	if files != nil {
		t.Fatalf("ListDriveTokens() files = %v, want nil on error", files)
	}
}

func TestListDriveTokensRequiresDirectory(t *testing.T) {
	service := New("", t.TempDir(), nil, nil)
	files, err := service.ListDriveTokens()
	if err == nil || !strings.Contains(err.Error(), "tokens directory not configured") {
		t.Fatalf("ListDriveTokens() error = %v, want configuration error", err)
	}
	if files != nil {
		t.Fatalf("ListDriveTokens() files = %v, want nil on error", files)
	}
}
