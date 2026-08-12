package drive

import (
	"path/filepath"
	"testing"

	"velox-server/internal/store"
)

func TestDriveReadModelsFailClosedOnStoreErrors(t *testing.T) {
	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "drive.sqlite"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	service := New("", t.TempDir(), nil, db)
	if folders, err := service.GetDriveLinks(); err == nil || folders != nil {
		t.Fatalf("GetDriveLinks() = (%v, %v), want nil and store error", folders, err)
	}
	if groups, err := service.GetDriveGroups(); err == nil || groups != nil {
		t.Fatalf("GetDriveGroups() = (%v, %v), want nil and store error", groups, err)
	}
	if masters, err := service.GetMasterFolders(); err == nil || masters != nil {
		t.Fatalf("GetMasterFolders() = (%v, %v), want nil and store error", masters, err)
	}
}

func TestDriveReadModelsRejectMissingStore(t *testing.T) {
	service := New("", t.TempDir(), nil, nil)
	if folders, err := service.GetDriveLinks(); err == nil || folders != nil {
		t.Fatalf("GetDriveLinks() = (%v, %v), want nil and configuration error", folders, err)
	}
	if masters, err := service.GetMasterFolders(); err == nil || masters != nil {
		t.Fatalf("GetMasterFolders() = (%v, %v), want nil and configuration error", masters, err)
	}
}
