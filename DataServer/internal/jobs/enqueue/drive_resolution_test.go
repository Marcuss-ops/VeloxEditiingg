package enqueue

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestExtractDriveFolderIDAcceptsUserScopedURL(t *testing.T) {
	got := extractDriveFolderID("https://drive.google.com/drive/u/2/folders/folder-123?usp=sharing")
	if got != "folder-123" {
		t.Fatalf("folder id = %q, want folder-123", got)
	}
}

func TestResolveDriveOutputFolderReferenceUsesBorrowedDB(t *testing.T) {
	db, err := sql.Open("sqlite3", "file:drive-resolution?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)

	_, err = db.Exec(`CREATE TABLE drive_master_folders (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		url TEXT NOT NULL,
		language TEXT NOT NULL,
		metadata_json TEXT NOT NULL
	)`)
	if err != nil {
		t.Fatalf("create drive_master_folders: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO drive_master_folders (id, name, url, language, metadata_json) VALUES (?, ?, ?, ?, ?)`,
		"folder-rap", "Rap Videos", "https://drive.google.com/drive/folders/folder-rap", "it", `{"alias":"rap"}`); err != nil {
		t.Fatalf("insert drive master folder: %v", err)
	}

	if got := ResolveDriveOutputFolderReference(t.TempDir(), " rap ", db); got != "folder-rap" {
		t.Fatalf("resolved alias = %q, want folder-rap", got)
	}
	// The resolver borrows the DB and must not close or replace it.
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM drive_master_folders`).Scan(&count); err != nil {
		t.Fatalf("borrowed DB unusable after resolution: %v", err)
	}
	if count != 1 {
		t.Fatalf("folder count = %d, want 1", count)
	}
}

func TestResolveDriveOutputFolderReferenceFailsClosed(t *testing.T) {
	const ref = "unknown-folder-alias"
	if got := ResolveDriveOutputFolderReference("/does/not/exist", ref); got != ref {
		t.Fatalf("nil DB result = %q, want original ref %q", got, ref)
	}

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	if got := ResolveDriveOutputFolderReference("", ref, db); got != ref {
		t.Fatalf("missing table result = %q, want original ref %q", got, ref)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := resolveDriveOutputFolderReference(ctx, db, ref); got != ref {
		t.Fatalf("canceled context result = %q, want original ref %q", got, ref)
	}
}

func TestResolveDriveOutputFolderReferenceDirectReferenceDoesNotNeedDB(t *testing.T) {
	for _, ref := range []string{
		"https://drive.google.com/drive/u/2/folders/folder-direct?usp=sharing",
		"folder-id-longer-than-15",
	} {
		if got := ResolveDriveOutputFolderReference("", ref); got == "" {
			t.Fatalf("direct reference %q resolved to empty", ref)
		}
	}
}
