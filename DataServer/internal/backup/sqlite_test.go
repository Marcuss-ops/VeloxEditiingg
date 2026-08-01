package backup

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"velox-server/internal/store"

	_ "github.com/mattn/go-sqlite3"
)

func TestBackupSQLiteAndRestoreSQLite(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	source := filepath.Join(dir, "velox.db")
	s, err := store.NewSQLiteStore(source)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer s.Close()

	backupPath := filepath.Join(dir, "snapshots", "velox.db")
	got, err := BackupSQLite(ctx, s.DB(), backupPath)
	if err != nil {
		t.Fatalf("backup: %v", err)
	}
	if !got.IntegrityOK || got.SHA256 == "" || got.SizeBytes == 0 {
		t.Fatalf("backup evidence incomplete: %+v", got)
	}
	if got.SchemaMigrationCount == 0 {
		t.Fatal("backup must contain applied migrations")
	}
	if got.TableCounts["jobs"] != 0 || got.TableCounts["tasks"] != 0 {
		t.Fatalf("unexpected initial rows: %+v", got.TableCounts)
	}

	verified, err := VerifySQLite(ctx, backupPath)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if verified.SHA256 != got.SHA256 {
		t.Fatalf("verification hash = %s, backup hash = %s", verified.SHA256, got.SHA256)
	}

	restored := filepath.Join(dir, "restore", "velox.db")
	restoredEvidence, err := RestoreSQLite(ctx, backupPath, restored)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if restoredEvidence.SHA256 != got.SHA256 {
		t.Fatalf("restore hash = %s, backup hash = %s", restoredEvidence.SHA256, got.SHA256)
	}
}

func TestVerifySQLiteRejectsMissingSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE unrelated (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create unrelated table: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close source: %v", err)
	}

	// A valid SQLite file without the application schema must not be treated
	// as a restorable Velox backup.
	if _, err := VerifySQLite(context.Background(), path); !errors.Is(err, ErrSchemaMissing) {
		t.Fatalf("VerifySQLite error = %v, want ErrSchemaMissing", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("database disappeared: %v", err)
	}
}

func TestRestoreSQLiteRejectsSamePath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "same.db")
	if _, err := RestoreSQLite(context.Background(), path, path); !errors.Is(err, ErrInvalidPath) {
		t.Fatalf("RestoreSQLite error = %v, want ErrInvalidPath", err)
	}
}
