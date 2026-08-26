package store

import (
	"path/filepath"
	"testing"

	"velox-server/internal/artifactsstore"
)

func TestArtifactRepositoriesFromStoreUseCanonicalSQLiteStore(t *testing.T) {
	t.Parallel()

	s, err := NewSQLiteStore(filepath.Join(t.TempDir(), "velox.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if got := artifactsstore.NewSQLiteUploadRepository(s.DB()); got == nil {
		t.Fatal("upload repository must be constructed from the canonical database")
	}
	if got := artifactsstore.NewSQLiteUploadSessionWriter(s.DB()); got == nil {
		t.Fatal("upload session writer must be constructed from the canonical database")
	}
	if got := artifactsstore.NewSQLiteArtifactRepository(s.DB()); got == nil {
		t.Fatal("artifact repository must be constructed from the canonical database")
	}
	if got := artifactsstore.NewSQLiteArtifactReader(s.DB()); got == nil {
		t.Fatal("artifact reader must be constructed from the canonical database")
	}
	if got := NewSQLiteAuthReaderFromStore(s); got == nil || got.db != s.db {
		t.Fatal("auth reader must use the canonical SQLiteStore database")
	}
	if got := artifactsstore.NewArtifactReconcilerRepository(s.DB()); got == nil {
		t.Fatal("artifact reconciler repository must be constructed from the canonical database")
	}
	if got := artifactsstore.NewArtifactGCStore(s.DB()); got == nil {
		t.Fatal("artifact GC store must be constructed from the canonical database")
	}
}
